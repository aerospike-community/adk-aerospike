"""AerospikeArtifactService — versioned binary artifacts in Aerospike.

Design
------
Each ``(app, user, scope_id, filename, version)`` is its own record. The
version is part of the primary key so reads of a specific version are a single
GET. Listing versions is a secondary-index query on ``fname`` followed by an
in-process filter on ``(app, user, scope_id)``.

Filename scoping
~~~~~~~~~~~~~~~~
Filenames prefixed ``user:`` are user-scoped (cross-session). For those, the
session-id slot in the key is replaced by the sentinel ``"user"`` —
matching ``InMemoryArtifactService``'s path scheme.

Record size cap
~~~~~~~~~~~~~~~
Aerospike caps records at namespace ``write-block-size`` (default 1 MiB, up to
8 MiB). Larger artifacts should be offloaded to object storage and referenced
here — planned, not in v0.0.1.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Self

from google.adk.artifacts import BaseArtifactService
from google.adk.artifacts.base_artifact_service import ArtifactVersion, ensure_part
from google.adk.errors.input_validation_error import InputValidationError

from .._internal.client import close_client, make_client
from .._internal.indexes import ensure_artifact_indexes
from .._internal.keys import USER_SCOPE_SID, artifact_key, artifact_scope_id
from .._internal.schema import Bins, Schema
from .._internal.uri import parse as parse_uri

if TYPE_CHECKING:
    import aerospike
    from google.genai import types as genai_types

log = logging.getLogger(__name__)


class AerospikeArtifactService(BaseArtifactService):
    """Versioned artifact storage on Aerospike Database."""

    def __init__(
        self,
        client: aerospike.Client,
        namespace: str,
        *,
        set_prefix: str = "adk_",
        ensure_indexes: bool = True,
    ) -> None:
        self._client = client
        self._schema = Schema(namespace=namespace, set_prefix=set_prefix)
        if ensure_indexes:
            ensure_artifact_indexes(client, self._schema)

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        parsed = parse_uri(uri)
        client = make_client(parsed)
        return cls(client, parsed.namespace, set_prefix=parsed.set_prefix)

    def close(self) -> None:
        close_client(self._client)

    # ---- BaseArtifactService ---------------------------------------------------

    async def save_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        artifact: genai_types.Part | dict[str, Any],
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> int:
        import aerospike

        part = ensure_part(artifact)
        scope_id = self._scope_id(filename, session_id)

        # Version = number of existing versions (first save → 0).
        existing = await self._versions_for(app_name, user_id, scope_id, filename)
        version = (max(existing) + 1) if existing else 0

        mime_type = _mime_for_part(part)
        data_bytes = _bytes_for_part(part)

        bins: dict[str, Any] = {
            Bins.APP_NAME: app_name,
            Bins.USER_ID: user_id,
            Bins.SESSION_ID: scope_id,
            Bins.FILENAME: filename,
            Bins.VERSION: version,
            Bins.MIME_TYPE: mime_type,
            Bins.DATA: data_bytes,
            Bins.CREATE_TIME: time.time(),
            Bins.CUSTOM_META: custom_metadata or {},
        }

        pk = (
            self._schema.namespace,
            self._schema.artifacts_set,
            artifact_key(app_name, user_id, scope_id, filename, version),
        )
        await asyncio.to_thread(
            self._client.put,
            pk,
            bins,
            None,
            {"exists": aerospike.POLICY_EXISTS_CREATE},
        )
        return version

    async def load_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> genai_types.Part | None:
        from aerospike import exception as ae

        scope_id = self._scope_id(filename, session_id)

        if version is None:
            versions = await self._versions_for(app_name, user_id, scope_id, filename)
            if not versions:
                return None
            version = max(versions)

        pk = (
            self._schema.namespace,
            self._schema.artifacts_set,
            artifact_key(app_name, user_id, scope_id, filename, version),
        )
        try:
            _, _, bins = await asyncio.to_thread(self._client.get, pk)
        except ae.RecordNotFound:
            return None

        return _part_from_bins(bins)

    async def list_artifact_keys(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str | None = None,
    ) -> list[str]:
        # Always include user-scoped artifacts; include session-scoped only
        # when a session_id is supplied. Matches InMemoryArtifactService.
        filenames: set[str] = set()
        filenames.update(
            await self._filenames_for(app_name, user_id, USER_SCOPE_SID)
        )
        if session_id is not None:
            filenames.update(
                await self._filenames_for(app_name, user_id, session_id)
            )
        return sorted(filenames)

    async def delete_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> None:
        from aerospike import exception as ae

        scope_id = self._scope_id(filename, session_id)
        versions = await self._versions_for(app_name, user_id, scope_id, filename)
        for v in versions:
            pk = (
                self._schema.namespace,
                self._schema.artifacts_set,
                artifact_key(app_name, user_id, scope_id, filename, v),
            )
            try:
                await asyncio.to_thread(self._client.remove, pk)
            except ae.RecordNotFound:
                pass

    async def list_versions(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[int]:
        scope_id = self._scope_id(filename, session_id)
        versions = await self._versions_for(app_name, user_id, scope_id, filename)
        return sorted(versions)

    async def list_artifact_versions(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[ArtifactVersion]:
        scope_id = self._scope_id(filename, session_id)
        rows = await self._rows_for(app_name, user_id, scope_id, filename)
        rows.sort(key=lambda r: r[0])
        return [
            _version_meta(app_name, user_id, scope_id, filename, bins)
            for _v, bins in rows
        ]

    async def get_artifact_version(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactVersion | None:
        from aerospike import exception as ae

        scope_id = self._scope_id(filename, session_id)
        if version is None:
            versions = await self._versions_for(app_name, user_id, scope_id, filename)
            if not versions:
                return None
            version = max(versions)

        pk = (
            self._schema.namespace,
            self._schema.artifacts_set,
            artifact_key(app_name, user_id, scope_id, filename, version),
        )
        try:
            _, _, bins = await asyncio.to_thread(self._client.get, pk)
        except ae.RecordNotFound:
            return None
        return _version_meta(app_name, user_id, scope_id, filename, bins)

    # ---- internals -------------------------------------------------------------

    def _scope_id(self, filename: str, session_id: str | None) -> str:
        try:
            return artifact_scope_id(filename, session_id)
        except ValueError as e:
            raise InputValidationError(str(e)) from e

    async def _versions_for(
        self, app_name: str, user_id: str, scope_id: str, filename: str
    ) -> list[int]:
        return [v for v, _bins in await self._rows_for(app_name, user_id, scope_id, filename)]

    async def _filenames_for(
        self, app_name: str, user_id: str, scope_id: str
    ) -> set[str]:
        from aerospike import predicates

        query = self._client.query(self._schema.namespace, self._schema.artifacts_set)
        query.where(predicates.equals(Bins.SESSION_ID, scope_id))
        records = await asyncio.to_thread(query.results)
        return {
            bins[Bins.FILENAME]
            for _, _, bins in records
            if bins.get(Bins.APP_NAME) == app_name
            and bins.get(Bins.USER_ID) == user_id
        }

    async def _rows_for(
        self, app_name: str, user_id: str, scope_id: str, filename: str
    ) -> list[tuple[int, dict[str, Any]]]:
        from aerospike import predicates

        query = self._client.query(self._schema.namespace, self._schema.artifacts_set)
        query.where(predicates.equals(Bins.FILENAME, filename))
        records = await asyncio.to_thread(query.results)
        out: list[tuple[int, dict[str, Any]]] = []
        for _, _, bins in records:
            if (
                bins.get(Bins.APP_NAME) == app_name
                and bins.get(Bins.USER_ID) == user_id
                and bins.get(Bins.SESSION_ID) == scope_id
            ):
                out.append((bins.get(Bins.VERSION, 0), bins))
        return out


def _mime_for_part(part: genai_types.Part) -> str | None:
    if part.inline_data is not None:
        return part.inline_data.mime_type
    if part.text is not None:
        return "text/plain"
    if part.file_data is not None:
        return part.file_data.mime_type
    raise InputValidationError("Not supported artifact type.")


def _bytes_for_part(part: genai_types.Part) -> bytes:
    if part.inline_data is not None:
        return part.inline_data.data or b""
    if part.text is not None:
        return part.text.encode("utf-8")
    if part.file_data is not None:
        # We store the URI; loading reconstructs the file_data part.
        return (part.file_data.file_uri or "").encode("utf-8")
    raise InputValidationError("Not supported artifact type.")


def _version_meta(
    app_name: str,
    user_id: str,
    scope_id: str,
    filename: str,
    bins: dict[str, Any],
) -> ArtifactVersion:
    version = bins.get(Bins.VERSION, 0)
    if scope_id == USER_SCOPE_SID:
        canonical_uri = (
            f"aerospike://apps/{app_name}/users/{user_id}/artifacts/{filename}"
            f"/versions/{version}"
        )
    else:
        canonical_uri = (
            f"aerospike://apps/{app_name}/users/{user_id}/sessions/{scope_id}"
            f"/artifacts/{filename}/versions/{version}"
        )
    return ArtifactVersion(
        version=version,
        canonical_uri=canonical_uri,
        custom_metadata=bins.get(Bins.CUSTOM_META) or {},
        create_time=float(bins.get(Bins.CREATE_TIME) or 0.0),
        mime_type=bins.get(Bins.MIME_TYPE),
    )


def _part_from_bins(bins: dict[str, Any]) -> genai_types.Part:
    from google.genai import types as genai_types

    mime: str | None = bins.get(Bins.MIME_TYPE)
    data: bytes = bins.get(Bins.DATA) or b""

    if mime == "text/plain":
        return genai_types.Part(text=data.decode("utf-8"))
    if mime is None:
        # File-data reference: data holds the URI bytes.
        return genai_types.Part(
            file_data=genai_types.FileData(file_uri=data.decode("utf-8"))
        )
    return genai_types.Part(
        inline_data=genai_types.Blob(mime_type=mime, data=data)
    )
