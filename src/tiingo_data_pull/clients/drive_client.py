"""Google Drive client helpers for uploading JSON exports."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from googleapiclient.discovery import Resource


class GoogleDriveClient:
    """Handles authenticated uploads to Google Drive."""

    def __init__(
        self,
        service_account_file: str,
        *,
        folder_id: str,
        scopes: Optional[list[str]] = None,
        chunk_size: int = 5 * 1024 * 1024,
    ) -> None:
        """Initialise the Drive client.

        Args:
            service_account_file: Path to the service account JSON credentials.
            folder_id: Destination Drive folder identifier.
            scopes: OAuth scopes to request.
            chunk_size: Upload chunk size in bytes.
        """

        effective_scopes = scopes or ["https://www.googleapis.com/auth/drive.file"]
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=effective_scopes,
        )
        self._service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        self._folder_id = folder_id
        self._chunk_size = chunk_size
        self._media_file_upload = MediaFileUpload

    def upload_json(self, file_path: str) -> Dict[str, str]:
        """Upload a JSON file to Google Drive.

        Args:
            file_path: Path to the JSON file to upload.

        Returns:
            Metadata for the uploaded file including its Drive identifier.
        """

        path = Path(file_path)
        media = self._media_file_upload(
            path,
            mimetype="application/json",
            resumable=True,
            chunksize=self._chunk_size,
        )
        file_metadata = {"name": path.name, "parents": [self._folder_id]}
        request = self._service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
        )
        response = request.execute()
        return {"id": response.get("id", ""), "webViewLink": response.get("webViewLink", "")}
