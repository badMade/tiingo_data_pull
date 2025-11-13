"""Google Drive uploader helpers using OAuth credentials stored outside the repo."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from googleapiclient.discovery import Resource

SCOPES = ("https://www.googleapis.com/auth/drive.file",)
CLIENT_SECRETS_ENV = "GOOGLE_OAUTH_CLIENT_SECRETS_FILE"
TOKEN_PATH_ENV = "GOOGLE_OAUTH_TOKEN_FILE"
DEFAULT_TOKEN_PATH = Path.home() / ".config" / "tiingo-data-pull" / "google-drive-token.json"


def upload_json(filepath: Path, drive_folder_id: str) -> Dict[str, str]:
    """Upload a JSON file to Google Drive using OAuth user credentials.

    The helper retrieves OAuth credentials from paths defined by environment
    variables so that secrets remain outside the repository. Credentials are
    refreshed transparently and stored in ``GOOGLE_OAUTH_TOKEN_FILE`` (or the
    default config path) to avoid repeated browser prompts.

    Args:
        filepath: Path to the JSON file to upload.
        drive_folder_id: Identifier of the Drive folder where the file lives.

    Returns:
        Metadata describing the uploaded file, including its Drive identifier.

    Raises:
        RuntimeError: If credential environment variables are missing or the
            Drive folder identifier is empty.
        FileNotFoundError: If ``filepath`` does not exist.
    """

    if not drive_folder_id:
        raise RuntimeError("Drive folder identifier is required for uploads.")
    if not filepath.exists():
        raise FileNotFoundError(f"Cannot upload missing file: {filepath}")

    credentials = _load_credentials()
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    media = MediaFileUpload(
        str(filepath),
        mimetype="application/json",
        resumable=True,
    )
    existing_file_id = _find_existing_file(service, filepath.name, drive_folder_id)
    if existing_file_id:
        request = service.files().update(
            fileId=existing_file_id,
            media_body=media,
            fields="id, name, webViewLink, version",
        )
    else:
        request = service.files().create(
            body={"name": filepath.name, "parents": [drive_folder_id]},
            media_body=media,
            fields="id, name, webViewLink, version",
        )
    response = request.execute()
    return {
        "id": response.get("id", ""),
        "name": response.get("name", filepath.name),
        "webViewLink": response.get("webViewLink", ""),
        "version": str(response.get("version", "")),
    }


def _load_credentials() -> Credentials:
    """Load Drive OAuth credentials, refreshing or re-authorising if needed."""

    client_secrets = os.getenv(CLIENT_SECRETS_ENV)
    if not client_secrets:
        raise RuntimeError(
            f"Missing OAuth client secrets file. Set {CLIENT_SECRETS_ENV} to the path"
        )
    secrets_path = Path(client_secrets).expanduser().resolve()
    token_path = _token_path()

    credentials: Optional[Credentials] = None
    if token_path.exists():
        credentials = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
        except RefreshError:
            credentials = None

    if not credentials or not credentials.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), list(SCOPES))
        credentials = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(credentials.to_json(), encoding="utf-8")

    return credentials


def _token_path() -> Path:
    """Resolve the path for persisted OAuth tokens, defaulting to config dir."""

    override = os.getenv(TOKEN_PATH_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_TOKEN_PATH


def _find_existing_file(service: "Resource", filename: str, folder_id: str) -> Optional[str]:
    """Locate an existing Drive file with the same name inside the folder."""

    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id)", pageSize=1)
        .execute()
    )
    files = response.get("files", [])
    if not files:
        return None
    return files[0].get("id")
