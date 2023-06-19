import io
import os.path
from typing import Optional, TypedDict

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


class DownloadRequestWrapper:
    def __init__(self, media_request):
        self.media_request = media_request
        self.resumable = media_request.resumable


class GoogleDriveFileListTypedDict(TypedDict):
    id: str  # unique id of the file in google drive
    name: str  # human name of the file as saved by user


class DriveService:
    """A Class created to make it easier to call different methods of the google drive API
    as specified here: https://developers.google.com/drive/api/quickstart/python
    """

    # defines what permissions the api call will have
    # in this case it's access to the file names (drive.readonly.metadata)
    # drive.file and auth/drive will alow us ot modify files
    scopes = [
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ]
    secrets_dir = ".secrets/"
    token_dir = f"{secrets_dir}token.json"

    def __init__(self):
        """Uses OAuth2 to handle connections and credentials
        reference documenation: https://developers.google.com/drive/api/quickstart/python
        https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html
        """
        self.creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_dir):
            self.creds = Credentials.from_authorized_user_file(
                self.token_dir, self.scopes
            )
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    f"{self.secrets_dir}drive_cred.json",
                    scopes=DriveService.scopes,
                    redirect_uri="urn:ietf:wg:oauth:2.0:oob",
                )
                auth_url, _ = flow.authorization_url(prompt="consent")
                print(f"Please go to this URL: {auth_url}")
                code = input("Enter the authorization code: ")
                flow.fetch_token(code=code)
                self.creds = flow.credentials
                # Save the credentials for the next run
                with open(self.token_dir, "w") as token:
                    token.write(flow.credentials.to_json())
        self.service = build("drive", "v3", credentials=self.creds)

    def list_files(self, **kwargs) -> Optional[list[GoogleDriveFileListTypedDict]]:
        """
        Lists files in drive.  Pass in arguments as specified in https://developers.google.com/drive/api/quickstart/python
        in a dictionary (ie {'pageSize':10}) and they'll be invoked.
        By default currently calls the account owners drive (no shared drive)
        kwargs[Dict]: Dictionary of arguments which will be invoked when the google drive service requests files

        Returns:
            Optional[dict[str,str]]: Assuming valid arguments returns file names and ids. Otherwise returns none
        """
        # set page size if not specified
        kwargs.setdefault("pageSize", 100)
        # set fields if not specified
        kwargs.setdefault("fields", "nextPageToken, files(id, name)")
        items = []
        next_page_token = None
        print(kwargs)
        try:
            # Call the Drive v3 API
            while True:
                results = (
                    self.service.files()
                    .list(**kwargs, pageToken=next_page_token)
                    .execute()
                )
                items += results.get("files", [])
                next_page_token = results.get("nextPageToken", None)
                print(
                    f"Retrieved {len(results.get('files'))} with last file being {results.get('files')[-1]}"
                )
                if next_page_token is None or not len(results.get("files")):
                    break
            return items
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f"An error occurred: {error} using the following arguments {kwargs}")

    def get_file(self, file_id: str, shared_drive: bool = True, **kwargs) -> bytes:
        """Downloads a file based on the file_id passed in.  You can get the file id
        by right clicking the file clicking share and getting the id from the end of the url.
        More documentation can be found here:
        https://developers.google.com/drive/api/guides/manage-downloads

        Args:
            file_id (str): The alphanumeric id for each file
            shared_drive (bool, optional): Whether to include all drives associated with the account. Defaults to True.

        Raises:
            ValueError: Incase of error

        Returns:
            bytes: The raw format of the downloaded file.  Use BytesIO to help decrypt.
        """
        query_dict = {
            "fileId": file_id,
            "supportsAllDrives": shared_drive,
            "alt": "media",
            **kwargs,
        }
        file = io.BytesIO()
        try:
            request = self.service.files().get_media(**query_dict)
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Downloaded {file_id} {int(status.progress() * 100)}%.")
        except HttpError as error:
            print(f"An error occurred: {error}")
            file = None
            raise ValueError(f"Unable to proceed due to error {error}")
        return file.getvalue()

    def upload_file(
        self,
        filename: str,
        folder_id: str,
        file,
        mimetype: str = "application/octet-stream",
        file_id: str = "",
    ) -> Optional[str]:
        try:
            file_metadata = {"name": filename, "parents": [folder_id]}
            # pylint: disable=maybe-no-member
            media = MediaIoBaseUpload(file, mimetype=mimetype)
            if not file_id:
                media = (
                    self.service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        media_mime_type=mimetype,
                    )
                    .execute()
                )
                print(f'File ID: {media.get("id")}')
            else:
                media = (
                    self.service.files()
                    .update(
                        fileId=file_id,
                        # body = file_metadata,
                        media_body=media,
                        media_mime_type=mimetype,
                    )
                    .execute()
                )
                print(f'Update File ID: {media.get("id")} with new contents')

        except HttpError as error:
            print(f"An error occurred: {error}")
            file = None
            return file

        return media.get("id")

    def list_files_in_shared_drive_folder(
        self,
        folder_id: str,
    ) -> list:
        return (
            self.list_files(
                **{
                    "supportsAllDrives": True,
                    "includeItemsFromAllDrives": True,
                    "q": f"'{folder_id}' in parents and trashed = false",
                    "pageSize": 1000,
                }
            )
            or []
        )
