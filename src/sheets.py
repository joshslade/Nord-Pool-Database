import os
from pathlib import Path
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


load_dotenv()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

def get_credentials():
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    token_path = Path("token.json")
    if os.path.exists(token_path):
        credentials = Credentials.from_authorized_user_file(token_path, SCOPES)
       
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            print('refreshed token')
        else:
            raise RuntimeError("Failed to obtain valid Google Sheets API credentials. Please check your 'credentials.json' and authentication setup.")
        with open(token_path, "w") as token:
            token.write(credentials.to_json())

    return credentials




def append_dataframe_to_sheet(df):
    """
    Appends rows from a pandas DataFrame to a Google Sheet.
    Assumes the sheet already has a header row matching df.columns.
    """
    spreadsheet_id = SPREADSHEET_ID
    sheet_name = "Day-Ahead Energy Market"
    
    try:
        credentials = get_credentials()
    except RuntimeError as e:
        return {"error": str(e)}

    try:
        # Convert DataFrame to list of lists
        df_to_upload = df.copy()
        for col in df_to_upload.columns[:4]:
            df_to_upload[col] = df_to_upload[col].astype(str)
        values = df_to_upload.values.tolist()

        # Find the next empty row
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()

        # Get current data to find where to append
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}"
        ).execute()
        existing_rows = result.get("values", [])
        start_row = len(existing_rows) + 1  # 1-based indexing

        # Prepare range for appending
        range_name = f"{sheet_name}!A{start_row}"

        body = {"values": values}
        response = sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()

        print(f"Appended {len(values)} rows {sheet_name}")
        return response
    except HttpError as e:
        return {"error": f"Google Sheets API error: {e}"}

