import os
import gspread

class SheetsClient:

    # The constructor for the SheetsClient class.
    def __init__(self):
        credentials = {
            "type": "service_account",
            "project_id": "precise-cabinet-280004",
            "private_key_id": os.environ["google_private_key_id"],
            "private_key": os.environ["google_private_key"],
            "client_email": "fantasyfootball-upload@precise-cabinet-280004.iam.gserviceaccount.com",
            "client_id": "114546278197267411100",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/fantasyfootball-upload%40precise-cabinet-280004.iam.gserviceaccount.com"
        }
        self.gc = gspread.service_account_from_dict(credentials)

    # create a new google empty google sheet
    def create_sheet(self, sheet_name):
        return self.gc.create(sheet_name)

    # share sheet with email
    def share_sheet(self, sheet):
        sheet.share("caltonji@gmail.com", perm_type='user', role='writer')
        sheet.share(None, perm_type='anyone', role='reader')

    def upload_df(self, sheet, df, worksheet_name):
        if df is not None and len(df.columns) > 0 and len(df.index) > 0:
            cols = len(df.columns)
            rows = len(df.index) + 1
            ws = sheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)
            ws.update([df.columns.values.tolist()] + df.values.tolist())
        else:
            ws = sheet.add_worksheet(title=worksheet_name, rows=1, cols=1)

    # copy an example worksheet into the google sheet
    def copy_example_ws(self, sheet):
        example_sheet = self.gc.open_by_key(os.environ["example_sheet_id"])
        example_ws = example_sheet.worksheet(os.environ["example_ws_name"])
        info = example_ws.copy_to(sheet.id)

        ws = sheet.worksheet(info["title"])

        # Update the title and move the worksheet to the front
        ws.update_title(os.environ["example_ws_name"])
        sheet.reorder_worksheets([ws])

        # delete the default sheet
        sheet.del_worksheet(sheet.worksheet("Sheet1"))