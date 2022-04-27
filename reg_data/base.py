import os
import pandas as pd
import urllib.request
import warnings

from datetime import datetime, timedelta

warnings.simplefilter(action='ignore', category=FutureWarning)


class DataSet:
    def __init__(self, update_window=30, development_mode=False):
        self.query_time = '(no new query)'
        self.update_window = update_window
        self.development_mode = development_mode

        self.base_path = fr'C:\Users\jhayes\Documents\nhtsa query data' + '\\'
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def retrieve_data(self):
        if not self.development_mode:
            urllib.request.urlretrieve(self.url, self.destination)
            self.query_time = datetime.now().strftime("%d %b %Y  %H:%M")

    @property
    def html_msg_no_new(self):
        description = f""" Website queried {self.query_time}.<br>
            No new {self.category} found in the last {self.update_window} days. <br>"""
        return description

    @property
    def html_msg_new(self):
        description = f"""<p> Website queried {self.query_time}. <br>
            New {self.category} data found in the last {self.update_window} days. <br> <br> """
        return description

    def write_html(self, df):
        if df.empty:
            html = f"""\
            <html>
              <body> <p> {self.html_msg_no_new} </p></body>
            </html>
            """
            return html
        else:
            with pd.option_context('display.max_colwidth', -1):
                html_table = df.to_html(justify='left', escape=False)
            html = f"""\
            <html>
              <body>
                <p> {self.html_msg_new}
                {html_table}            
                </p>
              </body>   
            </html>
            """
            return html
