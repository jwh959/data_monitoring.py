import urllib
import re
import pandas as pd
from datetime import datetime, timedelta

from .base import DataSet


class CPSCNov(DataSet):
    # destination = r'C:\Users\aschmidt\Documents\nhtsa query data\NOV Data.xlsx'
    website_url = r'https://www.cpsc.gov/Recalls/violations'
    pickle_location = r'C:\Users\jhayes\Documents\nhtsa query data\cpsc_nov.pkl'
    col_names = ['letter_date', 'product_name', 'model_no', 'violation',
                 'citation', 'action', 'firm', 'address', 'city', 'country']
    drop_col = ['model_no', 'address', 'city']
    category = "CPSC Notice of Violation"

    def __init__(self, update_window=30, development_mode=False):
        self._clean_data = None
        self._url = None
        super().__init__(update_window, development_mode)
        self.destination = self.base_path + 'NOV Data.xlsx'

    def run_update(self):
        new_df = self.find_recent(self.clean_data)
        summarized_df = self.summarize_monthly(new_df)

        return summarized_df

    def retrieve_data(self):
        super().retrieve_data()
        df_full = pd.read_excel(self.destination,
                                skiprows=list(range(5)))
        return df_full

    def find_recent(self, df):
        # returns entries from whole months, starting with 1st of month
        months = self.update_window//30 - 1
        back = df.letter_date.max() - pd.DateOffset(months=months)
        back = back.replace(day=1)
        mask = df.letter_date.map(lambda x: x >= back)
        new_df = df[mask]
        return new_df

    def summarize_monthly(self, df):
        df['month_year'] = df['letter_date'].map(lambda x: x.strftime('%Y %m'))
        df2 = pd.crosstab([df.month_year, df.citation, df.violation], df.action)
        df2 = df2.loc[(df2 != 0).any(axis=1)]

        df2.columns = df2.columns.tolist()
        cols = list(df2.columns.values)
        idx = df2.index.names
        df2.reset_index(inplace=True)
        cols.insert(0, idx[0])
        df2.sort_values(cols, ascending=False, inplace=True)
        df2.set_index(idx, inplace=True)
        return df2

    @property
    def clean_data(self):
        if self._clean_data is None:
            df_full = self.retrieve_data()
            orig_names = df_full.columns.values
            col_dictionary = dict(zip(orig_names, self.col_names))
            df_full.rename(columns=col_dictionary, inplace=True)
            df = df_full.drop(self.drop_col, axis=1)

            df['action'] = pd.Categorical(df['action'],
                                          categories=['RSSC - Consumer Level Recall',
                                                      'DSSC - Distribution Level Recall',
                                                      'SSC - Stop Sale and Correct Future Production',
                                                      'CFP - Correct Future Production'],
                                          ordered=True)
            self._clean_data = df

        return self._clean_data

    @property
    def url(self):
        if self._url is None:
            self._url = self.retrieve_url(self.website_url)

        return self._url

    @property
    def html_msg_new(self):
        description = f"""Website queried {self.query_time}.<br />
            {self.category} data is updated infrequently. Below is the most recent {self.update_window} days of data available.<br><br>"""
        return description

    @staticmethod
    def retrieve_url(website_url):
        response = urllib.request.urlopen(website_url)
        html_str = response.read().decode('utf-8')
        pat = 'href="([^"]*.xlsx.*?)"'
        a = re.search(pat, html_str)[1]
        url = a if a.startswith('https://www.cpsc.gov') else r'https://www.cpsc.gov' + a
        return url