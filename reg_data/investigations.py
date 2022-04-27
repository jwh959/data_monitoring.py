import pandas as pd
from datetime import datetime, timedelta
import re
from functools import lru_cache
import io

from .base import DataSet


class Investigations(DataSet):
    url = 'https://www-odi.nhtsa.dot.gov/downloads/folders/Investigations/FLAT_INV.zip'
    col_names = ['NHTSA ACTION NUMBER', 'MAKE', 'MODEL', 'YEAR', 'COMPNAME', 'MFR_NAME', 'ODATE',
                 'CDATE', 'CAMPNO', 'SUBJECT', 'SUMMARY']
    encoding = 'cp1252'
    category = "NHTSA Investigations"

    def __init__(self, update_window=30, development_mode=False):
        super().__init__(update_window, development_mode)
        self.col_names = [item.lower() for item in self.col_names]
        self.destination = self.base_path + 'FLAT_INV.zip'

    def retrieve_data(self):
        super().retrieve_data()

        data = io.BytesIO()
        df_full = pd.read_csv(self.destination,
                              sep='\t',
                              header=None,
                              compression='zip',
                              names=self.col_names,
                              encoding=self.encoding,
                              encoding_errors='replace')
        return df_full

    def isolate_crs(self):
        df = self.retrieve_data()
        df.index.name = 'NHTSA Index'
        cols_to_search = ['make', 'model', 'mfr_name']
        df2 = pd.melt(
            df[cols_to_search].reset_index(),
            id_vars='NHTSA Index',
            var_name='cols',
            value_name='names'
        ).dropna()

        with open('brand list.txt') as f:
            brands = f.readlines()
        brands = [x.strip() for x in brands]

        with open('exclude list.txt') as f:
            excluded_words = f.readlines()
        excluded_words = [x.strip() for x in excluded_words]

        pat_include = re.compile(pattern='|'.join(map(re.escape, brands)), flags=re.IGNORECASE)
        pat_exclude = re.compile(pattern='|'.join(map(re.escape, excluded_words)), flags=re.IGNORECASE)

        @lru_cache(maxsize=None)
        def compare(x, pat):
            return bool(pat.search(x))

        contains = df2['names'].apply(lambda x: compare(x, pat_include))
        excludes = df2['names'].apply(lambda x: compare(x, pat_exclude))
        contain_index = df2[contains]['NHTSA Index']
        exclude_index = df2[excludes]['NHTSA Index']
        # crs_indices = contain_index[~contain_index.isin(exclude_index)]

        crs_investigations = df[df.index.isin(contain_index)
                                & ~df.index.isin(exclude_index)]

        for col in ['odate', 'cdate']:
            crs_investigations[col] = pd.to_datetime(crs_investigations[col], format='%Y%m%d')
        crs_investigations.sort_values(by='odate', na_position='first', inplace=True)

        return crs_investigations

    def find_recent(self, crs_investigations):
        back = datetime.today() - timedelta(days=self.update_window)
        recently_opened = crs_investigations[crs_investigations['odate'] >= back]
        recently_closed = crs_investigations[crs_investigations['cdate'] >= back]
        new_investigations = pd.concat([recently_opened, recently_closed],
                                       keys=[f'Opened last {self.update_window} days',
                                             f'Closed last {self.update_window} days'])
        return new_investigations

    def format(self, crs_df):
        crs_df_print = crs_df[['nhtsa action number', 'make', 'model', 'mfr_name',
                               'odate', 'cdate', 'subject', 'summary']]
        # crs_df_print.summary = crs_df_print.summary.str.replace("\x92", "'")
        crs_df_print.rename(columns={'nhtsa action number': 'NHTSA Action Number', 'make': 'Make', 'model': 'Model',
                                     'mfr_name': 'Manufacturer', 'odate': 'Open Date', 'cdate': 'Close Date',
                                     'subject': 'Subject', 'summary': 'Summary'},
                            inplace=True)
        return crs_df_print

    def run_update(self, mode='recent'):
        """
        Executes methods to update data
        :param mode: (str) 'recent' returns data in timeframe specified by update_window parameter
        :return: print_df (Pandas DataFrame). Cleaned and relabeled dataframe with new data
        """
        crs_data = self.isolate_crs()
        if mode == 'recent':
            new_data = self.find_recent(crs_data)
        else:
            new_data = self.isolate_crs()
        formatted_data = self.format(new_data)
        return formatted_data

    @property
    def html_msg_no_new(self):
        description = f""" Website queried {self.query_time}.<br />
            No {self.category} have been opened or closed in the last {self.update_window} days."""
        return description

    @property
    def html_msg_new(self):
        description = f"""<p> Website queried {self.query_time}. 
            New {self.category} have been opened or closed in the last {self.update_window} days. <br> <br> """
        return description
