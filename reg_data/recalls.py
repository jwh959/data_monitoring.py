import pandas as pd
from datetime import datetime, timedelta

from .base import DataSet


class Recalls(DataSet):
    url = 'https://www-odi.nhtsa.dot.gov/downloads/folders/Recalls/FLAT_RCL.zip'
    # destination = r'C:\Users\aschmidt\Documents\nhtsa query data\FLAT_RCL.zip'
    col_names = ['RECORD_ID', 'CAMPNO', 'MAKETXT', 'MODELTXT', 'YEARTXT', 'MFGCAMPNO', 'COMPNAME', 'MFGNAME',
                 'BGMAN', 'ENDMAN', 'RCLTYPECD', 'POTAFF', 'ODATE', 'INFLUENCED_BY', 'MFGTXT', 'RCDATE', 'DATEA',
                 'RPNO', 'FMVSS', 'DESC_DEFECT', 'CONEQUENCE_DEFECT', 'CORRECTIVE_ACTION', 'NOTES', 'RCL_CMPT_ID',
                 'MFR_COMP_NAME', 'MFR_COMP_DESC', 'MFR_COMP_PTNO']
    encoding = 'latin-1'
    category = "NHTSA Recalls"

    def __init__(self, update_window=730, development_mode=False):
        super().__init__(update_window, development_mode)
        self.col_names = [item.lower() for item in self.col_names]
        self.destination = self.base_path + 'FLAT_RCL.zip'
        return

    def retrieve_data(self):
        super().retrieve_data()
        df_full = pd.read_csv(self.destination,
                              sep='\t',
                              header=None,
                              compression='zip',
                              names=self.col_names,
                              encoding=self.encoding)
        return df_full

    def isolate_crs(self):
        df = self.retrieve_data()
        df.set_index('record_id', inplace=True)
        crs_recalls = df[df['rcltypecd'].str.contains('c', case=False)]
        for col in ['bgman', 'endman', 'odate', 'rcdate', 'datea']:
            crs_recalls[col] = pd.to_datetime(crs_recalls[col], format='%Y%m%d')

        return crs_recalls

    def find_recent(self, crs_recalls):
        back = datetime.today() - timedelta(days=self.update_window)
        new_recalls = crs_recalls[crs_recalls['datea'] >= back]
        return new_recalls

    def format(self, crs_df):
        crs_df_print = crs_df[['maketxt', 'modeltxt', 'campno', 'datea', 'desc_defect']]
        crs_df_print.index.name = 'NHTSA Record ID'
        crs_df_print.rename(columns={'record_id': 'NHTSA Record',
                                     'maketxt': 'Make', 'modeltxt': 'Model',
                                     'campno': 'Campaign Number', 'datea': 'Date Added',
                                     'desc_defect': 'Description'},
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
