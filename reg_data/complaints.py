import pandas as pd
from datetime import datetime, timedelta

from .base import DataSet


class Complaints(DataSet):
    url = 'https://www-odi.nhtsa.dot.gov/downloads/folders/Complaints/FLAT_CMPL.zip'
    col_names = complaint_col_names = ['CMPLID', 'ODINO', 'MFR_NAME', 'MAKETXT', 'MODELTXT', 'YEARTXT', 'CRASH',
                                       'FAILDATE', 'FIRE', 'INJURED', 'DEATHS', 'COMPDESC', 'CITY', 'STATE', 'VIN',
                                       'DATEA', 'LDATE', 'MILES', 'OCCURENCES', 'CDESCR', 'CMPL_TYPE', 'POLICE_RPT_YN',
                                       'PURCH_DT', 'ORIG_OWNER_YN', 'ANTI_BRAKES_YN', 'CRUISE_CONT_YN', 'NUM_CYLS', 'DRIVE_TRAIN',
                                       'FUEL_SYS', 'FUEL_TYPE', 'TRANS_TYPE', 'VEH_SPEED', 'DOT', 'TIRE_SIZE', 'LOC_OF_TIRE',
                                       'TIRE_FAIL_TYPE', 'ORIG_EQUIP_YN', 'MANUF_DT', 'SEAT_TYPE', 'RESTRAINT_TYPE', 'DEALER_NAME',
                                       'DEALER_TEL', 'DEALER_CITY', 'DEALER_STATE', 'DEALER_ZIP', 'PROD_TYPE', 'REPAIRED_YN',
                                       'MEDICAL_ATTN', 'VEHICLES_TOWED_YN']
    drop_col = ['DOT', 'DRIVE_TRAIN', 'FUEL_SYS', 'FUEL_TYPE', 'LOC_OF_TIRE', 'MILES',
                'NUM_CYLS', 'REPAIRED_YN', 'TIRE_FAIL_TYPE', 'TIRE_SIZE', 'TRANS_TYPE',
                'VEH_SPEED', 'CRUISE_CONT_YN',
                'DEALER_CITY', 'DEALER_NAME', 'DEALER_STATE', 'DEALER_TEL', 'DEALER_ZIP']
    encoding = 'latin-1'
    category = "NHTSA Complaints"

    def __init__(self, update_window=30, development_mode=False):
        super().__init__(update_window, development_mode)
        self.col_names = [item.lower() for item in self.col_names]
        self.drop_col = [item.lower() for item in self.drop_col]
        self.destination = self.base_path + 'FLAT_CMPL.zip'
        self.pickle_location = self.base_path + 'complaints.pkl'

    def retrieve_data(self):
        super().retrieve_data()
        df_full = pd.read_csv(self.destination,
                              sep='\t',
                              header=None,
                              compression='zip',
                              names=self.col_names,
                              encoding=self.encoding,
                              encoding_errors='replace')
        return df_full

    def isolate_crs(self):
        df_complaints = self.retrieve_data()
        crs_complaints = df_complaints[(df_complaints['prod_type'].str.contains('C', na=False)) &
                                       ~(df_complaints['cdescr'].str.strip().str.fullmatch('Test', na=False, case=False))].\
            copy()
        crs_complaints.drop(self.drop_col, axis=1, inplace=True)

        crs_complaints['faildate'] = crs_complaints['faildate'].astype(str).replace('\.0', '', regex=True)
        crs_complaints['manuf_dt'] = crs_complaints['manuf_dt'].astype(str).replace('\.0', '', regex=True)
        crs_complaints['odino'] = crs_complaints['odino'].astype('int64')

        for col in ['faildate', 'datea', 'ldate', 'manuf_dt']:
            crs_complaints[col] = pd.to_datetime(crs_complaints[col], format='%Y%m%d')

        return crs_complaints

    # Compare complaints df against last pickle, then replace pickle
    def compare_crs_pickle(self, crs_complaints):
        crs_complaints_old = pd.read_pickle(self.pickle_location)
        new_complaints = pd.concat([crs_complaints, crs_complaints_old]). \
            drop_duplicates(subset=['odino', 'compdesc'], keep=False)
        if not self.development_mode:
            crs_complaints.to_pickle(self.pickle_location)
        return new_complaints

    def find_recent(self, crs_complaints):
        back = datetime.today() - timedelta(days=self.update_window)
        new_complaints = crs_complaints[crs_complaints['datea'] >= back]
        return new_complaints

    def format(self, crs_df):
        crs_df['cdescr'] = crs_df['cdescr'].str.capitalize()
        print_df = crs_df[['odino', 'mfr_name', 'maketxt',
                           'modeltxt', 'datea', 'manuf_dt', 'cdescr']]
        print_df['cdescr'] = print_df['cdescr'].str.encode('ascii', 'ignore').str.decode('ascii')

        print_df[['maketxt', 'modeltxt']] = print_df[['maketxt', 'modeltxt']].fillna('')
        print_df.insert(loc=2,
                        column='Make / Model',
                        value=print_df['maketxt'] + r' /<br/>' + print_df['modeltxt'])
        print_df.drop(['maketxt', 'modeltxt'], axis=1, inplace=True)

        print_df.index.name = 'NHTSA Index'
        print_df.rename(
            columns={'odino': 'NHTSA Ref Number', 'mfr_name': 'Manufacturer',
                     'maketxt': 'Make', 'modeltxt': 'Model', 'datea': 'Date Added',
                     'manuf_dt': 'Date of Manufacture', 'cdescr': 'Description of Complaint'},
            inplace=True)

        return print_df

    def run_update(self, mode='compare'):
        """
        Executes methods to update data
        :param mode: (str) 'compare' returns data since last update,
                           'recent' returns data in timeframe specified by update_window parameter
        :return: print_df (Pandas DataFrame). Cleaned and relabeled dataframe with new data
        """
        crs_data = self.isolate_crs()
        if mode == 'compare':
            new_data = self.compare_crs_pickle(crs_data)
        elif mode == 'recent':
            new_data = self.find_recent(crs_data)
        else:
            new_data = self.isolate_crs()
        formatted_data = self.format(new_data)
        return formatted_data
