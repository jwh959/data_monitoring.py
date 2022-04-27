import pandas as pd
import zipfile
from datetime import datetime, timedelta

from .base import DataSet


class CPSCReports(DataSet):
    url = 'https://www.saferproducts.gov/SPDB.zip'
    destination = r'C:\Users\jhayes\Documents\nhtsa query data\SPDB.zip'
    filename = r'IncidentReports.csv'
    category = "CPSC Reports"
    pickle_location = r'C:\Users\aschmidt\Documents\nhtsa query data\cpsc_reports.pkl'
    col_names = ['Report No.', 'Report Date', 'Sent to Manufacturer / Importer / Private Labeler', 'Publication Date',
                 'Category of Submitter', 'Product Description', 'Product Category', 'Product Sub Category', 'Product Type',
                 'Product Code', 'Manufacturer / Importer / Private Labeler Name', 'Brand', 'Model Name or Number', 'Serial Number',
                 'UPC', 'Date Manufactured', 'Manufacturer Date Code', 'Retailer', 'Retailer State', 'Purchase Date',
                 'Purchase Date Is Estimate', 'Incident Description', 'City', 'State', 'ZIP', 'Location', '(Primary) Victim Severity',
                 '(Primary) Victim\'s Gender', 'My Relation To The (Primary) Victim', '(Primary) Victim\'s Age (years)',
                 'Submitter Has Product', 'Product Was Damaged Before Incident', 'Damage Description', 'Damage Repaired',
                 'Product Was Modified Before Incident', 'Have You Contacted The Manufacturer', 'If Not Do You Plan To',
                 'Answer Explanation', 'Company Comments', 'Associated Report Numbers']
    drop_col = ['Sent to Manufacturer / Importer / Private Labeler', 'Serial Number', 'UPC', 'Retailer', 'Retailer State', 'Purchase Date',
                'Purchase Date Is Estimate', 'City', 'State', 'ZIP', 'Location', '(Primary) Victim\'s Gender',
                'My Relation To The (Primary) Victim',  'Submitter Has Product', 'Product Was Damaged Before Incident',
                'Damage Description', 'Damage Repaired', 'Product Was Modified Before Incident', 'Have You Contacted The Manufacturer',
                'If Not Do You Plan To']
    encoding = 'cp1252'

    stroller_codes = [1522, 1328, 1505, 1531]
    infant_carrier_codes = [1519]
    bassinet_codes = [1537]
    baby_codes = [1522, 1328, 1505, 1519, 1537, 1398, 1531, 1537]

    def __init__(self, update_window=30, development_mode=False):
        super().__init__(update_window, development_mode)

    def retrieve_data(self):
        super().retrieve_data()
        with zipfile.ZipFile(self.destination) as myzip:
            df_full = pd.read_csv(myzip.open(self.filename),
                                  sep=',',
                                  skiprows=[0],
                                  encoding=self.encoding)
        return df_full

    @property
    def clean_data(self):
        df_full = self.retrieve_data()
        df = df_full.drop(self.drop_col, axis=1)
        df['Product Code'] = pd.to_numeric(df['Product Code'], errors='coerce')

        for col in ['Report Date', 'Publication Date']:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
        return df

    def find_recent(self, df):
        back = datetime.today() - timedelta(days=self.update_window)
        new_df = df[df['Publication Date'] >= back]
        return new_df

    def isolate_strollers(self, df):
        stroller_data = df[df['Product Code'].isin(self.stroller_codes)]
        return stroller_data

    def isolate_ics(self, df):
        ics_data = df[df['Product Code'].isin(self.infant_carrier_codes)]
        return ics_data

    def isolate_bassinet(self, df):
        bassinet_data = df[df['Product Code'].isin(self.bassinet_codes)]
        return bassinet_data

    def data_overview(self):
        df = self.clean_data
        a = df[df['Product Code'].isin(self.baby_codes)]
        print(a.groupby(by='Product Code')['Product Type'].value_counts())
        print(a.groupby(by=['Product Code', '(Primary) Victim Severity'])['Product Type'].describe())

    def format_to_print(self, cpsc_df):
        cpsc_df.drop(['Report Date', 'Category of Submitter', 'Product Category', 'Product Sub Category',
                      'Product Code', 'Date Manufactured', 'Manufacturer Date Code', 'Answer Explanation',
                      'Associated Report Numbers'],
                     axis=1, inplace=True)

        cols = ['(Primary) Victim\'s Age (years)']
        cpsc_df[cols] = cpsc_df[cols].astype('Int16')

        linebreak_cols = ['Product Description', 'Incident Description', 'Company Comments']
        cpsc_df[linebreak_cols] = cpsc_df[linebreak_cols].replace(r'\n', r'<br/>', regex=True)
        cpsc_df[linebreak_cols] = cpsc_df[linebreak_cols].replace(r'\r', '', regex=True)

        cpsc_df.rename(columns={'Manufacturer / Importer / Private Labeler Name': 'Mfr.',
                                'Model Name or Number': 'Model',
                                'Publication Date': 'Pub Date', '(Primary) Victim Severity': 'Injury Severity',
                                '(Primary) Victim\'s Age (years)': 'Age (yrs)'}, inplace=True)

        cpsc_df[['Mfr.', 'Brand', 'Model']] = cpsc_df[['Mfr.', 'Brand', 'Model']].fillna('')
        cpsc_df.insert(loc=4,
                       column='Mfr / Brand / Model',
                       value=cpsc_df['Mfr.'] + r' /<br/>' + cpsc_df['Brand'] + r' /<br/>' + cpsc_df['Model'])
        cpsc_df.drop(['Mfr.', 'Brand', 'Model'],
                     axis=1, inplace=True)

        cpsc_df['Product Description'] = cpsc_df['Product Description'].str.slice(0, 150)

        return cpsc_df

    def write_html(self, df):
        # df['Report No.'] = df['Report No.'].apply(lambda x: f'<a href="https://www.saferproducts.gov/Search/Result.aspx?dm=0&q={x}">{x}</a>')
        df.set_index('Report No.', inplace=True)
        html = super().write_html(df)
        return html

    def run_update(self, mode='compare'):
        clean_df = self.clean_data

        if mode == 'recent':
            new_df = self.find_recent(clean_df)
        else:
            new_df = clean_df
        stroller_df = self.isolate_strollers(new_df)
        ics_df = self.isolate_ics(new_df)
        bassinet_df = self.isolate_bassinet(new_df)

        new_reports = pd.concat([stroller_df, ics_df, bassinet_df],
                                keys=['Wheeled Goods', 'Car Seats', 'Bassinets and Sleepers'])

        formatted_data = self.format_to_print(new_reports)
        return formatted_data
