import pandas as pd
import numpy as np
from google.cloud import storage
from datetime import datetime

agency_master = pd.read_csv('data/raw_data/AgencyMaster.csv')
emp_master = pd.read_csv('data/raw_data/EmpMaster.csv')
nycpayroll_2020 = pd.read_csv('data/raw_data/nycpayroll_2020.csv')
nycpayroll_2021 = pd.read_csv('data/raw_data/nycpayroll_2021.csv')
title_master = pd.read_csv('data/raw_data/TitleMaster.csv')



# changing date datatype from object to datetime

date_formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]

def parse_date(date_string):
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    return pd.NaT 


nycpayroll_2020['AgencyStartDate'] = nycpayroll_2020['AgencyStartDate'].apply(parse_date)
nycpayroll_2021['AgencyStartDate'] = nycpayroll_2021['AgencyStartDate'].apply(parse_date)


nyc_payrolldf = pd.concat([nycpayroll_2020, nycpayroll_2021])

# write to csv
agency_master.to_csv('data/cleaned_data/agency_master.csv', index= False)
emp_master.to_csv('data/cleaned_data/emp_master.csv', index= False)
nyc_payrolldf.to_csv('data/cleaned_data/nyc_payrolldf.csv', index= False)
title_master.to_csv('data/cleaned_data/title_master.csv', index= False)



def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json('gcp/service_account_key.json')
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    
    blob.upload_from_filename(data)
    
def main():
    
    bucket_name = 'nyc_bucket_ds'
    # create new account
    service_account_key = 'service_account_key.json'
    
    upload_to_gcs(bucket_name, 'transformed_data/agency_master.csv', 'data/cleaned_data/agency_master.csv', service_account_key)
    upload_to_gcs(bucket_name, 'transformed_data/emp_master.csv', 'data/cleaned_data/emp_master.csv', service_account_key)
    upload_to_gcs(bucket_name, 'transformed_data/nyc_payrolldf.csv', 'data/cleaned_data/nyc_payrolldf.csv', service_account_key)
    upload_to_gcs(bucket_name, 'transformed_data/title_master.csv', 'data/cleaned_data/title_master.csv', service_account_key)
    

    
    print('transformed data successfully added')
if __name__ == '__main__':
    main()
    
    
