from google.cloud import storage
import pandas as pd


def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json('service_account_key.json')
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    
    blob.upload_from_filename(data)
    
    
def main():
    # create new account
    service_account = 'abc-service'
    bucket_name = 'nyc_bucket_ds'
    
    service_account_key = 'service_account_key.json'
    
   
    agency_master = pd.read_csv('data/raw_data/AgencyMaster.csv')
    emp_master = pd.read_csv('data/raw_data/EmpMaster.csv')
    nycpayroll_2020 = pd.read_csv('data/raw_data/nycpayroll_2020.csv')
    nycpayroll_2021 = pd.read_csv('data/raw_data/nycpayroll_2021.csv')
    title_master = pd.read_csv('data/raw_data/TitleMaster.csv')
          
        

    upload_to_gcs(bucket_name, 'raw data/agency_master.csv', 'data/raw_data/AgencyMaster.csv', service_account_key)
    upload_to_gcs(bucket_name, 'raw data/emp_master.csv', 'data/raw_data/EmpMaster.csv', service_account_key)
    upload_to_gcs(bucket_name, 'raw data/nycpayroll_2020.csv', 'data/raw_data/nycpayroll_2020.csv', service_account_key)
    upload_to_gcs(bucket_name, 'raw data/nycpayroll_2021.csv', 'data/raw_data/nycpayroll_2021.csv', service_account_key)
    upload_to_gcs(bucket_name, 'raw data/title_master.csv', 'data/raw_data/TitleMaster.csv', service_account_key)
    
 
    print('Data successfully uploaded')
if __name__ == '__main__':
    main()