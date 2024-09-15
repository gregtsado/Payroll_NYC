from google.cloud import bigquery

def load_csv_to_bigquery(client, dataset_id, table_id, csv_file_path):
    uri = f'gs://{csv_file_path}'
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    table_ref = client.dataset(dataset_id).table(table_id)
    
    load_job = client.load_table_from_uri(
        uri, 
        table_ref,
        job_config=job_config
    )
    
    # Wait for job completion
    load_job.result()
    
    # Verify load job
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_id}")


def main():
    # Initialize the BigQuery client
    service_account_key = 'service_account_key.json'
    client = bigquery.Client.from_service_account_json(service_account_key)
    
    dataset_id = "nyc_stagging"
    
    # Looping through files
    files = [
        {"filepath":"nyc_bucket_ds/transformed_data/nyc_payrolldf.csv", "table_id":"nyc_payrolldf"},
        {"filepath":"nyc_bucket_ds/transformed_data/emp_master.csv", "table_id":"EmpMaster"},
        {"filepath":"nyc_bucket_ds/transformed_data/agency_master.csv", "table_id":"AgencyMaster"},
        {"filepath":"nyc_bucket_ds/transformed_data/title_master.csv", "table_id":"TitleMaster"}
    ]
    
    for file in files:
        load_csv_to_bigquery(client, dataset_id, file['table_id'], file['filepath'])
    
    # Call the stored procedure
    query = """
    CALL `tactile-reason-430717-g4.nyc_stagging.agg_nyc_data`()
    """

    try:
        # Run the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        print('Stored procedure executed successfully')

    except Exception as e:
        print(f'An error occurred: {e}')

        
if __name__ == '__main__':
    main()
