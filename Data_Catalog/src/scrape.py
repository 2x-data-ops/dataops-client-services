import pandas_gbq as bq
import pandas as pd
import json
from log import setup_logging
from auth import GoogleCloudAuth


class DataPipeline:
    def __init__(self):
        self.logger = setup_logging()
        self.auth = GoogleCloudAuth()
        self.credentials = self.auth.get_credentials()
        self.crm_sources = [
            "salesforce", "google", "bing", "sfmc", "hubspot", 
            "mailchimp", "linkedin", "facebook", "pardot"
        ]

    def get_crm_source(self):
        """Fetch CRM source data from BigQuery."""
        self.logger.info(f"Starting to fetch data for {len(self.crm_sources)} CRM sources")
        all_dataset_table_pairs = []
        
        for source in self.crm_sources:
            self.logger.info(f"Processing source: {source}")
            try:
                query = f"""
                    SELECT DISTINCT _raw_dataset, _table
                    FROM `data_catalog.data_catalog`
                    WHERE _raw_dataset LIKE "%{source}%"
                    """    
                dataset_table_pairs = bq.read_gbq(
                    query, 
                    project_id='x-marketing',
                    credentials=self.credentials 
                )
                self.logger.info(f"Retrieved {len(dataset_table_pairs)} dataset-table pairs for {source}")
                all_dataset_table_pairs.append(dataset_table_pairs)
                
            except Exception as e:
                self.logger.error(f"Error fetching dataset-table pairs for {source}: {str(e)}", exc_info=True)
                continue
                
        combined_dataset_table_pairs = pd.concat(all_dataset_table_pairs)
        self.logger.info(f"Total dataset-table pairs retrieved: {len(combined_dataset_table_pairs)}")
        return combined_dataset_table_pairs

    def tabulate_sample_records(self, data):
        """Generate sample records for each dataset-table pair."""
        self.logger.info(f"Starting to tabulate sample records for {len(data)} dataset-table pairs")
        data2 = []
        total_count = len(data)
        
        current_source = None
        processed_pairs = 0

        for idx, row in data.iterrows():
            dataset = row['_raw_dataset']
            table = row['_table']
            # self.logger.info(f"Processing {dataset}.{table} ({idx + 1}/{len(data)})")
            
            source_match = None

            for source in self.crm_sources:
                if source in dataset.lower():
                    source_match = source
                    break

            if source_match != current_source:
                self.logger.info(f"Completed processing {current_source} source")
            current_source = source_match
            self.logger.info(f"Starting to process {current_source} source")

            processed_pairs += 1
            self.logger.info(f"Processing {dataset}.{table} ({processed_pairs}/{total_count})")
            
            query = f"""
                SELECT *
                FROM `{dataset}.{table}`
                TABLESAMPLE SYSTEM (1 PERCENT)
                LIMIT 1
            """
            
            try:
                sample_records = bq.read_gbq(
                    query, 
                    project_id='x-marketing',
                    credentials=self.credentials
                )
                sample_records_json = sample_records.to_json(orient='records')[1:-1]
                new_data = pd.DataFrame({
                    'dataset': [dataset],
                    'table': [table],
                    'sample_records': [sample_records_json]
                })
                data2.append(new_data)
                self.logger.debug(f"Successfully retrieved sample records for {dataset}.{table}")
                
            except Exception as e:
                self.logger.error(f"Error fetching sample records for {dataset}.{table}: {str(e)}", exc_info=True)
                continue
        
        if data2:  # Check if we have any successful results
            all_data2 = pd.concat(data2)
            self.logger.info(f"Completed tabulating sample records. Processed {len(all_data2)} tables successfully")
            return all_data2
        else:
            self.logger.error("No sample records were successfully retrieved")
            return pd.DataFrame(columns=['dataset', 'table', 'sample_records'])

    def process_to_csv(self, data2, output_path='output/sample_records.csv'):
        """Save the sample records to CSV."""
        self.logger.info(f"Saving sample records to {output_path}")
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            data2.to_csv(output_path, index=False)
            self.logger.info(f"Successfully saved sample records to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}", exc_info=True)
            raise

    def process_unnest_json(self, input_path='output/sample_records.csv', output_path='output/unnested_records.csv'):
        """Process and unnest JSON data."""
        self.logger.info("Starting JSON unnesting process")
        try:
            data = pd.read_csv(input_path)
            self.logger.info(f"Loaded {len(data)} records from {input_path}")
            
            final_data = pd.DataFrame(columns=["dataset", "table", "column", "sample_record"])
            processed_count = 0
            error_count = 0
            
            for index, row in data.iterrows():
                dataset = row['dataset']
                table = row['table']
                try:
                    json_data = json.loads(row['sample_records'])
                    temp_df = pd.DataFrame({
                        "dataset": [dataset] * len(json_data),
                        "table": [table] * len(json_data),
                        "column": json_data.keys(),
                        "sample_record": json_data.values()
                    })
                    final_data = pd.concat([final_data, temp_df], ignore_index=True)
                    processed_count += 1
                    self.logger.debug(f"Successfully processed record {index} from {dataset}.{table}")
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error for {dataset}.{table}: {str(e)}")
                    error_count += 1
                    continue
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {dataset}.{table}: {str(e)}", exc_info=True)
                    error_count += 1
                    continue
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            final_data.to_csv(output_path, index=False)
            self.logger.info(f"Completed JSON processing. Processed: {processed_count}, Errors: {error_count}")
            self.logger.info(f"Saved unnested data to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Fatal error in process_unnest_json: {str(e)}", exc_info=True)
            raise

    def remove_unwanted_records(self):
        """Remove unwanted records from the dataset."""
        self.logger.info("Starting removal of unwanted records")
        # Implementation pending
        self.logger.warning("remove_unwanted_records function is not implemented yet")
        pass

    def insert_to_bigquery(self):
        """Inserting data to BigQuery"""
        self.logger.info("Starting to insert data to BigQuery")
        df = pd.read_csv('output/data_baru.csv')
        project_id = 'x-marketing'
        bq.to_gbq(df, 'data_catalog.test', project_id, if_exists='replace', credentials=self.credentials)

    def run(self):
        """Execute the complete pipeline."""
        self.logger.info("Starting data pipeline execution")
        try:
            data = self.get_crm_source()
            data2 = self.tabulate_sample_records(data)
            self.process_to_csv(data2)
            self.process_unnest_json()
            self.remove_unwanted_records()
            self.insert_to_bigquery()
            self.logger.info("Data pipeline completed successfully")
            
        except Exception as e:
            self.logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
            raise

def main():
    pipeline = DataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()