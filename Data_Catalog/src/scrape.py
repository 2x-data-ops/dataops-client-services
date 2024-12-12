import pandas_gbq as bq
import pandas as pd
import os
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

    def get_data_from_bigquery(self):
        """Fetch raw data from SQL query."""
        query = """
                WITH column_name AS (
                    SELECT
                        table_schema AS _raw_dataset,
                        table_name AS _table,
                        field_path AS _field,
                        CASE
                        WHEN table_schema LIKE '%_hubspot' THEN 'hubspot'
                        WHEN table_schema LIKE '%_salesforce' THEN 'salesforce'
                        WHEN table_schema LIKE '%linkedin%' THEN 'linkedin'
                        WHEN table_schema LIKE '%google%' THEN 'google'
                        WHEN table_schema LIKE '%_pardot' THEN 'pardot'
                        WHEN table_schema LIKE '%_marketo' THEN 'marketo'
                        WHEN table_schema LIKE '%bing%' THEN 'bing'
                        WHEN table_schema LIKE '%facebook%' THEN 'facebook'
                        WHEN table_schema LIKE '%sfmc%' THEN 'sfmc'
                        WHEN table_schema LIKE '%mailchimp%' THEN 'mailchimp'
                        ELSE table_schema
                        END AS _crm_source,
                        data_type AS _data_type
                    FROM `x-marketing.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                    WHERE data_type NOT LIKE 'STRUCT%'
                        AND data_type NOT LIKE '%ARRAY%'
                        AND field_path NOT LIKE '%sdc%'
                        AND table_schema NOT LIKE '%_mysql'
                        AND table_schema NOT LIKE 'analytic%'
                        AND table_name NOT LIKE '_sdc_%'
                        AND table_schema NOT LIKE '%_sheet%'
                    ),
                    table_created_time AS (
                        SELECT DISTINCT
                            table_schema,
                            table_name,
                            TIMESTAMP(creation_time) AS table_created
                        FROM `x-marketing.region-us.INFORMATION_SCHEMA.TABLES`
                    ),
                    main_data AS (
                        SELECT
                            column_name.*,
                            table_created_time.table_created
                        FROM column_name
                        LEFT JOIN table_created_time
                            ON column_name._raw_dataset = table_created_time.table_schema
                            AND column_name._table = table_created_time.table_name
                    )
                    SELECT 
                        *,
                    SPLIT(_raw_dataset, '_')[OFFSET(0)] AS _client_name
                    FROM main_data
                    WHERE _crm_source IN ('hubspot', 'salesforce', 'linkedin', 'google', 'pardot', 'marketo', 'bing', 'facebook', 'sfmc', 'mailchimp')
                """
        try:
            df = bq.read_gbq(query, project_id='x-marketing', credentials=self.credentials)
            df.to_csv('output/1_main_data.csv', index=False)
            self.logger.info(f"Retrieved records from SQL query")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching data from SQL: {str(e)}", exc_info=True)
            raise

    def get_crm_source(self, df):
        """Process dataframe to get distinct dataset-table pairs."""
        self.logger.info(f"Starting to process data for {len(self.crm_sources)} CRM sources")
        all_dataset_table_pairs = []
        
        for source in self.crm_sources:
            self.logger.info(f"Processing source: {source}")
            try:
                filtered_df = df[df['_raw_dataset'].str.contains(source, case=False)][['_raw_dataset', '_table']].drop_duplicates()
                self.logger.info(f"Retrieved {len(filtered_df)} dataset-table pairs for {source}")
                all_dataset_table_pairs.append(filtered_df)
            except Exception as e:
                self.logger.error(f"Error processing {source}: {str(e)}", exc_info=True)
                continue
                
        combined_dataset_table_pairs = pd.concat(all_dataset_table_pairs)
        self.logger.info(f"Total dataset-table pairs retrieved: {len(combined_dataset_table_pairs)}")
        return combined_dataset_table_pairs

    def tabulate_sample_records(self, data):
        self.logger.info(f"Starting to tabulate sample records for {len(data)} dataset-table pairs")
        data2 = []
        total_count = len(data)
        current_source = None
        processed_pairs = 0

        for idx, row in data.iterrows():
            dataset = row['_raw_dataset']
            table = row['_table']
            
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
            
            try:
                # First get column names
                schema_query = f"""
                    SELECT column_name 
                    FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = '{table}'
                    AND column_name NOT IN ('_sdc_batched_at', '_sdc_extracted_at', '_sdc_received_at', '_sdc_sequence', '_sdc_table_version')
                """
                
                columns = bq.read_gbq(
                    schema_query,
                    project_id='x-marketing',
                    credentials=self.credentials
                )
                
                # Build ANY_VALUE query using column names
                col_list = [f"ANY_VALUE({col['column_name']}) as {col['column_name']}" for _, col in columns.iterrows()]
                cols_str = ', '.join(col_list)
                
                query = f"""
                    WITH sample AS (
                        SELECT *
                        FROM `{dataset}.{table}`
                        TABLESAMPLE SYSTEM (1 PERCENT)
                    )
                    SELECT {cols_str}
                    FROM sample
                    LIMIT 1
                """
                
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
        
        if data2:
            all_data2 = pd.concat(data2)
            self.logger.info(f"Completed tabulating sample records. Processed {len(all_data2)} tables successfully")
            return all_data2
        
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

    def get_additional_column(self):
        """Get the datatype of the column from original source."""
        self.logger.info("Starting to get data types from original source")
        try:
            # Read the source data and unnested data
            source_df = pd.read_csv('output/1_main_data.csv')
            unnested_df = pd.read_csv('output/unnested_records.csv')
            
            # First merge for data_type using all three columns
            merged_df = unnested_df.merge(
                source_df[['_raw_dataset', '_table', '_field', '_data_type']], 
                left_on=['dataset', 'table', 'column'],
                right_on=['_raw_dataset', '_table', '_field'],
                how='left'
            )
            
            # Second merge for table_created and client_name using just dataset and table
            merged_df = merged_df.merge(
                source_df[['_raw_dataset', '_table', 'table_created', '_client_name']].drop_duplicates(),
                left_on=['dataset', 'table'],
                right_on=['_raw_dataset', '_table'],
                how='left'
            )
            
            # Update the data_type column with correct values
            merged_df['data_type'] = merged_df['_data_type']
            
            # Keep only the columns we need
            final_df = merged_df[['dataset', 'table', 'column', 'data_type', 'sample_record', 'table_created', '_client_name']]
            
            # Save back to the same file
            final_df.to_csv('output/4_final_output.csv', index=False)
            self.logger.info("Joined datatype column with main table")
            
        except Exception as e:
            self.logger.error(f"Error getting data types: {str(e)}", exc_info=True)
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
        try:
            # Read CSV and ensure column names are correct
            df = pd.read_csv('output/4_final_output.csv')
            
            # Rename any problematic column if needed
            if '_client_name' in df.columns:
                df = df.rename(columns={'_client_name': 'client_name'})
            
            project_id = 'x-marketing'
            bq.to_gbq(
                df, 
                'data_catalog.data_catalog',
                project_id, 
                if_exists='replace', 
                credentials=self.credentials,
                table_schema=[
                    {'name': 'dataset', 'type': 'STRING'},
                    {'name': 'table', 'type': 'STRING'},
                    {'name': 'column', 'type': 'STRING'},
                    {'name': 'data_type', 'type': 'STRING'},
                    {'name': 'sample_record', 'type': 'STRING'},
                    {'name': 'table_created', 'type': 'STRING'},
                    {'name': 'client_name', 'type': 'STRING'}  # Make sure this matches exactly
                ]
            )
            self.logger.info("Successfully uploaded to BigQuery")
        except Exception as e:
            self.logger.error(f"Error in insert_to_bigquery: {str(e)}")
            raise

    def run(self):
        """Execute the complete pipeline."""
        self.logger.info("Starting data pipeline execution")
        try:
            raw_data = self.get_data_from_bigquery()
            data = self.get_crm_source(raw_data)
            data2 = self.tabulate_sample_records(data)
            self.process_to_csv(data2)
            self.process_unnest_json()
            self.get_additional_column()
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