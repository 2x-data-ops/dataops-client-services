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
            )
            SELECT *
            FROM column_name
            WHERE _crm_source IN ('hubspot', 'salesforce', 'linkedin', 'google', 'pardot', 'marketo', 'bing', 'facebook', 'sfmc', 'mailchimp')
        """
        try:
            df = bq.read_gbq(query, project_id='x-marketing', credentials=self.credentials)
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
        try:
            data = pd.read_csv(input_path)
            final_data = pd.DataFrame(columns=["dataset", "table", "column", "sample_record"])
            processed_count = error_count = 0
            
            for index, row in data.iterrows():
                try:
                    json_data = json.loads(row['sample_records'])
                    flattened_data = {}
                    
                    def flatten_json(d, parent_key=''):
                        for k, v in d.items():
                            new_key = f"{parent_key}.{k}" if parent_key else k
                            if isinstance(v, dict):
                                flatten_json(v, new_key)
                            elif isinstance(v, str) and (v.startswith('{') or v.startswith("'{")):
                                try:
                                    parsed = json.loads(v.replace("'", '"'))
                                    flatten_json(parsed, new_key)
                                except:
                                    flattened_data[new_key] = v
                            else:
                                flattened_data[new_key] = v

                    flatten_json(json_data)
                    
                    temp_df = pd.DataFrame({
                        "dataset": [row['dataset']] * len(flattened_data),
                        "table": [row['table']] * len(flattened_data),
                        "column": flattened_data.keys(),
                        "sample_record": [str(v) if v is not None else '' for v in flattened_data.values()]
                    })
                    final_data = pd.concat([final_data, temp_df], ignore_index=True)
                    processed_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing record {index}: {str(e)}")
                    error_count += 1
                    continue

            final_data.to_csv(output_path, index=False)
            return final_data
            
        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}")
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
        df = pd.read_csv('output/unnested_records.csv')
        project_id = 'x-marketing'
        bq.to_gbq(df, 'data_catalog.sample_records', project_id, if_exists='replace', credentials=self.credentials)

    # def join_with_data_catalog(self):
    #     """Joining records to data_catalog"""
    #     self.logger.info("Starting to join records to data_catalog table") 
    #     query =  """
    #                 SELECT
    #                     main.*,
    #                     side.sample_record AS _sample_record
    #                 FROM `x-marketing.data_catalog.data_catalog` main
    #                 LEFT JOIN `x-marketing.data_catalog.test` side
    #                     ON main._raw_dataset = side.dataset
    #                     AND main._table = side.table
    #                     AND main._field = side.column
    #             """

    #     project_id = 'x-marketing'
    #     bq.read_gbq(query, 'data_catalog.test', project_id, credentials=self.credentials)
    def run(self):
        """Execute the complete pipeline."""
        self.logger.info("Starting data pipeline execution")
        try:
            raw_data = self.get_data_from_bigquery()
            data = self.get_crm_source(raw_data)
            data2 = self.tabulate_sample_records(data)
            self.process_to_csv(data2)
            self.process_unnest_json()
            self.remove_unwanted_records()
            # self.insert_to_bigquery()
            self.logger.info("Data pipeline completed successfully")
            
        except Exception as e:
            self.logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
            raise

def main():
    pipeline = DataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()