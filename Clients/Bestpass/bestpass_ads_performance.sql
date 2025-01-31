CREATE OR REPLACE TABLE `x-marketing.bestpass.6sense_ads_performance` AS
SELECT
  _id,
  _6senseid,
  _accountreached,
  _accountsnewlyengagedlifetime,
  _accountswithincreasedengagementlifetime,
  _batchid,
  _campaigntype,
  _createdby,
  _datatype,
  _name,
  _rownumber,
  _sdc_batched_at,
  _sdc_received_at,
  _sdc_sequence,
  _sdc_table_version,
  _status,
  _viewthroughs,
  CAST(REGEXP_REPLACE(_spend, r'[\$,]', '')AS FLOAT64) AS _spend,
  CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
  CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
  SAFE_CAST(REGEXP_REPLACE(_ecpc, r'[\$,]', '') AS FLOAT64) AS _ecpc,
  SAFE_CAST(REGEXP_REPLACE(_ecpm, r'[\$,]', '') AS FLOAT64) _ecpm,
  ROUND(SAFE_CAST(REPLACE(_accountctr,'%','') AS FLOAT64) / 100, 4) AS _accountctr,
  ROUND(SAFE_CAST(REPLACE(_accountvtr,'%','') AS FLOAT64) / 100, 4) AS _accountvtr,
  ROUND(SAFE_CAST(REPLACE(_ctr,'%','') AS FLOAT64) / 100, 4) AS _ctr,
  CASE 
    WHEN _budget = '-' THEN NULL
    ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[\$,]', '') AS FLOAT64)
  END AS _budget,
  CASE 
    WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
    WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date) 
  END AS _date,
  CASE
    WHEN _enddate = '-' THEN NULL
    WHEN _enddate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _enddate)
    WHEN _enddate LIKE '%-%' THEN PARSE_DATE('%F', _enddate)	      
  END AS _enddate,
  CASE
    WHEN _startdate = '-' THEN NULL
    WHEN _startdate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _startdate)
    WHEN _startdate LIKE '%-%' THEN PARSE_DATE('%F', _startdate)
  END AS _startdate,
  ROUND(SAFE_CAST(REPLACE(_viewability,'%','') AS FLOAT64) / 100, 4) AS _viewability,
  ROUND(SAFE_CAST(REPLACE(_vtr,'%','') AS FLOAT64) / 100, 4) AS _vtr,
FROM `x-marketing.bestpass_mysql.db_campaign_performance_6sense`;

CREATE OR REPLACE TABLE `x-marketing.bestpass.6sense_account_reached` AS
SELECT
  CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
  CASE
    WHEN _extractdate = '-' THEN NULL
    WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _extractdate)
    WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
  END AS _extract_date,
  _websiteengagement AS _website_engagement,
  _influencedformfills AS _influenced_form_fills,
  _campaignid AS _campaign_id,
  _sdc_table_version,
  CAST(REGEXP_REPLACE(_spend, r'[\$,]', '')AS FLOAT64) AS _spend,
  _campaignname AS _campaign_name,
  _6sensecountry AS _6sense_country,
  _sdc_received_at,
  _sdc_sequence,
  _6sensedomain AS _6sense_domain,
  _id,
  _batchid AS _batch_id,
  _6sensecompanyname AS _6sense_company_name,
  _sdc_batched_at,
  CASE
    WHEN _latestimpression = '-' THEN NULL
    WHEN _latestimpression LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _latestimpression)
    WHEN _latestimpression LIKE '%-%' THEN PARSE_DATE('%F', _latestimpression)
  END AS _latest_impression,
  CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
  _rownumber AS _row_number
FROM `x-marketing.bestpass_mysql.db_account_reached_6sense`;

CREATE OR REPLACE TABLE `x-marketing.bestpass.6sense_account_segment` AS
SELECT
  CASE
    WHEN _extractdate = '-' THEN NULL
    WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _extractdate)
    WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
  END AS _extract_date,
  IF(_industrylegacy = '', NULL, _industrylegacy) AS _industry_legacy,
  IF(_industry = '', NULL, _industry) AS _industry,
  _sdc_table_version,
  IF(_6senseemployeerange = '', NULL, _6senseemployeerange) AS _6sense_employee_range,
  _segmentname AS _segment_name,
  IF(_6sensecountry = '', NULL, _6sensecountry) AS _6sense_country,
  IF(_6senserevenuerange = '', NULL, _6senserevenuerange) AS _6sense_revenue_range,
  _sdc_received_at,
  _sdc_sequence,
  IF(_6sensedomain = '', NULL, _6sensedomain) AS _6sense_domain,
  _id,
  _batchid AS _batch_id,
  _6sensecompanyname AS _6sense_company_name,
  _sdc_batched_at,
  _rownumber AS _row_number
FROM `x-marketing.bestpass_mysql.db_segment_accounts_6sense`;