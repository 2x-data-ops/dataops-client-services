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