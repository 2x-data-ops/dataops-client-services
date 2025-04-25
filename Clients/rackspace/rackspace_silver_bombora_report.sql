TRUNCATE TABLE `x-marketing.rackspace.bombora_report`;
INSERT INTO `x-marketing.rackspace.bombora_report` (
  _hqzip,
  _companyrevenue,
  _countrycompositescoredelta,
  _clustername,
  _address2,
  _intentcountry,
  _companysize,
  _companyname,
  _topicid,
  _industry,
  _address1,
  _hqcountry,
  _hqcity,
  _hqstate,
  _topicname,
  _domain,
  _compositescoredelta,
  _timestamp,
  _countrycompositescore,
  _compositescore,
  _company_cluster_streak,
  _company_streak,
  _company_topic_streak

)
WITH base_data AS (
  SELECT
    * EXCEPT (
      _sdc_table_version,
      _sdc_received_at,
      _sdc_sequence,
      _id,
      _batchid,
      _sdc_batched_at,
      _rownumber,
      _countrycompositescore,
      _compositescore,
      _timestamp
    ),
    CAST(_timestamp AS DATE) AS _timestamp,
    CASE
      WHEN _countrycompositescore LIKE '%.0' THEN CAST(REGEXP_EXTRACT(_countrycompositescore, r'\d+') AS INT64)
      WHEN _countrycompositescore != '' THEN CAST(_countrycompositescore AS INT64)
      ELSE NULL
    END AS _countrycompositescore,
    CASE
      WHEN _compositescore LIKE '%.0' THEN CAST(REGEXP_EXTRACT(_compositescore , r'\d+') AS INT64)
      WHEN _compositescore != '' THEN CAST(_compositescore AS INT64)
      ELSE NULL
    END AS _compositescore
  FROM `x-marketing.rackspace_mysql_2.db_bombora_account`
),
-- CLUSTER-LEVEL STREAK
valid_weeks AS (
  SELECT DISTINCT
    _companyname,
    _clustername,
    _timestamp
  FROM base_data
  WHERE _compositescore IS NOT NULL 
    OR _compositescoredelta = 'New'
),
with_gaps AS (
  SELECT
    *,
    LAG(_timestamp) OVER (PARTITION BY _companyname, _clustername ORDER BY _timestamp) AS prev_week,
    DATE_DIFF(_timestamp, LAG(_timestamp) OVER (PARTITION BY _companyname, _clustername ORDER BY _timestamp), DAY) AS gap_days
  FROM valid_weeks
),
with_streak_groups AS (
  SELECT 
    *,
    SUM(IF(prev_week IS NULL OR gap_days > 7, 1, 0)) OVER (
      PARTITION BY _companyname, _clustername ORDER BY _timestamp
    ) AS streak_group
  FROM with_gaps
),
streaked_weeks AS (
  SELECT
    _companyname,
    _clustername,
    _timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY _companyname, _clustername, streak_group ORDER BY _timestamp
    ) AS _company_cluster_streak
  FROM with_streak_groups
),
-- COMPANY-LEVEL STREAK
company_valid_weeks AS (
  SELECT DISTINCT
    _companyname,
    _timestamp
  FROM base_data
  WHERE _compositescore IS NOT NULL 
    OR _compositescoredelta = 'New'
),
company_with_gaps AS (
  SELECT
    *,
    LAG(_timestamp) OVER (PARTITION BY _companyname ORDER BY _timestamp) AS prev_week,
    DATE_DIFF(_timestamp, LAG(_timestamp) OVER (PARTITION BY _companyname ORDER BY _timestamp), DAY) AS gap_days
  FROM company_valid_weeks
),
company_streak_groups AS (
  SELECT *,
    SUM(IF(prev_week IS NULL OR gap_days > 7, 1, 0)) OVER (
      PARTITION BY _companyname ORDER BY _timestamp
    ) AS streak_group
  FROM company_with_gaps
),
company_streaked AS (
  SELECT
    _companyname,
    _timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY _companyname, streak_group ORDER BY _timestamp
    ) AS _company_streak
  FROM company_streak_groups
),
-- TOPIC LEVEL STREAK
topic_valid_weeks AS (
  SELECT DISTINCT
    _companyname,
    _topicname,
    _timestamp
  FROM base_data
  WHERE _compositescore IS NOT NULL 
    OR _compositescoredelta = 'New'
),
topic_with_gaps AS (
  SELECT
    *,
    LAG(_timestamp) OVER (PARTITION BY _companyname, _topicname ORDER BY _timestamp) AS prev_week,
    DATE_DIFF(_timestamp, LAG(_timestamp) OVER (PARTITION BY _companyname, _topicname ORDER BY _timestamp), DAY) AS gap_days
  FROM topic_valid_weeks
),
topic_streak_groups AS (
  SELECT *,
    SUM(IF(prev_week IS NULL OR gap_days > 7, 1, 0)) OVER (
      PARTITION BY _companyname, _topicname ORDER BY _timestamp
    ) AS streak_group
  FROM topic_with_gaps
),
topic_streaked AS (
  SELECT
    _companyname,
    _topicname,
    _timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY _companyname, _topicname, streak_group ORDER BY _timestamp
    ) AS _company_topic_streak
  FROM topic_streak_groups
),
final_output AS (
  SELECT
    b.*,
    s._company_cluster_streak,
    cs._company_streak,
    ts._company_topic_streak
  FROM base_data b
  LEFT JOIN streaked_weeks s
    ON b._companyname = s._companyname
    AND b._clustername = s._clustername
    AND b._timestamp = s._timestamp
  LEFT JOIN company_streaked cs
    ON b._companyname = cs._companyname
    AND b._timestamp = cs._timestamp
  LEFT JOIN topic_streaked ts
    ON b._companyname = ts._companyname
    AND b._topicname = ts._topicname
    AND b._timestamp = ts._timestamp
)
SELECT 
  *
FROM final_output
ORDER BY _companyname, _clustername, _timestamp, _topicname;