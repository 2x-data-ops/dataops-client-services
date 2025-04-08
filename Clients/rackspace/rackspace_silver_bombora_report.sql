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
  _timestamp,
  _hqstate,
  _topicname,
  _domain,
  _compositescoredelta,
  _countrycompositescore,
  _compositescore
)
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
    _compositescore
  ),
  CASE
    WHEN _countrycompositescore LIKE '%.0' THEN CAST(CAST(_countrycompositescore AS FLOAT64) AS INT64)
    WHEN _countrycompositescore != '' THEN CAST(_countrycompositescore AS INT64)
    ELSE CAST(NULL AS INT64)
  END AS _countrycompositescore,
  CASE
    WHEN _compositescore LIKE '%.0' THEN CAST(CAST(_compositescore AS FLOAT64) AS INT64)
    WHEN _compositescore != '' THEN CAST(_compositescore AS INT64)
    ELSE CAST(NULL AS INT64)
  END AS _compositescore
FROM `x-marketing.rackspace_mysql_2.db_bombora_account`;