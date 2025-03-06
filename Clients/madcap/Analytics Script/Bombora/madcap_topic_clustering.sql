TRUNCATE TABLE `x-marketing.madcap.topic_clustering`;
INSERT INTO `x-marketing.madcap.topic_clustering` (
  _domain,
  _companyrevenue,
  _hqzip,
  _countrycompositescoredelta,
  _countrycompositescore,
  _clustername,
  _intentcountry,
  _companysize,
  _companyname,
  _topicid,
  _industry,
  _address,
  _hqcountry,
  _hqcity,
  _hqstate,
  _topicname,
  _compositescoredelta,
  _date,
  _compositescore,
  topic_count,
  avg_surge_score
)
WITH target_bombora AS (
  SELECT DISTINCT 
    _domain, 
    _companyrevenue, 
    _hqzip, 
    _countrycompositescoredelta, 
    CAST(_countrycompositescore AS INT64) AS _countrycompositescore, 
    _clustername,
    _intentcountry, 
    _companysize, 
    _companyname, 
    _topicid, 
    _industry, 
    CONCAT(_address1," ",_address2) AS _address, 
    _hqcountry, 
    _hqcity,
    _hqstate, 
    CAST(_compositescore AS INT64) AS _compositescore, 
    _topicname, 
    _compositescoredelta,
    _timestamp AS _date
  FROM `x-marketing.madcap_mysql.madcap_db_bombora_comprehensive`
),
total_date AS (
  SELECT 
    * EXCEPT (_compositescore),
    CASE 
      WHEN _compositescore IS NULL THEN 0 
      ELSE CAST(_compositescore AS INT64) 
    END AS _compositescore,
    COUNT(_topicname) OVER (PARTITION BY _date,_domain) AS topic_count
  FROM target_bombora
),
average_surge_score AS (
  SELECT 
    *, 
    CASE 
      WHEN topic_count > 0 THEN _compositescore / topic_count 
      ELSE 0 
    END AS avg_surge_score
  FROM total_date
)
SELECT 
  * 
FROM average_surge_score;