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
  avg_surge_score,
  _account_name,
  _account_owner,
  _priority,
  _top20
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
  WHERE _topicname IS NOT NULL
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
),
_all_accounts AS (
  SELECT 
    main.*,
    side._account_name,
    CASE
      WHEN _hqstate IN ("AL", "DC", "FL", "GA", "IN", "KY", "MD", "NC", "SC", "TN", "VA", "WV") THEN "Kathy Bernardino"
      WHEN _hqstate IN ("CT", "DE", "MA", "ME", "MI", "NH", "NJ", "NY", "OH", "PA", "RI", "VT") THEN "Bill Herriott"
      WHEN _hqstate IN ("AK", "AZ", "CA", "CO", "HI", "ID", "MT", "NM", "NV", "OR", "UT", "WA") THEN "Michelle Klatt"
      WHEN _hqstate IN ("AR", "IA", "IL", "KS", "LA", "MN", "MO", "MS", "ND", "NE", "OK", "SD", "TX", "WI", "WY") THEN "David Dye"
      ELSE ""
    END AS _account_owner,
    _priority,
    "All Accounts" AS _top20
  FROM average_surge_score main
  LEFT JOIN `x-marketing.madcap.top_20_target_account` side
    ON main._domain = side._domain
),
_top_accounts AS (
  SELECT 
    main.*,
    side._account_name,
    side._account_owner,
    _priority,
    "Top 20 Accounts" AS _top20
  FROM average_surge_score main
  LEFT JOIN `x-marketing.madcap.top_20_target_account` side
    ON main._domain = side._domain
  WHERE side._account_name IS NOT NULL
)
SELECT 
  *
FROM _all_accounts
UNION ALL
SELECT
  *
FROM _top_accounts