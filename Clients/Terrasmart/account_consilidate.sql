CREATE OR REPLACE TABLE `x-marketing.terrasmart.account_consilidate` AS 

WITH month_year AS (
SELECT AS STRUCT
    DATE_SUB(_date, INTERVAL 1 DAY)  AS max_date, 
    DATE_SUB(_date, INTERVAL 1 MONTH) AS _extract_date,
    DATE_SUB(_date, INTERVAL 1 YEAR) AS previous_year,
     EXTRACT(MONTH FROM _date) AS _Month,
     EXTRACT(YEAR FROM _date) AS _year,
    _account
    
  FROM 
  (SELECT DISTINCT _account FROM `x-marketing.terrasmart.account_90days_score`
  UNION DISTINCT 
  SELECT DISTINCT _account FROM `x-marketing.terrasmart.opportunity_with_account_90days_score` )
   CROSS JOIN UNNEST(GENERATE_DATE_ARRAY('2022-01-01',DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS _date
  ORDER BY
    1 DESC
),current_data AS (
   SELECT _Year,
   EXTRACT(MONTH FROM _extract_date) AS _Month,
    _extract_date,
    DATE_SUB(_extract_date, INTERVAL 1 YEAR) previous_years,
    _account, 
SUM(_monthly_account_score) AS _monthly_account_score
FROM `x-marketing.terrasmart.account_90days_score`
GROUP BY _account, _Year, EXTRACT(MONTH FROM _extract_date) ,_extract_date
), previous_data AS (
   SELECT _Year,
   EXTRACT(MONTH FROM _extract_date) AS _Month,
    _extract_date ,DATE_ADD(_extract_date, INTERVAL 1 YEAR) previous_year,
    _account, 
SUM(_monthly_account_score) AS _previous_monthly_account_score
FROM `x-marketing.terrasmart.account_90days_score`
GROUP BY _account, _Year, EXTRACT(MONTH FROM _extract_date) ,_extract_date
), account_score AS (
   SELECT current_data.*,
previous_data._previous_monthly_account_score
 FROM current_data 
LEFT JOIN previous_data ON current_data._extract_date = previous_data.previous_year AND current_data._account = previous_data._account
), opportunity AS ( 
  SELECT _account, 
COUNT(_opportunity_id) AS _no_opportunity, 
SUM(system_size_in_mw__c) AS _sum_system_size_in_mw__c,
SUM(total_system_size_auto_calc__c) AS total_system_size_auto_calc__c,
DATE_TRUNC(CAST(_createdate AS DATE), MONTH) AS _created_month,
EXTRACT(YEAR FROM _createdate) AS _year, 
EXTRACT(MONTH FROM _createdate) AS _month, 
FROM `x-marketing.terrasmart.opportunity_with_account_90days_score`
GROUP BY _account,DATE_TRUNC(CAST(_createdate AS DATE), MONTH), EXTRACT(YEAR FROM _createdate) ,  EXTRACT(MONTH FROM _createdate)
),previous_opportunity AS (
  SELECT _account, 
COUNT(_opportunity_id) AS _no_opportunity, 
SUM(system_size_in_mw__c) AS _sum_system_size_in_mw__c,
SUM(total_system_size_auto_calc__c) AS total_system_size_auto_calc__c,
DATE_TRUNC(CAST(_createdate AS DATE), MONTH) AS _created_month,
DATE_ADD(DATE_TRUNC(CAST(_createdate AS DATE), MONTH), INTERVAL 1 YEAR) AS _previous_created_month,
EXTRACT(YEAR FROM _createdate) AS _year, 
EXTRACT(MONTH FROM _createdate) AS _month, 
FROM `x-marketing.terrasmart.opportunity_with_account_90days_score`
GROUP BY _account,DATE_TRUNC(CAST(_createdate AS DATE), MONTH), EXTRACT(YEAR FROM _createdate) ,  EXTRACT(MONTH FROM _createdate),DATE_ADD(DATE_TRUNC(CAST(_createdate AS DATE), MONTH), INTERVAL 1 YEAR) 
), opps AS ( SELECT opportunity.*,
previous_opportunity._no_opportunity AS _previous_no_opportunity,
previous_opportunity._sum_system_size_in_mw__c AS _previous_sum_system_size_in_mw__c ,
previous_opportunity.total_system_size_auto_calc__c AS _previous_total_system_size_auto_calc__c
FROM opportunity
LEFT JOIN previous_opportunity ON opportunity._created_month = previous_opportunity._previous_created_month AND opportunity._account = previous_opportunity._account
) SELECT month_year.* ,
account_score._monthly_account_score,
account_score._previous_monthly_account_score,
opps.* EXCEPT (_created_month,_year,_month,_account)
FROM month_year
LEFT JOIN account_score ON month_year._extract_date = account_score._extract_date AND month_year._account = account_score._account
LEFT JOIN opps ON month_year._extract_date = opps._created_month AND month_year._account = opps._account
