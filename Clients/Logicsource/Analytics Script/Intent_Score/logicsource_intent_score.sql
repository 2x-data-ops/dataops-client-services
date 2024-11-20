CREATE OR REPLACE TABLE `logicsource.zoominfo_intent_score` AS
WITH account AS (
SELECT 
_company, 
_domain, 
_revenue, 
_industry, 
_company_segment, 
_employee_range, 
_employee_range_c, 
_numberofemployees, 
_annualrevenue, 
_annual_revenue_range, 
_annual_revenue_range_c, 
_source,
days_since_last_engaged, 
_score_new, 
_last_engagement_date, 
_total_score, 
total_employee, total_score_divide_2, total_score, max_score, _total_score_icp_intent, legend,source_zi_intent,_zi_intent
FROM logicsource.zoominfo_account_engagement_scoring
WHERE _domain IS NOT NULL

)
,dummy_date AS (
  SELECT *, 
  DENSE_RANK() OVER (ORDER BY _date DESC) AS dense_rank  FROM (
  SELECT DISTINCT CAST(_lastsignal AS DATE) AS _date ,CAST(_exporteddate AS DATE) AS _exporteddate,
  FROM `x-marketing.logicsource_mysql.db_zoominfo_intent`
  )
)
,all_account AS ( 
SELECT acc.*, report._score AS _intent_score,report._topic,
_date,
_exporteddate,
EXTRACT(WEEK FROM _date)-1 AS _week, 
EXTRACT(YEAR FROM _date) AS _year, 
CAST(report._score AS INT64) AS _score,
 AVG(CAST(report._score AS INT64))  AS _avgCompositeScore,
FROM account acc 
--CROSS JOIN dummy_date 
LEFT JOIN (SELECT _score,_topic,_domain , CAST(_lastsignal AS DATE) AS _date,CAST(_exporteddate AS DATE) AS _exporteddate,
FROM `x-marketing.logicsource_mysql.db_zoominfo_intent` ) report ON acc._domain = report._domain

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
), _engagement AS (
SELECT  
     *,
    SUM(_emailOpened) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_opened,
    SUM(_emailClicked) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_clicked,
    SUM(_formfilled) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_formfilled,
    SUM(_paidads) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_paidads,
    SUM(_organicsocial) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_organicsocial,
    SUM(_webvisit) OVER(PARTITION BY _domain ORDER BY _week, _year) AS running_webvisit,
     FROM (
      SELECT
      _domain,EXTRACT(WEEK FROM _date) AS _week,  EXTRACT(YEAR FROM _date) AS _year,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      SUM( CASE WHEN _engagement = 'Form Filled' THEN 1 ELSE 0 END) AS _formfilled,
      SUM( CASE WHEN _engagement = 'Paid Ads' THEN 1 ELSE 0 END) AS _paidads,
      SUM( CASE WHEN _engagement = 'Organic Social' THEN 1 ELSE 0 END) AS _organicsocial,
      SUM( CASE WHEN _engagement = 'Web Visit' THEN 1 ELSE 0 END) AS _webvisit,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM ( SELECT * FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked','Form Filled','Paid Ads','Organic Social','Web Visit') AND EXTRACT(DATE FROM _date) BETWEEN (SELECT MIN(_date) FROM dummy_date) AND CURRENT_DATE()
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
    ) a
    --WHERE _domain = 'foodtravelexperts.com'
   ORDER BY 1, 3 DESC, 2 DESC
) SELECT all_account.*, 
_engagement.* EXCEPT (_domain,_week,_year),
CASE WHEN _source = 'Target' THEN "Existing"  ELSE "Net New" END AS _account_status,
CAST(NULL AS STRING) AS _intent

FROM all_account 
LEFT JOIN _engagement ON (all_account._domain = _engagement._domain AND 
all_account._week = _engagement._week AND
all_account._year = _engagement._year )

;


#Set NULLS to 0 for aggregation in
UPDATE `logicsource.zoominfo_intent_score` 
SET running_opened = CASE WHEN running_opened IS NOT NULL THEN running_opened ELSE 0 END,
running_clicked = CASE WHEN running_clicked IS NOT NULL THEN running_clicked ELSE 0 END,
running_formfilled = CASE WHEN running_formfilled IS NOT NULL THEN running_formfilled ELSE 0 END,
running_paidads = CASE WHEN running_paidads IS NOT NULL THEN running_paidads ELSE 0 END,
running_organicsocial = CASE WHEN running_organicsocial IS NOT NULL THEN running_organicsocial ELSE 0 END,
running_webvisit = CASE WHEN running_webvisit IS NOT NULL THEN running_webvisit ELSE 0 END,
_avgCompositeScore = CASE WHEN _avgCompositeScore IS NOT NULL THEN _avgCompositeScore ELSE 0 END,
max_score = CASE WHEN max_score IS NOT NULL THEN max_score ELSE 0 END
WHERE _domain IS NOT NULL;



#Set Intent based on the rules on dashboard
UPDATE `logicsource.zoominfo_intent_score`
SET _intent = 
  CASE 
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore >= 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore < 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore >= 60  AND _total_score_icp_intent
 < 60 THEN "Medium"
        WHEN /* REGEXP_CONTAINS(CAST(_tier AS STRING),'1|2') AND */ _avgCompositeScore < 60 AND _total_score_icp_intent
 < 60 THEN "Low"
        /* WHEN _tier = 3 AND _avgCompositeScore >= 60  AND _total_score_icp_intent
 >= 60 THEN "High"
        WHEN _tier = 3 AND _avgCompositeScore < 60  AND _total_score_icp_intent
 >= 80 THEN "High"
        WHEN _tier = 3 AND _avgCompositeScore >= 60  AND _total_score_icp_intent
 < 60 THEN "Medium"
        WHEN _tier = 3 AND _avgCompositeScore < 60  AND _total_score_icp_intent
 BETWEEN 60 AND 79 THEN "Medium"
        WHEN _tier = 3 AND _avgCompositeScore < 60 AND _total_score_icp_intent
 < 60 THEN "Low" */
    END
WHERE _domain IS NOT NULL;