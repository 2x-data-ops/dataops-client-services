--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

-- No bombora report so it's being excluded.
--CREATE OR REPLACE TABLE `x-marketing.logicsource.db_tam_database_report` AS
TRUNCATE TABLE `x-marketing.logicsource.db_tam_database_report`;

INSERT INTO `x-marketing.logicsource.db_tam_database_report` (
  _sfdcaccountid,	
  _email,	
  _seniority,	
  _company,	
  _industry,	
  _tier,
  _revenue,	
  _domain,	
  _id,	
  _total_contacts,
  _nonManagerial,
  _manager,
  _director,
  _seniorExec,
  _date,	
  _week,
  _year,
  new_contacts,
  _emailOpened,
  _emailClicked,
  _formFilled,
  _adsClicked,
  _contentClicked,
  _webVisited,
  _t90_days_score,
  _t90days_intent,	
  _t90days_first_party_breakdown,	
  _third_party_breakdown
)
WITH
contacts AS (
     SELECT 
lead.id AS _id,
 email AS _email,
lead.name AS _name, 
COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value) AS _domain,
title AS _jobtitle,
 CASE
          WHEN LOWER(title) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%Senior Counsel%") THEN "VP"  
          WHEN LOWER(title) LIKE LOWER("%General Counsel%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Founder%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%C-Level%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CDO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CIO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CMO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CFO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CEO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Chief%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%COO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%srvp%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%SR VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%EVP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%SVP%") THEN "Senior VP" 
          WHEN LOWER(title) LIKE LOWER("%V.P%") THEN "VP" 
          WHEN LOWER(title) LIKE LOWER("%VP%") THEN "VP" 
          WHEN LOWER(title) LIKE LOWER("%Vice Pres%") THEN "VP" 
          WHEN LOWER(title) LIKE LOWER("%V P%") THEN "VP" 
          WHEN LOWER(title) LIKE LOWER("%President%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Director%") THEN "Director" 
          WHEN LOWER(title) LIKE LOWER("%CTO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Dir%") THEN "Director" 
          WHEN LOWER(title) LIKE LOWER("%Dir.%") THEN "Director" 
          WHEN LOWER(title) LIKE LOWER("%MDR%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%MD%") THEN "Director" 
          WHEN LOWER(title) LIKE LOWER("%GM%") THEN "Director" 
          WHEN LOWER(title) LIKE LOWER("%Head%") THEN "VP" 
          WHEN LOWER(title) LIKE LOWER("%Manager%") THEN "Manager" 
          WHEN LOWER(title) LIKE LOWER("%escrow%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%cross%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%crosse%") THEN "Non-Manager" 
          WHEN LOWER(title) LIKE LOWER("%Partner%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%CRO%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Chairman%") THEN "C-Level" 
          WHEN LOWER(title) LIKE LOWER("%Owner%") THEN "C-Level"
          WHEN LOWER(title) LIKE LOWER("%Team Lead%") THEN "Manager"
        END AS _seniority,
        properties.job_function.value AS _function,
          lead.phone AS _phone,
          company AS _company, 
          CAST(lead.annualrevenue AS STRING) AS _revenue,
           lead.industry AS _industry,
           city AS _city, 
        state AS _state,
        country AS _country,
        '' AS _persona,
        property_lifecyclestage.value AS _lifecycleStage,
        form_submissions,
        lead.createddate AS _createddate,
         convertedaccountid AS _sfdcaccountid,
        convertedcontactid AS _sfdccontactid,
        
FROM `x-marketing.logicsource_salesforce.Lead`  lead
LEFT JOIN `x-marketing.logicsource_hubspot.contacts` k ON lead.id = k.properties.salesforceleadid.value
LEFT JOIN `x-marketing.logicsource_salesforce.Account`  acc ON lead.convertedaccountid = acc.id
WHERE lead.isdeleted IS FALSE
--QUALIFY ROW_NUMBER() OVER( PARTITION BY email, name ORDER BY id DESC) = 1
),
new_weekly_contacts AS (
  SELECT
    DISTINCT _domain,
    COUNT(DISTINCT _id) AS new_contacts,
    EXTRACT(WEEK FROM _createddate) AS _week,
    EXTRACT(YEAR FROM _createddate) AS _year,
  FROM
    contacts
  GROUP BY
    1, 3, 4
)
,

companies AS (
  SELECT 
              property_createdate.value AS _createddate,
              --property_domain.value 
              companyid ,
              property_domain.value AS _domain,
              properties.salesforceaccountid.value AS salesforceaccountid 
            FROM `x-marketing.logicsource_hubspot.companies`
),

accounts AS (
  SELECT 
              DISTINCT acc.id as _sfdcaccountid, 
              _email,
              _seniority,
              name AS _company, 
              industry AS _industry, 
              CAST(NULL AS INTEGER) AS _tier,
              _revenue, 
             hub._domain AS _domain, 
              _id 
              -- IF(acc.target_account__c = true, 1, 0) AS _target_account
            FROM
            `x-marketing.logicsource_salesforce.Account` acc 
           LEFT JOIN companies AS hub ON acc.id = hub.salesforceaccountid

            --LEFT JOIN (SELECT DISTINCT  accountid FROM `x-marketing.logicsource_salesforce.Contact` 
--UNION ALL 
--SELECT DISTINCT  matched_account__c FROM `x-marketing.logicsource_salesforce.Lead` ) lead ON acc.id = lead.accountid
             LEFT JOIN  contacts ON acc.id = contacts._sfdcaccountid
             WHERE isdeleted IS FALSE
),

tam_account AS (
        SELECT 
          DISTINCT *,
          COUNT(DISTINCT _id) OVER(PARTITION BY  _sfdcaccountid) AS _total_contacts,
          COUNT(DISTINCT IF(_seniority IN ('Non-Manager', 'Other', 'Executive', 'Student', 'Security Administrator/Analyst'), _id, NULL )) OVER(PARTITION BY _sfdcaccountid) AS _nonManagerial,
          COUNT(DISTINCT IF(REGEXP_CONTAINS(_seniority, 'Manager'), _id, NULL )) OVER(PARTITION BY _sfdcaccountid) AS _manager,
          COUNT(DISTINCT IF(REGEXP_CONTAINS(_seniority, 'Director'), _id, NULL )) OVER(PARTITION BY _sfdcaccountid) AS _director,
          COUNT(DISTINCT IF(REGEXP_CONTAINS(_seniority, 'Senior Exec|Partner'),_id, NULL )) OVER(PARTITION BY _sfdcaccountid) AS _seniorExec, 
        FROM accounts
        QUALIFY ROW_NUMBER() OVER(PARTITION BY  _sfdcaccountid ORDER BY _tier ASC, _company DESC, _industry DESC, _sfdcaccountid DESC, _revenue DESC) = 1 
),
dummy_dates AS (
  SELECT 
    DATE_TRUNC(_date, WEEK(MONDAY)) AS _date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2019-12-01', INTERVAL -1 WEEK )) AS _date
),
# Each domain needs to be shown regardless if they are part of the bombora report
/* dummy_dates AS ( 
  SELECT DISTINCT EXTRACT(DATE FROM _date) AS _date FROM `logicsource.bombora_surge_report` ORDER BY 1
), 
intent_data AS (
# Combination of Bombora & Target account. Those Target account that's not part of the report will have NULL avgCompositeScore and _week = current week.
  SELECT 
    DISTINCT *,  
    EXTRACT(WEEK FROM _date)-1 AS _week, 
    EXTRACT(YEAR FROM _date) AS _year,
    ROUND(AVG(_weekly_avgCompositeScore) OVER(PARTITION BY _domain, DATE_SUB(DATE(_date), INTERVAL 90 DAY) ), 1) AS _ytd_avgCompositescore,

  FROM (
    SELECT 
      accs.*, 
      ddates.*, 
      MAX(COALESCE(CAST(report._averagecompositescore AS INT64), 0 )) OVER(PARTITION BY accs._domain ORDER BY ddates._date) AS _weekly_avgCompositeScore
    FROM 
      tam_account accs 
    CROSS JOIN dummy_dates ddates
    LEFT JOIN 
      `spirion.bombora_surge_report` report 
      ON accs._domain = report._domain AND ddates._date = EXTRACT(DATE FROM report._date)
  )
), */

engagement AS (
  SELECT 
      DISTINCT _domain,   
      _week,
      _year,
      COUNT(DISTINCT CASE WHEN _engagement = 'Email Opened' THEN CONCAT(_email, _contentTitle) END ) AS _emailOpened,
      COUNT(DISTINCT CASE WHEN _engagement = 'Email Clicked' THEN CONCAT(_email, _contentTitle) END ) AS _emailClicked,
      COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' THEN CONCAT(_email, _contentTitle) END ) AS _formFilled,
      COUNT(DISTINCT CASE WHEN _engagement = 'Ad Clicks' THEN CONCAT(_email, _contentTitle) END ) AS _adsClicked,
      COUNT(DISTINCT CASE WHEN _engagement = 'Content Engagement' THEN CONCAT(_email, _contentTitle) END ) AS _contentClicked,
      COUNT(DISTINCT CASE WHEN _engagement = 'Web Visit' THEN CONCAT(_email, _contentTitle) END ) AS _webVisited,
    FROM 
      `x-marketing.logicsource.db_consolidated_engagements_log` eng
    /* JOIN 
      (SELECT DISTINCT _sfdcaccountid, _email FROM `spirion_mysql.db_all_contacts` WHERE _email IS NOT NULL) USING(_email) */
    WHERE 
      EXTRACT(YEAR FROM _date) = 2022
      AND _domain IS NOT NULL
      AND _week IS NOT NULL
    GROUP BY 
      1, 2, 3
),
first_party_score AS (
  SELECT 
    DISTINCT _domain, 
    _extract_date,
    EXTRACT(WEEK FROM _extract_date) - 1 AS _week, -- Minus 1 as the score is referring to the week before.
    EXTRACT(YEAR FROM _extract_date) AS _year,
   (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_paid_ads_score , 0)+ COALESCE(_quarterly_web_score, 0)+ COALESCE(_quarterly_organic_social_score, 0))AS _t90_days_score
  FROM
    `x-marketing.logicsource.account_90days_score`
),

account_dates AS (
  SELECT 
      accs.*, 
      ddates.*,
      EXTRACT(WEEK FROM _date) AS _week, 
      EXTRACT(YEAR FROM _date) AS _year, 
    FROM 
      tam_account AS accs 
    CROSS JOIN dummy_dates AS ddates
)

SELECT 
  DISTINCT /*  intent_data.*,  */
  main.*, 
  new_weekly_contacts.new_contacts,
  engagement.*EXCEPT(_domain, _week, _year),
  _t90_days_score,
  -- _ytd_first_party_score,
  CAST(NULL AS STRING) AS _t90days_intent,
  -- CAST(NULL AS STRING) AS _weekly_intent,
  IF(_t90_days_score > 14, 'High', 
        IF(_t90_days_score BETWEEN 1 AND 14, 'Low', 
          'No') ) AS _t90days_first_party_breakdown,
  (
    /* CASE
      WHEN (_ytd_avgCompositescore >= 60) THEN "Medium"
      WHEN (_ytd_avgCompositescore < 60) THEN "Low"
      WHEN (_ytd_avgCompositescore IS NULL) THEN "Low"
      ELSE CAST(NULL AS STRING)
    END */
  cAST(NULL AS STRING)) AS _third_party_breakdown
FROM 
  account_dates main
LEFT JOIN
  engagement USING(_domain, _week, _year)
LEFT JOIN 
  first_party_score USING(_domain, _week, _year)
LEFT JOIN
  new_weekly_contacts USING(_domain, _week, _year)
ORDER BY 
  main._domain ASC, main._year DESC , main._week DESC;


#Set YTD Intent based on the rules on dashboard
-- UPDATE `logicsource.db_tam_database_report`  
-- SET _t90days_intent = 
--   CASE 
--         WHEN /* _tier IN ('1', '2') AND */ _ytd_avgCompositeScore >= 60  AND _t90_days_score >= 60 THEN "High"
--         WHEN /* _tier IN ('1', '2') AND */ _ytd_avgCompositeScore < 60  AND _t90_days_score >= 60 THEN "High"
--         WHEN /* _tier IN ('1', '2') AND */ _ytd_avgCompositeScore >= 60  AND _t90_days_score < 60 THEN "Medium"
--         WHEN /* _tier IN ('1', '2') AND */ _ytd_avgCompositeScore < 60 AND _t90_days_score < 60 THEN "Low"
--         WHEN /* _tier = '3' AND */ _ytd_avgCompositeScore >= 60  AND _t90_days_score >= 60 THEN "High"
--         WHEN /* _tier = '3' AND */ _ytd_avgCompositeScore < 60  AND _t90_days_score >= 80 THEN "High"
--         WHEN /* _tier = '3' AND */ _ytd_avgCompositeScore >= 60  AND _t90_days_score < 60 THEN "Medium"
--         WHEN /* _tier = '3' AND */ _ytd_avgCompositeScore < 60  AND _t90_days_score BETWEEN 60 AND 79 THEN "Medium"
--         WHEN /* _tier = '3' AND */ _ytd_avgCompositeScore < 60 AND _t90_days_score < 60 THEN "Low"
--     END
-- WHERE _t90days_intent IS NULL;


#Set weekly Intent based on the rules on dashboard
/* UPDATE `logicsource.db_tam_database_report`  
SET _weekly_intent = 
  CASE 
        WHEN _tier IN ('1', '2') AND _weekly_avgCompositeScore >= 60  AND _weekly_first_party_score >= 60 THEN "High"
        WHEN _tier IN ('1', '2') AND _weekly_avgCompositeScore < 60  AND _weekly_first_party_score >= 60 THEN "High"
        WHEN _tier IN ('1', '2') AND _weekly_avgCompositeScore >= 60  AND _weekly_first_party_score < 60 THEN "Medium"
        WHEN _tier IN ('1', '2') AND _weekly_avgCompositeScore < 60 AND _weekly_first_party_score < 60 THEN "Low"
        WHEN _tier = '3' AND _weekly_avgCompositeScore >= 60  AND _weekly_first_party_score >= 60 THEN "High"
        WHEN _tier = '3' AND _weekly_avgCompositeScore < 60  AND _weekly_first_party_score >= 80 THEN "High"
        WHEN _tier = '3' AND _weekly_avgCompositeScore >= 60  AND _weekly_first_party_score < 60 THEN "Medium"
        WHEN _tier = '3' AND _weekly_avgCompositeScore < 60  AND _weekly_first_party_score BETWEEN 60 AND 79 THEN "Medium"
        WHEN _tier = '3' AND _weekly_avgCompositeScore < 60 AND _weekly_first_party_score < 60 THEN "Low"
    END
WHERE _weekly_intent IS NULL; */


