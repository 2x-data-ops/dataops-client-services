------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

#This is the script to power the Account Health reporting page



-- CREATE OR REPLACE TABLE terrasmart.db_consolidated_engagements_log AS
TRUNCATE TABLE `terrasmart.db_consolidated_engagements_log`;
INSERT INTO `terrasmart.db_consolidated_engagements_log`
-- CREATE OR REPLACE TABLE terrasmart.db_consolidated_engagements_log AS
WITH 
#Query to pull all the contacts in the leads table from Marketo
  contacts AS (
  SELECT 
 contacts.*,
 _ebos, 
 _utilityprojects, 
 _tracker, 
 _linkedinurl AS _acc_linkedinurl, 
 _canopy, 
 _prospect, 
 _fixedtilt, 
 _account, 
 acc._persona AS _acc_persona, 
 _rep, 
 _midwest,
 CASE WHEN acc._domain IS NOT NULL THEN "Key Account" ELSE "Other Account" END AS _account_segment

 FROM 
 (
 SELECT 
      _id,
      _email,
      _name,
      _domain, 
      _jobtitle,
      _seniority,
      _function,
      _phone,
      _company,
      _revenue,
      _industry,
      _city, 
      _state,
      _country,
      _persona,
      _lifecycleStage,
      _sfdcaccountid,
      _sfdccontactid,
      _contact_type,
      _leadorcontactid,
      CAST(NULL AS STRING) AS _contactlinkedin,
      CAST(NULL AS STRING) AS _accountlinkedin, 
    FROM 
      `terrasmart.db_icp_database_log` tam
      UNION ALL
      SELECT * EXCEPT (rownum)
      FROM (
        SELECT * ,
        ROW_NUMBER() OVER(
            PARTITION BY _email
            ORDER BY _id DESC
          ) 
          AS rownum
          FROM (
            SELECT 
            DISTINCT 
            _id AS _id, 
            CONCAT(_contactname,"@",
            CASE WHEN _accountdomain LIKE "%www.%" THEN ( REGEXP_REPLACE(
              SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
              '(www\\.|/+$)',''))
              ELSE (
                REGEXP_REPLACE(_accountdomain,'(https?://|www\\.|/+$)',''))END )  AS _email,
                _contactname,
                CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        _contacttitle,
        CAST(NULL AS STRING) AS _seniority,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _phone,
        _accountname AS    _company,
        CAST(NULL AS STRING) AS _revenue,
        CAST(NULL AS STRING) AS _industry,
        CAST(NULL AS STRING) AS _city, 
        CAST(NULL AS STRING) AS _state,
        _contactcountry AS    _country,
        CAST(NULL AS STRING) AS    _persona,
        CAST(NULL AS STRING) AS    _lifecycleStage,
        CAST(NULL AS STRING) AS    _sfdcaccountid,
        CAST(NULL AS STRING) AS    _sfdccontactid,
        CAST(NULL AS STRING) AS   _contact_type,
        CAST(NULL AS STRING) AS   _leadorcontactid,
        _contactlinkedin,
        _accountlinkedin, 
        FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`
        )
        ) contacts
        WHERE rownum = 1
 ) contacts
 LEFT JOIN  (SELECT * FROM `x-marketing.terrasmart_mysql.db_key_accounts` 
 WHERE _linkedinurl <> 'www.linkedin.com/company/centrica-business-solutions/')  acc ON contacts._domain = acc._domain
)
  ,
  accounts AS (
      SELECT 
        DISTINCT _domain, 
        CAST(NULL AS INT64) AS _id,
        CAST(NULL AS STRING) AS _name, 
        -- CAST(NULL AS STRING) AS _lastname,
        CAST(NULL AS STRING) AS _title,
        CAST(NULL AS STRING) AS _seniority,
        MAX(_phone) AS _phone,
        MIN(_company) AS _company, 
        MAX(_revenue) AS _revenue,
        MAX(_industry) AS _industry, 
        MAX(_city) AS _city,
        MAX(_state) AS _state,
        MAX(_country) AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        MAX(_sfdccontactid) AS _sfdcaccountid,
        CAST(NULL AS STRING) _sfdccontactid,
        MAX(_ebos), 
        MAX(_utilityprojects), 
        MAX(_tracker),
        MAX(_acc_linkedinurl), 
        MAX(_canopy), 
        MAX(_prospect), 
        MAX(_fixedtilt), 
        MAX(_account),  
        MAX(_acc_persona), 
        MAX(_rep), 
        MAX(_midwest),
        MAX(_account_segment)
        -- ROW_NUMBER() OVER(PARTITION BY _sfdcaccountid ORDER BY lastactivitydate DESC) AS _order
      FROM 
        contacts
      WHERE
        _country IS NOT NULL
        OR _industry IS NOT NULL
        OR _company IS NOT NULL
        OR _revenue IS NOT NULL
        OR _phone IS NOT NULL
        OR _city IS NOT NULL
        OR _state IS NOT NULL
      GROUP BY 
        _domain
      ORDER BY
        _domain
      /* RIGHT JOIN
        `terrasmart_salesforce.Account` sfdc ON contacts._sfdcaccountid = sfdc.id */
)
,
  #Query to pull the email engagement 
email_engagement AS (
      SELECT 
        *
      FROM ( 
        SELECT DISTINCT _email, 
        RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
        TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _campaignSentDate)) AS _timestamp,
        DATE_TRUNC(DATE(_campaignSentDate), MONTH) AS _month,
        EXTRACT(WEEK FROM _campaignSentDate) AS _week,  
        EXTRACT(YEAR FROM _campaignSentDate) AS _year,
        _utmcampaign AS _contentTitle, 
        CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
        _description
        FROM 
          (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
          /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
          AND */ LOWER(_engagement) NOT IN ('sent', 'downloaded', 'bounced', 'unsubscribed', 'processed', 'deffered', 'spam', 'suppressed', 'dropped')
      ) a
      WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart') 
        AND _domain IS NOT NULL 
      ORDER BY 
        1, 3 DESC, 2 DESC
),form_filled_engagement AS (
     SELECT * 
    FROM ( 
        SELECT 
            LOWER(_email) AS _email, 
            RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
            TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date , 
            DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year,
            _utmcampaign AS _contentTitle, 
            CONCAT("Email ", "Form Filled") AS _engagement,
            CAST(NULL AS STRING) AS _description
        FROM 
            (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ 
            _engagement IN ('Downloaded')
    ) a
    WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart|gmail|yahoo|outlook|hotmail') 
        AND _domain IS NOT NULL 
    ORDER BY 1, 3 DESC, 2 DESC
), paid_social_engagment AS (
    SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
        
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Paid Social ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow') 
        AND _medium = 'Paid Social'
), organic_social_engagment AS (
     SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Organic Social ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow') 
        AND _medium = 'Organic Social'
), webinar_engagement AS (
    SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Webinar ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow') 
        AND _medium = 'Webinar'
) ,
  dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements

    SELECT
      _date,
      DATE_TRUNC(DATE(_date), MONTH) AS _month,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 

  ),
  account_scores AS (

    SELECT
      *,
      EXTRACT(WEEK FROM _extract_date) AS _week,
      EXTRACT(MONTH FROM _extract_date) AS _month,
      EXTRACT(YEAR FROM _extract_date) AS _year,
      (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_paid_social , 0)+ COALESCE(_quarterly_organic_social , 0)+ COALESCE(_quarterly_webinar , 0)+ COALESCE(_quarterly_event_score, 0)+ COALESCE(_quarterly_form_filled_score, 0) )AS _account_90days_score
    FROM 
      `x-marketing.terrasmart.account_90days_score`
    ORDER BY
      _extract_date DESC

  ), contact_engagements AS (
  SELECT     DISTINCT /* dummy_dates.*, */
      engagements.* /* EXCEPT(_date, _week, _year) */,
      contacts.*EXCEPT(_domain, _email)
  FROM
  (
SELECT * FROM email_engagement
UNION ALL 
SELECT * FROM form_filled_engagement
UNION ALL 
SELECT * FROM paid_social_engagment
UNION ALL 
SELECT * FROM organic_social_engagment
UNION ALL 
SELECT * FROM webinar_engagement
)engagements
    LEFT JOIN
      contacts USING(_email)
),
  consolidated_engagement AS (

    SELECT * FROM contact_engagements

    
), all_data_join AS (
SELECT 
  DISTINCT consolidated_engagement.*, 
  COALESCE(account_scores. _account_90days_score,0)  AS _account_90days_score,
  --COALESCE(account_scores. _account_90days_score,0) ,
  /*COALESCE(latest_score._account_90days_score,0)*/
  COALESCE(latest_score._account_90days_score,0) AS _latest_account_score,
  /* COALESCE(_weekly_first_party_score, 0) AS _weekly_first_party_score, 
  COALESCE(_ytd_first_party_score, 0) AS _ytd_first_party_score, 
  engagement_grade._weekly_contact_score, 
  engagement_grade._ytd_contact_score,
  engagement_grade._ytd_grade */
FROM 
  consolidated_engagement
LEFT JOIN 
  account_scores 
    ON ( consolidated_engagement._domain = account_scores._domain AND DATE(consolidated_engagement._month ) = account_scores._extract_date ) 
LEFT JOIN 
  account_scores AS latest_score 
    ON (latest_score._domain =  consolidated_engagement._domain AND latest_score._month = EXTRACT(MONTH FROM CURRENT_DATE())  AND latest_score._year = EXTRACT(year FROM CURRENT_DATE()) ) 

WHERE
  LENGTH(consolidated_engagement._domain) > 1
  -- AND consolidated_engagement._year IN ( 2023,2024)
  -- AND consolidated_engagement._domain NOT LIKE '%pubpng.com%'
  -- AND  consolidated_engagement._domain NOT LIKE '%2x.marketing%'
  ---AND LOWER(consolidated_engagement._country) IN ('united states', 'us')
  ---AND consolidated_engagement._domain = '2u.com'
ORDER BY 
  _week DESC
) 
SELECT all_data_join.*,
FROM all_data_join;


