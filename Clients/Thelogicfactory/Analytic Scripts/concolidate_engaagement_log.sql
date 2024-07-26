TRUNCATE TABLE `thelogicfactory.db_consolidated_engagements_log` ;
INSERT INTO `thelogicfactory.db_consolidated_engagements_log`
WITH contacts AS (
  SELECT * EXCEPT (_rownum)
  FROM (
  SELECT *,
  ROW_NUMBER() OVER(
                PARTITION BY _email
                ORDER BY _id DESC
            ) _rownum
             FROM 
  (
    SELECT * EXCEPT(_rownum)
    FROM (
        SELECT _id,
            LOWER(_email) AS _email, 
            _name,
            _companywebsite AS _domain,
            _title, 
            _function,
            _seniority, 
            _phone, 
            _companyname AS _company, 
            _revenuerange AS _revenue, 
            _industry,
            CAST(_employees AS STRING) AS _employee, 
            _city, 
            _state, 
            _country, 
            _persona,
            CAST(NULL AS STRING) AS _lifecycleStage,
            CAST(NULL AS STRING) AS _sfdccontactid, 
            CAST(NULL AS STRING) AS _sfdcaccountid,
            CAST(NULL AS STRING) AS _sfdcleadid, 
            CAST(NULL AS STRING) AS _target_contacts, 
            "Target" AS _target_accounts,
            CAST(NULL AS STRING) AS _account_type ,
           
            _linkedinurl,
             SAFE_CASt(_tier AS INT64) AS _tier, 
            "Target" AS _source,
            'No' AS _suppressed,
            "Active" AS account_suppression, 
            ROW_NUMBER() OVER(
                PARTITION BY _email
                ORDER BY _updated DESC
            ) _rownum
        FROM 
            `x-marketing.thelogicfactory_mysql.w_routables`
    )
    WHERE _rownum = 1
    UNION ALL 
    SELECT * EXCEPT(_rownum)
    FROM (
      SELECT DISTINCT
      _id, 
      CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,
      _contactname,
      _accountdomain AS _domain,
      _contacttitle,
      CAST(NULL AS STRING) AS _function,
      CAST(NULL AS STRING) AS  _seniority, 
      CAST(NULL AS STRING) AS  _phone, 
      _accountname AS _company, 
      CAST(NULL AS STRING)  AS _revenue, 
      CAST(NULL AS STRING)  AS _industry,
      CAST(NULL AS STRING)   AS _employee, 
      CAST(NULL AS STRING)  AS _city, 
      CAST(NULL AS STRING)  AS  _state, 
      CAST(NULL AS STRING)  AS  _country, 
      CAST(NULL AS STRING)  AS  _persona,
      CAST(NULL AS STRING) AS _lifecycleStage,
      CAST(NULL AS STRING) AS _sfdccontactid, 
      CAST(NULL AS STRING) AS _sfdcaccountid,
      CAST(NULL AS STRING) AS _sfdcleadid, 
      CAST(NULL AS STRING) AS _target_contacts, 
      "Target" AS _target_accounts,
      CAST(NULL AS STRING) AS _account_type ,
      _contactlinkedin AS _linkedinurl,
      SAFE_CASt(0 AS INT64) AS _tier, 
      "Target" AS _source,
      'No' AS _suppressed,
      "Active" AS account_suppression,
      ROW_NUMBER() OVER(
                PARTITION BY  CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END
                ORDER BY _id DESC
            ) _rownum
            FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
            )
    WHERE _rownum = 1
  )
  ) WHERE _rownum = 1 
 
), accounts AS (
      SELECT * EXCEPT (rownum)
    FROM (
      SELECT
        DISTINCT 
        CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        _phone,
        _company,
        _revenue,
        _industry,
        _employee,
        _city,
        _state,
        _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 
        _target_contacts, 
        _target_accounts,
        _account_type,
        _linkedinurl,
        0 AS _tier,
        _source,
        '' AS _suppressed,
         "Active" AS account_suppression,
        
        ROW_NUMBER() OVER(
            PARTITION BY _domain
             ORDER BY _id DESC
          ) 
          AS rownum
      FROM
        contacts
    ) WHERE rownum = 1
),all_accounts AS ( #Pulling all account details
        SELECT * EXCEPT (rownum)
    FROM (
    SELECT *,
    ROW_NUMBER() OVER(
            PARTITION BY _domain
             ORDER BY _tier DESC
          ) 
          AS rownum 
    FROM (
      SELECT 
      DISTINCT 
      _domain, 
      _companyname AS _company, 
      CASE WHEN _domain = 'refresco.com' THEN 'Food & Beverage'
      WHEN _domain = 'diageo.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacolacompany.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacola.com' THEN 'Food & Beverage'
         WHEN _domain = 'ejgallo.com' THEN 'Food & Beverage' 
         WHEN _domain = 'cokecce.com' THEN 'Food & Beverage'
         WHEN _domain = 'pernodricard.com' THEN 'Food & Beverage'
         WHEN _domain = 'pernod-ricard.com' THEN 'Food & Beverage' ELSE _segment END AS _industry, 
      CAST(_tier AS INT64) AS _tier, 
      "Target" AS _source,
      _linkedinurl, 
       _suppressed
    FROM `thelogicfactory_mysql.db_account_list` 
    WHERE  _domain IS NOT NULL AND  _domain NOT IN ('.', '(blank)') AND _sdc_deleted_at IS NULL 
    UNION ALL 
       SELECT * 
    FROM (
    SELECT 
      DISTINCT 
      CASE WHEN _accountdomain LIKE '%ferrero%' THEN 'ferrero.com'
      WHEN _accountdomain LIKE '%www.%' THEN RIGHT(LOWER(_accountdomain),LENGTH(LOWER(_accountdomain))-STRPOS(LOWER(_accountdomain),'.')) 
      WHEN _accountdomain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com' 
      ELSE LOWER(_accountdomain) END AS _accountdomain,
      _accountname, 
      '' AS _industry,
      0 AS _tier,
      "Non Target",
      '' AS Linkedinurl,
      '' AS _suppressed 
    FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` ) 
    WHERE  
      (_accountdomain NOT IN (SELECT DISTINCT _domain FROM (SELECT _domain FROM `x-marketing.thelogicfactory_mysql.db_bombora_accounts`
      UNION ALL 
      SELECT _domain FROM `x-marketing.thelogicfactory_mysql.db_bombora_account_report` )) AND _accountdomain <> "")
        AND 
      (_accountdomain NOT IN (SELECT DISTINCT _domain FROM `x-marketing.thelogicfactory_mysql.db_account_list`
      WHERE  _sdc_deleted_at IS NOT NULL) AND _accountdomain <> "")
    )
     ) WHERE rownum = 1 
     

)
,target_bombora AS ( #Combining the list of accounts - Target & Bombora
  
  SELECT 
  DISTINCT 
    _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry,
    CASE WHEN mainAcc._tier IS NOT NULL THEN mainAcc._tier ELSE CAST(NULL AS INT64) END AS _tier,
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    mainAcc._linkedinurl AS _linkedinurl,
    mainAcc._suppressed  AS _suppressed,
    CASE WHEN _source = 'Target' THEN "Active" ELSE account_suppression END AS account_suppression , 
    FROM all_accounts mainAcc
      FULL JOIN ( SELECT * FROM (
        SELECT _companyrevenue, _hqzip, _countrycompositescoredelta, _metrocompositescoredelta, _countrycompositescore, _clustername, _address2, _intentcountry, _linkedinurl, _companysize, _companyname, _topicid, _metrocompositescore, CASE WHEN _domain = 'refresco.com' THEN 'Food & Beverage'
      WHEN _domain = 'diageo.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacolacompany.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacola.com' THEN 'Food & Beverage'
      WHEN _domain = 'ejgallo.com' THEN 'Food & Beverage' 
      WHEN _domain = 'cokecce.com' THEN 'Food & Beverage'
      WHEN _domain = 'pernodricard.com' THEN 'Food & Beverage'
      WHEN _domain = 'pernod-ricard.com' THEN 'Food & Beverage'
      WHEN _domain = 'coors.com' THEN 'Manufacturing > Food & Beverage'  ELSE _industry END AS _industry, _address1, _hqcountry, _hqcity, _metroarea, _state, _hqstate, _compositescore, _statecompositescoredelta, _topicname,CASE WHEN _domain = "sapagroup.com" THEN "hydro.com" ELSE _domain END AS _domain, _date, _domainorigin, _compositescoredelta, _statecompositescore,_segment,CASE WHEN _Segment IS NOT NULL THEN "Active" ELSE 'Suppressed' END AS account_suppression 
    FROM (
          SELECT *,
            CASE WHEN  _intentcountry IN ('United States','Canada','United Kingdom (Great Britain)','Ireland',
          'Netherlands','Belgium','Sweden','Denmark','Finland','Norway','Switzerland') AND _clustername = 'Food & Beverages' AND _industry = 'Manufacturing > Food & Beverage' AND _companyrevenue IN ('XLarge ($200MM-$1B)','XXLarge ($1B+)') AND _companysize IN('Medium (200 - 499 Employees)','Medium-Large (500 - 999 Employees)','Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Food & Beverage'
          WHEN  _intentcountry IN ('United States','Canada','United Kingdom (Great Britain)','France','Netherlands','Spain','Belgium','Sweden','Switzerland') AND _clustername = 'Retail Logistics' AND _industry IN ('Retail','Retail > Consumer Electronics','Retail > Department Stores & Super Stores','Retail > Drug Stores & Pharmacies','Retail > Furniture','Retail > Grocery','Retail > Home Improvement & Hardware','Retail > Motor Vehicles','Retail > Office Products') AND _companyrevenue IN ('XXLarge ($1B+)')AND _companysize IN('Medium (200 - 499 Employees)','Medium-Large (500 - 999 Employees)','Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Retail Logistics' 
          WHEN  _intentcountry IN ('Austria' , 'Belgium' , 'Brazil' , 'Canada' , 'Finland' , 'France' , 'Germany' ,'Italy' , 'Luxembourg' , 'Mexico' ,'Netherlands' , 'Norway' , 'Poland' , 'Romania' , 'Slovenia' , 'Spain' , 'Sweden' , 'Switzerland' , 'United Kingdom (Great Britain)' , 'United States') AND _clustername = 'Metals' AND (_industry LIKE  '%Metals%' ) AND _companyrevenue IN ('XXLarge ($1B+)','XLarge ($200MM-$1B)') AND _companysize IN('Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Metals' END AS _Segment 
          FROM `x-marketing.thelogicfactory_mysql.db_bombora_account_report` 
          WHERE _sdc_deleted_at IS NULL 
          )
          UNION ALL
            SELECT _companyrevenue, _hqzip, _countrycompositescoredelta, _metrocompositescoredelta, _countrycompositescore,'' AS _clustername, _address2, _intentcountry, _linkedinurl, _companysize, _companyname, _topicid, _metrocompositescore, _industry, _address1, _hqcountry, _hqcity, _metroarea, _state, _hqstate, _compositescore, _statecompositescoredelta, _topicname, _domain, _date, _domainorigin, _compositescoredelta, _statecompositescore, _segment,"Active" AS account_suppression 
          FROM (
          SELECT *, 'Food & Beverage'AS _Segment 
          FROM `x-marketing.thelogicfactory_mysql.db_bombora_accounts` 
          WHERE _sdc_deleted_at IS NULL 
    ))WHERE  account_suppression = 'Active') bomboraAcc USING(_domain)
  
   
  ORDER BY _domain
), final_accounts AS ( SELECT 
 CAST(NULL AS INT64) AS _id,
  CAST(NULL AS STRING) AS _email,
  CAST(NULL AS STRING) AS _name,
  _domain,
  CAST(NULL AS STRING) AS _jobtitle,
  CAST(NULL AS STRING) AS _function,
  CAST(NULL AS STRING) AS _seniority,
  CAST(NULL AS STRING) AS  _phone,
  _company,
  CAST(NULL AS STRING) AS _revenue,
  _industry,
  CAST(NULL AS STRING) AS _employee,
  CAST(NULL AS STRING) _city,
 CAST(NULL AS STRING) AS _state,
   CAST(NULL AS STRING) AS _country,
  CAST(NULL AS STRING) AS _persona,
  CAST(NULL AS STRING) AS _lifecycleStage,
  CAST(NULL AS STRING) AS _sfdccontactid,
  CAST(NULL AS STRING) AS _sfdcaccountid,
  CAST(NULL AS STRING) AS _sfdcleadid, 
  CAST(NULL AS STRING) AS _target_contacts, 
  "Non Target"  AS _target_accounts,
  CAST(NULL AS STRING) AS _account_type,
  CAST(NULL AS STRING) AS _linkedinurl,
  _tier,
   _source,
  mainAcc._suppressed  AS _suppressed,
   account_suppression , 
  FROM target_bombora mainAcc
),web_account AS (
  SELECT 
  DISTINCT 
  CAST(NULL AS INT64) AS _id,
  CAST(NULL AS STRING) AS _email,
  CAST(NULL AS STRING) AS _name,
  _domain,
  CAST(NULL AS STRING) AS _jobtitle,
  CAST(NULL AS STRING) AS _function,
  CAST(NULL AS STRING) AS _seniority,
  CAST(NULL AS STRING) AS  _phone,_name,
  CAST(NULL AS STRING) AS _revenue,
  CAST(NULL AS STRING) AS _industry,
  CAST(NULL AS STRING) AS _employee,
  _city AS  _city,
 CAST(NULL AS STRING) AS _state,
 _country AS _country,
  CAST(NULL AS STRING) AS _persona,
  CAST(NULL AS STRING) AS _lifecycleStage,
  CAST(NULL AS STRING) AS _sfdccontactid,
  CAST(NULL AS STRING) AS _sfdcaccountid,
  CAST(NULL AS STRING) AS _sfdcleadid, 
  CAST(NULL AS STRING) AS _target_contacts, 
  "Non Target"  AS _target_accounts,
  CAST(NULL AS STRING) AS _account_type,
  CAST(NULL AS STRING) AS _linkedinurl,
  CAST(NULL AS INT64) AS _tier,
   "Non Target" AS _source,
  CAST(NULL AS STRING)  AS _suppressed,
   CAST(NULL AS STRING) AS account_suppression

  FROM `x-marketing.thelogicfactory.db_web_engagements_log` web 
  WHERE _domain NOT IN (SELECT DISTINCT _domain FROM final_accounts)
  AND 
   _domain NOT IN (SELECT DISTINCT _domain FROM accounts)
), final_account AS (
  SELECT * FROM final_accounts
UNION ALL 
SELECT * ,
FROM accounts 
WHERE _domain NOT IN (SELECT DISTINCT _domain FROM final_accounts)
UNION ALL 
SELECT * ,
FROM web_account
),email_engagement AS (
   SELECT 
      * 
    FROM ( 
      SELECT _email, 
      _domain AS _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _subject AS _description,
      REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)') AS _utmsource,
      REGEXP_EXTRACT(_description, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
      REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
      REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent, 
      _description AS _fullurl,
       CAST(NULL AS INT64) AS _frequency,
      FROM 
        (SELECT DISTINCT * FROM `thelogicfactory.db_campaign_analysis`
        WHERE 
        LOWER(_engagement) NOT IN ('sent','delivered','hard bounced', 'soft bounced', 'unsubscribed'))
      WHERE 
        _timestamp >= '2022-01-01' AND _utm_campaign NOT LIKE '%Survey%' AND _isBot is NULL AND _utm_source LIKE '%email%'
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing') 
      AND LENGTH(_domain)  > 1 
    ORDER BY 
      1, 3 DESC, 2 DESC
),paid_social_engagement AS (
  SELECT 
  CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,
  _accountdomain AS _domain,
  CAST(_date AS TIMESTAMP) AS _timestamp,
  EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)  ) AS _week,  
  EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)  ) AS _year,
  _campaignname AS _contentTitle, 
  CONCAT(_medium, ' ',INITCAP(_engagementtype)) AS _engagement,
  '' AS _description,
  '' AS _utmsource,
  '' AS _utmcampaign,
  '' AS _utmmedium,
  '' AS _utmcontent, 
  _notes AS _fullurl,
  SAFE_CAST(_frequency AS INT64) AS _frequency,
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
  WHERE ( _medium = 'Paid Social' AND _engagementtype NOT IN ( '','Love', 'Cellebrate'))
), organic_social_engagement AS (
  SELECT 
  CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,
  _accountdomain AS _domain,
  CAST(_date AS TIMESTAMP) AS _timestamp,
  EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)  ) AS _week,  
  EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)  ) AS _year,
  _campaignname AS _contentTitle, 
  CONCAT(_medium, ' ',INITCAP(_engagementtype)) AS _engagement,
  '' AS _description,
  '' AS _utmsource,
  '' AS _utmcampaign,
  '' AS _utmmedium,
  '' AS _utmcontent, 
  _notes AS _fullurl,
  SAFE_CAST(_frequency AS INT64)  AS _frequency,
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
  WHERE  _medium = 'Organic Social' AND _engagementtype <> ''
), organic_engagement AS (
  SELECT 
  CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,
  _accountdomain AS _domain,
  CAST(_date AS TIMESTAMP) AS _timestamp,
  EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)  ) AS _week,  
  EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)  ) AS _year,
  _campaignname AS _contentTitle, 
  _medium AS _engagement,
  '' AS _description,
  '' AS _utmsource,
  '' AS _utmcampaign,
  '' AS _utmmedium,
  '' AS _utmcontent, 
  _notes AS _fullurl,
  SAFE_CAST(_frequency AS INT64)  AS _frequency,
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
  WHERE (_medium IN( 'Organic Search','Paid Search') AND _engagementtype <> '') 
  
), web_views AS (
  SELECT 
  DISTINCT 
  CAST(NULL AS STRING) AS _email, 
  _domain, 
  _timestamp, 
  EXTRACT(WEEK FROM _timestamp) AS _week,  
  EXTRACT(YEAR FROM _timestamp) AS _year, 
  _page AS _pageName, 
  "Web Visit" AS _engagement, 
  CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING)) AS _description,
  _utmsource,
  _utmcampaign,
  _utmmedium,
  _utmcontent,
  _fullurl,
  CAST(NULL AS INT64) AS _frequency,
  FROM `x-marketing.thelogicfactory.db_web_engagements_log` web 
) 
, ad_clicks AS (
  SELECT 
  DISTINCT 
  CAST(NULL AS STRING) AS _email, 
  _domain, 
  _timestamp, 
  EXTRACT(WEEK FROM _timestamp) AS _week,  
  EXTRACT(YEAR FROM _timestamp) AS _year, 
  _page AS _pageName, 
      "Paid Ads Clicks" AS _engagement, 
      CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING), " \n", "Clicked on: ", _utmcontent) AS _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
       CAST(NULL AS INT64) AS _frequency,
    FROM 
     `x-marketing.thelogicfactory.db_web_engagements_log` web 
    WHERE 
      LENGTH(_domain) > 1
      AND NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe')
      AND REGEXP_CONTAINS(_utmmedium, 'cpc|social')
      AND (NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|email') OR _utmsource IS NULL)
) 
, content_engagement AS (
    SELECT 
      DISTINCT CAST(NULL AS STRING) AS _email, 
      _domain, 
      _timestamp, 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year, 
      _page AS _pageName, 
      "Content Engagement" AS _engagement, 
      CONCAT("Time spent (secs): ",CAST(_engagementtime AS STRING)) AS _description,
      _utmsource,
      _utmcampaign,
      _utmmedium,
      _utmcontent,
      _fullurl,
      CAST(NULL AS INT64) AS _frequency,
    FROM 
     `x-marketing.thelogicfactory.db_web_engagements_log` web 
    WHERE 
      LENGTH(_domain) > 1
      AND NOT REGEXP_CONTAINS(_fullurl, 'Unsubscribe')
      AND REGEXP_CONTAINS(LOWER(_utmsource), 'case_study|blog')
      -- AND REGEXP_CONTAINS(_utmmedium, 'organic')
)
,form_fills AS (

    SELECT 
      _email,
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain,
      _datetime AS _timestamp,
       EXTRACT(WEEK FROM _datetime) AS _week,  
      EXTRACT(YEAR FROM _datetime) AS _year,
       _subject AS _form_title,
      'Form Filled' AS _engagement,
       _subject  AS _description,
      '' AS _utmsource,
      '' AS _utmcampaign,
      '' AS _utmmedium,
      '' AS _utmcontent,
      '' AS _fullurl,
      CAST(NULL AS INT64) AS _frequency,
    FROM 
       `x-marketing.thelogicfactory_mysql.web_form_submission`
)
,
 dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements

    SELECT
      _date,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 

)
,
bombora_report AS (

    SELECT
      CAST(NULL AS STRING) AS _email, 
      _domain,
      _date,
      EXTRACT(WEEK FROM _date) AS _week, 
      EXTRACT(YEAR FROM _date) AS _year,
      CAST(NULL AS STRING) AS _pageName,
      "Bombora Report" AS _engagement,
      STRING_AGG(CONCAT(_topicname, " - ", _compositescore), "\n") OVER(PARTITION BY _domain, EXTRACT(WEEK FROM _date)) AS _description,
      CAST(NULL AS STRING) AS _utmsource,
      CAST(NULL AS STRING) AS _utmcampaign,
      CAST(NULL AS STRING) AS _utmmedium,
      CAST(NULL AS STRING) AS _utmcontent,
      CAST(NULL AS STRING) AS _fullurl,
      CAST(NULL AS INT64) AS _frequency,
      ROUND(AVG(COALESCE(_compositescore, 0)) OVER(PARTITION BY _domain, _date), 0, "ROUND_HALF_AWAY_FROM_ZERO") AS _avg_bombora_score,
    FROM (
      SELECT 
          DISTINCT TIMESTAMP(_date) AS _date, 
          _domain, 
          _companyname, 
          _intentcountry,
          COALESCE(_topicname, NULL) AS _topicname, 
          COALESCE(CAST(_compositescore AS NUMERIC), 0) AS _compositescore,
          COALESCE(_compositescoredelta, NULL) AS _compositescoredelta
      FROM 
        `x-marketing.thelogicfactory_mysql.db_bombora_accounts` 
        UNION ALL
        SELECT 
          DISTINCT TIMESTAMP(_date) AS _date, 
          _domain, 
          _companyname, 
          _intentcountry,
          COALESCE(_topicname, NULL) AS _topicname, 
          COALESCE(CAST(_compositescore AS NUMERIC), 0) AS _compositescore,
          COALESCE(_compositescoredelta, NULL) AS _compositescoredelta
      FROM 
        `x-marketing.thelogicfactory_mysql.db_bombora_account_report` 
    )
 ),
  account_scores AS (

    SELECT
      *,
      EXTRACT(WEEK FROM _extract_date) AS _week,
      EXTRACT(YEAR FROM _extract_date) AS _year,
      (COALESCE(_quarterly_email_score, 0) + COALESCE( _quarterly_organic_social_score, 0)+ COALESCE(_quarterly_search_ads_score , 0)+ COALESCE(_quarterly_formfilled_score , 0)+ COALESCE(_quarterly_paidsocial_score , 0)+ COALESCE(_quarterly_web_score, 0) + COALESCE(_quarterly_formfilled_score, 0)) AS _account_90days_score
    FROM 
     `x-marketing.thelogicfactory.account_90days_score`
    ORDER BY
      _extract_date DESC

  ),/* 
  engagement_grade AS (

    SELECT 
      DISTINCT _week, 
      _year, 
      _email, 
      _weekly_contact_score,
      _ytd_contact_score,
      (
        CASE 
        WHEN _ytd_contact_score < 59 THEN 'C'
        WHEN _ytd_contact_score BETWEEN 60 AND 79 THEN 'B'
        WHEN _ytd_contact_score >= 80 THEN 'A'
        END
      ) AS _ytd_grade 
    FROM 
      `3x.contact_engagement_scoring` 
    ORDER BY 
      _week DESC

  ),  */
  account_engagements AS (

    SELECT 
      DISTINCT /* dummy_dates.*, */
      engagements.* /* EXCEPT(_date , _week, _year) */,
      accounts.*EXCEPT(_domain, _email)
    FROM
      /* dummy_dates
    JOIN */
      (
        -- SELECT * FROM bombora_report UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM web_views UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM ad_clicks UNION ALL
        SELECT *, CAST(NULL AS INTEGER) FROM content_engagement 
      ) engagements /* USING(_week, _year) */
    LEFT JOIN
     final_account AS  accounts USING(_domain)

  ),
  contact_engagements AS (

    SELECT 
      DISTINCT /* dummy_dates.*, */
      engagements.* /* EXCEPT(_date, _week, _year) */,
      contacts.*EXCEPT(_domain, _email)
    FROM
      /* dummy_dates
    JOIN */
      (
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM email_engagement 
        UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM form_fills
        UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM organic_engagement
        UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM organic_social_engagement
        UNION ALL
        SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM paid_social_engagement
        
      ) engagements /* USING(_week, _year) */
    LEFT JOIN
      contacts USING(_email)

)--,
  --consolidated_engagement AS (

    SELECT * FROM contact_engagements
     UNION ALL
    SELECT * FROM account_engagements 
    --limit 1



