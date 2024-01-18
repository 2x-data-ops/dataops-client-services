--CREATE OR REPLACE TABLE INSERT INTO `epam.db_consolidated_engagements_log`AS
TRUNCATE TABLE  `epam.db_consolidated_engagements_log`;
INSERT INTO `epam.db_consolidated_engagements_log`
WITH 
#Query to pull all the contacts in the leads table from Hubspot
contacts AS (
SELECT * EXCEPT (rownum) FROM (
      SELECT 
        CAST(marketo.id AS STRING) AS _leadid,
        marketo.email AS _email,
        CONCAT(marketo.firstname,' ', marketo.lastname) AS _name,
        RIGHT(marketo.email, LENGTH(marketo.email)-STRPOS(marketo.email, '@')) AS _domain,
        marketo.title AS _jobtitle,
        CASE   WHEN LOWER(marketo.title) LIKE LOWER("%Senior Partner Marketing Manager%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%Principal Marketing Manager, Strategic Partnerships%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%Head of Channel Success%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%Channel Partnerships Manager%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%Partner Marketing Program Manager%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%COO%") THEN "C-Level"
        WHEN LOWER(marketo.title) LIKE LOWER("%CEO%") THEN "C-Level"
        WHEN LOWER(marketo.title) LIKE LOWER("%Vice President%") THEN "VP"
        WHEN LOWER(marketo.title) LIKE LOWER("%VP%") THEN "VP"
        WHEN LOWER(marketo.title) LIKE LOWER("%Sr marketing manager%") THEN "Manager"
        WHEN LOWER(marketo.title) LIKE LOWER("%Senior%") THEN "Other"
        WHEN LOWER(marketo.title) LIKE LOWER("%Staff%") THEN "Other"
       WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Senior Partner Marketing Manager%") THEN "Manager"
       WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Principal Marketing Manager%") THEN "Manager"
       WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Strategic Partnerships%") THEN "Manager"
       WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Partner Marketing Program Manager%") THEN "Manager"
       WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Channel Partnerships Manager%") THEN "Manager"
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Senior Counsel%") THEN "VP"  
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%General Counsel%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Founder%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%C-Level%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CDO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CIO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CMO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CFO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CEO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Chief%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%COO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Sr. V.P.%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Sr.VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Senior-Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%srvp%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Senior VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%SR VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Sr Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Sr. VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Sr. Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%S.V.P%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Senior Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Exec Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Exec Vp%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Executive VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Exec VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Executive Vice President%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%EVP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%E.V.P%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%SVP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%V.P%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Vice President%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Vice Pres%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%V P%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%VP%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%President%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Director%") THEN "Director" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CTO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Dir%") THEN "Director" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Dir.%") THEN "Director" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%MDR%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%MD%") THEN "Director" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%GM%") THEN "Director" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Head%") THEN "VP" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Manager%") THEN "Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%escrow%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%cross%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%crosse%") THEN "Non-Manager" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Partner%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%CRO%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Chairman%") THEN "C-Level" 
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Owner%") THEN "C-Level"
         WHEN marketo.title IS NULL AND LOWER(marketo.title) LIKE LOWER("%Team Lead%") THEN "Manager"
        ELSE marketo.title END AS _seniority,
        "" AS _function,
        marketo.phone,
        marketo.company,
        "" AS _revenue,
        marketo.industry,
        marketo.city,
        marketo.state,
        CASE WHEN marketo.country = 'Unknown value' THEN 'Other'
        WHEN marketo.country LIKE '%US%' OR marketo.country LIKE '%USA%' THEN 'United States' 
        WHEN marketo.country LIKE '%UK%' THEN 'United Kingdom'
        WHEN marketo.country LIKE '%South Korea%' THEN 'Korea, Republic of' ELSE marketo.country END AS country,
        c.region,
        '' AS _persona,
        '' AS _lifecycleStage,
        '' AS form_submissions,
        '' AS _sfdcaccountid,
        createdat,
        CASE WHEN LOWER(title) LIKE '%journalist%' OR LOWER(title) LIKE '%reporter%' OR LOWER(title) LIKE 'student' OR LOWER(title) LIKE 'students' OR LOWER(title) LIKE 'studentin' OR LOWER(title) LIKE 'grad student' OR LOWER(title) LIKE 'master student' OR LOWER(title) LIKE 'intern' OR LOWER(title) LIKE 'mba intern' OR LOWER(title) LIKE 'machine learning intern' OR LOWER (title) LIKE '%publication%' OR LOWER(title) LIKE 'freelance' THEN 'Disqualified Leads' ELSE 'Qualified Leads'  END AS _leadqualification,
        leadsource,
        industrypreference,
        l._personsource,
        ROW_NUMBER() OVER( PARTITION BY CAST(marketo.id AS STRING) ORDER BY createdat DESC) AS rownum
    FROM `x-marketing.epam_marketo.leads` marketo
    LEFT JOIN `x-marketing.epam.db_country` c ON marketo.country = c.country
    LEFT JOIN `x-marketing.epam_mysql.epam_db_db_personsource_lead` l ON l._prospectid = CAST(marketo.id AS STRING)
    WHERE 
        marketo.email  NOT LIKE '%@2x.marketing%' AND marketo.email NOT LIKE '%2X%' AND marketo.email NOT LIKE '%@epam.com' AND marketo.email NOT LIKE 'skylarulry@yahoo.com' AND marketo.email NOT LIKE '%test%' AND marketo.email <> 'sonam.gupta@capgemini.com' AND LOWER(company) <> 'Endava')
        WHERE rownum = 1
) ,
accounts AS (
   SELECT *EXCEPT(_order) FROM 
  (
    SELECT 
      DISTINCT *,
    
      ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY createdat DESC) AS _order
    FROM 
      contacts

  )
  WHERE
    _order = 1
)
,
#Query to pull the email engagement 
email_engagement AS (
    SELECT 
      *
    FROM ( 
      SELECT DISTINCT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date,
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _description AS _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `x-marketing.epam.db_campaign_analysis_marketo`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'downloaded', 'bounced', 'unsubscribed', 'processed', 'deffered', 'spam', 'suppressed', 'dropped')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|epam') 
      AND _domain IS NOT NULL 
    ORDER BY 
      1, 3 DESC, 2 DESC
),web_views AS (
  WITH all_web_visits_in_timeframe AS (

    SELECT
        pages.activitydate AS activity_date,
        leads.mktoname AS lead_name,
        leads.title AS job_title,
        leads.company,
        pages.webpage_url,
        pages.referrer_url,
        leads.email,
        client_ip_address,
        utmsource 
    FROM `x-marketing.epam_marketo.activities_visit_webpage` pages
    LEFT JOIN `x-marketing.epam_marketo.leads` AS leads
    ON pages.leadid = leads.id 
    WHERE LOWER(pages.primary_attribute_value) NOT LIKE '%unsubscribe%'
    AND DATE_DIFF(CURRENT_DATE('America/Los_Angeles'), DATE(pages.activitydate, 'America/Los_Angeles'), DAY) <= 7
    -- AND leads.mktoname = 'James Wills'

),
-- Find the first page visited by a lead
find_entry_page AS (

    SELECT * EXCEPT(rownum)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY lead_name
                ORDER BY activity_date
            ) rownum
        FROM all_web_visits_in_timeframe
    )
    WHERE rownum = 1

),
-- Count the total page visited by a lead
get_total_page_views AS (

    SELECT
        lead_name, 
        COUNT(*) AS page_views
    FROM all_web_visits_in_timeframe
    GROUP BY 1

),
-- Combine aggregated information
combined_data AS (

    SELECT
        main.* EXCEPT(webpage_url),
        main.webpage_url AS entry_page,
        side.page_views
    FROM find_entry_page AS main
    LEFT JOIN get_total_page_views AS side
    ON main.lead_name = side.lead_name

)
SELECT email AS _email,
RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain,
activity_date AS _date,
EXTRACT(WEEK FROM activity_date) AS _week,  
EXTRACT(YEAR FROM activity_date) AS _year, 
entry_page AS _pageName ,
' ' AS _description,
referrer_url AS _first_fullurl,
page_views
 FROM combined_data
),
web_engagements AS (
  SELECT
    DISTINCT CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date AS _timestamp, 
    _week,  
    _year, 
    _pageName AS _webActivity, 
    "Web Visit" AS _engagement, 
    _description,
  FROM
    web_views
  WHERE 
    NOT REGEXP_CONTAINS(LOWER(_first_fullurl), 'unsubscribe')
    AND NOT REGEXP_CONTAINS(LOWER(_first_fullurl), 'linkedin|adwords')
),
ad_clicks AS (
  SELECT 
    DISTINCT CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date AS _timestamp, 
    _week,  
    _year, 
    _pageName AS _webActivity, 
    "Ad Clicks" AS _engagement, 
    _description,
  FROM 
    web_views
  WHERE 
    NOT REGEXP_CONTAINS(LOWER(_first_fullurl), 'unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_first_fullurl), 'linkedin|adwords')
)
,content_engagement AS (
  SELECT 
    DISTINCT CAST(NULL AS STRING) AS _email, 
    _domain, 
    CAST(_date  AS TIMESTAMP) AS _timestamp, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _webActivity, 
    "Content Engagement" AS _engagement, 
    CONCAT("Total Page Views: ", _totalsessionviews) AS _description,
  FROM 
    epam.web_metrics
  WHERE 
    REGEXP_CONTAINS(LOWER(_page), '/blog/')
  
) ,form_fills AS (
 SELECT 
      DISTINCT email AS _email, 
      RIGHT(email, LENGTH(email)-STRPOS(email, '@')) AS _domain,
      _timestamp AS _timestamp , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _formTitle, 
      INITCAP(_engagement) AS _engagement,
      referrer_url
    FROM ( 
        SELECT 
            leadid AS _leadid, email,
            primary_attribute_value_id AS _campaignID, 
            activitydate AS _timestamp, 
            primary_attribute_value AS _formTitle, 
            'form filled' AS _engagement,
            referrer_url,
            ROW_NUMBER() OVER(
                PARTITION BY leadid, primary_attribute_value_id 
                ORDER BY activitydate DESC
            ) AS rownum
        FROM `x-marketing.epam_marketo.activities_fill_out_form` fill
        LEFT JOIN `epam_marketo.leads` main ON fill.leadid = main.id
        WHERE NOT REGEXP_CONTAINS(LOWER(primary_attribute_value),'unsubscribe|become a partner|test') 
    ) A 
    WHERE 
      rownum = 1
) ,
 dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
),
/*intent_score AS (
  SELECT DISTINCT *,  
    FROM (
        SELECT 
          CAST(NULL AS STRING) AS _email, 
          _domain,
          EXTRACT(DATETIME FROM report._date) AS _date,
          EXTRACT(WEEK FROM _date)-1 AS _week, 
          EXTRACT(YEAR FROM _date) AS _year,
          "Bombora",
          "Bombora" AS _engagements,
          STRING_AGG(CONCAT(_topicname), ", ") OVER(PARTITION BY _domain)AS _topics,
          MAX(CAST(_averagecompositescore AS INT64)) OVER(PARTITION BY _domain) AS _weekly_avgCompositeScore,
        FROM 
          `epam.bombora_surge_report` report
    )
),  */
first_party_score AS (
  SELECT 
    DISTINCT _domain, _extract_date,
    EXTRACT(WEEK FROM _extract_date) -1 AS _week,  -- Minus 1 as the score is referring to the week before.
    EXTRACT(YEAR FROM _extract_date) AS _year,
    (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_ads_score , 0)+ COALESCE(_quarterly_web_score, 0)) AS _t90_days_score
  FROM
   `epam.account_90days_score`
  ORDER BY
    _extract_date DESC
/* ), 
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
    `epam.contact_engagement_scoring` 
  ORDER BY 
    _week DESC */
)
,
# Combining the engagements - Contact based and account based engagements
engagements AS (
# Contact based engagement query
  SELECT 
    DISTINCT contacts._domain, 
    contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS INTEGER) AS _avg_bombora_score,
    contacts.*EXCEPT(_domain, _email, form_submissions, _function),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  RIGHT JOIN
    contacts USING(_email) 
  UNION DISTINCT
# Account based engagement query
  SELECT 
    DISTINCT accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_timestamp, _week, _year, _domain, _email),
    accounts.*EXCEPT(_domain, _email, form_submissions, _function),
    engagements._timestamp
  FROM 
    dummy_dates
  CROSS JOIN
    accounts
  JOIN (
    -- SELECT * FROM intent_score UNION ALL
    SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM web_engagements UNION ALL
    SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM ad_clicks UNION ALL
    SELECT *, CAST(NULL AS INTEGER) AS _avg_bombora_score FROM content_engagement
  ) engagements USING(_domain, _week, _year)
 )SELECT 
  DISTINCT engagements.*, 
  COALESCE(_t90_days_score, 0) AS _t90_days_score, 
  -- COALESCE(_ytd_first_party_score, 0) AS _ytd_first_party_score, 
  -- engagement_grade._weekly_contact_score, 
  -- engagement_grade._ytd_contact_score,
  -- engagement_grade._ytd_grade
FROM 
  engagements
LEFT JOIN 
  first_party_score USING(_domain, _week, _year)
WHERE
  LENGTH(_domain) > 1