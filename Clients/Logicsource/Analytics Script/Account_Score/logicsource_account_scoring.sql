/*TEMP TABLE section: for code reuseability*/
CREATE TEMP TABLE temp_contacts_source AS
SELECT
  associated_company.properties.domain.value AS _domain,
  associated_company.properties.name.value AS _company,
  CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
  CASE
    WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
    WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
    WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
    WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
    WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
    WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet'
    ELSE associated_company.properties.industry.value
  END AS _industry,
  associated_company.properties.segment__c.value AS _company_segment,
  associated_company.properties.employee_range.value AS _employee_range,
  associated_company.properties.employee_range_c.value AS _employee_range_c,
  CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees,
  CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue,
  associated_company.properties.annual_revenue_range.value AS _annual_revenue_range,
  associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
FROM `x-marketing.logicsource_hubspot.contacts` k
LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l
  ON LOWER(l.email) = LOWER(property_email.value)
QUALIFY ROW_NUMBER() OVER (PARTITION BY associated_company.properties.domain.value, associated_company.company_id ORDER BY properties.createdate.value DESC) = 1;

CREATE TEMP TABLE temp_zoominfo AS
SELECT DISTINCT
  _domain,
  _company,
  CAST(NULL AS STRING) AS _revenue,
  _companyindustry AS _industry,
  CAST(NULL AS STRING) AS _company_segment,
  CAST(NULL AS STRING) AS _employee_range,
  CAST(NULL AS STRING) AS _employee_range_c,
  CAST(NULL AS NUMERIC) AS _numberofemployees,
  CAST(NULL AS NUMERIC) AS _annualrevenue,
  CAST(NULL AS STRING) AS _annual_revenue_range,
  CAST(NULL AS STRING) AS _annual_revenue_range_c
FROM `x-marketing.logicsource_mysql.db_zoominfo_intent`;

CREATE TEMP TABLE temp_account AS
SELECT
  *
FROM temp_contacts_source
UNION ALL
SELECT DISTINCT
  _domain AS _domain,
  CAST(NULL AS STRING) AS _company,
  -- CAST(NULL AS STRING) AS _lastname,
  CAST(NULL AS STRING) AS _revenue,
  CAST(NULL AS STRING) AS _industry,
  CAST(NULL AS STRING) AS _company_segment,
  CAST(NULL AS STRING) AS _employee_range,
  CAST(NULL AS STRING) AS _employee_range_c,
  CAST(NULL AS NUMERIC) AS _numberofemployees,
  CAST(NULL AS NUMERIC) AS _annualrevenue,
  CAST(NULL AS STRING) AS _annual_revenue_range,
  CAST(NULL AS STRING) AS _annual_revenue_range_c,
FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire`
WHERE _domain IS NOT NULL
  AND _domain != ''
UNION ALL
SELECT DISTINCT
  CASE
    WHEN _accountdomain = 'optum.com/' THEN 'optum.com'
    ELSE _accountdomain
  END AS _domain,
  CAST(NULL AS STRING) AS _company,
  -- CAST(NULL AS STRING) AS _lastname,
  CAST(NULL AS STRING) AS _revenue,
  _industry AS _industry,
  CAST(NULL AS STRING) AS _company_segment,
  CAST(NULL AS STRING) AS _employee_range,
  CAST(NULL AS STRING) AS _employee_range_c,
  CAST(NULL AS NUMERIC) AS _numberofemployees,
  CAST(NULL AS NUMERIC) AS _annualrevenue,
  CAST(NULL AS STRING) AS _annual_revenue_range,
  CAST(NULL AS STRING) AS _annual_revenue_range_c,
FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement`
WHERE _accountdomain IS NOT NULL
  AND _accountdomain != '';

CREATE TEMP TABLE temp_email_last_engagementdate AS
WITH email_opened_clicked AS (
  SELECT
    _domain,
    SUM(IF(_engagement = 'Email Opened', 1, 0)) AS _emailOpened,
    SUM(IF(_engagement = 'Email Clicked', 1, 0)) AS _emailClicked,
    -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
    -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Email Opened', 'Email Clicked') --WHERE _domain = 'fedex.com'
  GROUP BY 1
),
email_opened_clicked_total AS (
  SELECT
    _domain,
    SUM(_emailOpened) AS _emailOpentotal,
    SUM(_emailClicked) AS _emailClickedtotal,
  FROM email_opened_clicked --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
),
email_opened_clicked_score AS (
  SELECT
    _domain,
    _emailOpentotal,
    _emailClickedtotal,
    SUM(DISTINCT(IF(_emailOpentotal >= 10, 1 * 10, 0))) AS _emailopenscore_more,
    SUM(DISTINCT (IF(_emailClickedtotal >= 5, 1 * 10, 0))) AS _emailopenscore,
    SUM(DISTINCT (IF(_emailClickedtotal >= 2, 1 * 10, 0)) ) AS _emailclickscore_more,
    (
      (IF(_emailOpentotal >= 10, 1 * 10, 0)) + 
      (IF(_emailClickedtotal >= 5, 1 * 10, 0)) + 
      (IF(_emailClickedtotal >= 2, 1 * 10, 0))
    ) AS _email_score
  FROM email_opened_clicked_total
  GROUP BY 1, 2, 3
),
email_engagements AS (
  SELECT
    * EXCEPT (_email_score),
    CASE
      WHEN _email_score >= 20 THEN 20
      ELSE _email_score
    END AS _email_score
  FROM email_opened_clicked_score
),
email_last_engagements AS (
  SELECT
    _domain,
    _email,
    _id,
    MAX(_date) OVER (PARTITION BY _domain) AS _last_engagement_TS
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Email Opened', 'Email Clicked')
  QUALIFY ROW_NUMBER() OVER ( PARTITION BY _domain ORDER BY _date DESC) = 1
),
email_last_engagementdate AS (
  SELECT
    email_engagements.*,
    CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_email,
    EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
    EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
  FROM email_last_engagements AS _last_engagement
  RIGHT JOIN email_engagements
    ON email_engagements._domain = _last_engagement._domain
)
SELECT
  *
FROM email_last_engagementdate;

CREATE TEMP TABLE temp_paid_social_last_engagement_dates AS
SELECT
  _domain,
  _email,
  CASE
    WHEN _email IS NULL THEN _domain
    WHEN _domain IS NULL THEN _email
    ELSE CONCAT(_email, " ", _domain)
  END AS _id,
  MAX(_date) OVER (
    PARTITION BY _domain, _email,
    CASE
      WHEN _email IS NULL THEN _domain
      WHEN _domain IS NULL THEN _email
      ELSE CONCAT(_email, " ", _domain)
    END
  ) AS _last_engagement_TS,
FROM `x-marketing.logicsource.db_consolidated_engagements_log`
WHERE _engagement IN ("Paid Ads")
QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain, _email, _id ORDER BY _date DESC) = 1;

CREATE TEMP TABLE temp_paid_social_last_engagement AS
WITH paid_social AS (
  SELECT
    _domain,
    SUM(
      CASE
        WHEN _engagement = 'Paid Ads'
        AND _contentTitle = 'Share' THEN 1
        ELSE 0
      END
    ) AS _paidadsshare,
    SUM(
      CASE
        WHEN _engagement = 'Paid Ads'
        AND _contentTitle = 'Comment' THEN 1
        ELSE 0
      END
    ) AS _paidadscomment,
    SUM(
      CASE
        WHEN _engagement = 'Paid Ads'
        AND _contentTitle = 'Follow' THEN 1
        ELSE 0
      END
    ) AS _paidadsfollow,
    SUM(
      CASE
        WHEN _engagement = 'Paid Ads'
        AND _contentTitle = 'Visit' THEN 1
        ELSE 0
      END
    ) AS _paidadsvisit,
    SUM(
      CASE
        WHEN _engagement = 'Paid Ads'
        AND SUBSTR(_description, 1, 7) LIKE '%Clicks%'
        OR SUBSTR(_description, 1, 4) LIKE '%Like%' THEN 1
        ELSE 0
      END
    ) AS _paidadsclick_like,
    -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
    -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Paid Ads')
  GROUP BY 1
),
paid_social_total AS (
  SELECT
    _domain,
    SUM(_paidadsshare) AS _paidadssharetotal,
    SUM(_paidadscomment) AS _paidadscommenttotal,
    SUM(_paidadsfollow) AS _paidadsfollowtotal,
    SUM(_paidadsvisit) AS _paidadsvisittotal,
    SUM(_paidadsclick_like) AS _paidadsclick_liketotal,
  FROM paid_social --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
),
paid_social_score AS (
  SELECT
    _domain,
    _paidadssharetotal,
    _paidadscommenttotal,
    _paidadsfollowtotal,
    _paidadsvisittotal,
    _paidadsclick_liketotal,
    SUM(DISTINCT (IF(_paidadssharetotal >= 1, 1 * 15, 0))) AS _paidadssharescore,
    SUM(DISTINCT (IF(_paidadscommenttotal >= 1, 1 * 10, 0))) AS _paidadscommentscore,
    SUM(DISTINCT (IF(_paidadsfollowtotal >= 1, 1 * 4, 0))) AS _paidadsfollowscore,
    SUM(DISTINCT (IF(_paidadsvisittotal >= 1, 1 * 5, 0))) AS _paidadsvisitscore,
    SUM(DISTINCT (IF(_paidadsclick_liketotal >= 1, 1 * 5, 0))) AS _paidadsclick_likescore,
  FROM paid_social_total
  GROUP BY 1, 2, 3, 4, 5, 6
),
paid_social_engagements AS (
  SELECT
    *,
    CASE
      WHEN _paidadssharescore + _paidadscommentscore + _paidadsfollowscore + _paidadsvisitscore + _paidadsclick_likescore >= 35 THEN 35
      ELSE _paidadssharescore + _paidadscommentscore + _paidadsfollowscore + _paidadsvisitscore + _paidadsclick_likescore
    END AS _paid_ads_score_total --EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
  FROM paid_social_score
),
paid_social_last_engagement AS (
  SELECT
    paid_social_engagements.*,
    _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
    EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _paid_social_week,
    EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _paid_social_year
  FROM temp_paid_social_last_engagement_dates AS _last_engagement
  RIGHT JOIN paid_social_engagements
    ON paid_social_engagements._domain = _last_engagement._domain
)
SELECT
  *
FROM paid_social_last_engagement;

CREATE TEMP TABLE temp_organic_social_last_engagement AS
WITH organic_social AS (
  SELECT
    _domain,
    SUM(
      CASE
        WHEN _engagement = 'Organic Social'
        AND _contentTitle = 'Share' THEN 1
        ELSE 0
      END
    ) AS _organicsshare,
    SUM(
      CASE
        WHEN _engagement = 'Organic Social'
        AND _contentTitle = 'Comment' THEN 1
        ELSE 0
      END
    ) AS _organiccomment,
    SUM(
      CASE
        WHEN _engagement = 'Organic Social'
        AND _contentTitle = 'Follow' THEN 1
        ELSE 0
      END
    ) AS _organicfollow,
    SUM(
      CASE
        WHEN _engagement = 'Organic Social'
        AND _contentTitle = 'Visit' THEN 1
        ELSE 0
      END
    ) AS _organicvisit,
    SUM(
      CASE
        WHEN _engagement = 'Organic Social'
        AND SUBSTR(_description, 1, 7) LIKE '%Clicks%'
        OR SUBSTR(_description, 1, 4) LIKE '%Like%' THEN 1
        ELSE 0
      END
    ) AS _organicclick_like,
    -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
    -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Organic Social') --WHERE _domain = 'fedex.com'
  GROUP BY 1
),
organic_social_total AS (
  SELECT
    _domain,
    SUM(_organicsshare) AS _organicsharetotal,
    SUM(_organiccomment) AS _organiccommenttotal,
    SUM(_organicfollow) AS _organicfollowtotal,
    SUM(_organicvisit) AS _organicvisittotal,
    SUM(_organicclick_like) AS _organicclick_liketotal,
  FROM organic_social --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
),
organic_social_score AS (
  SELECT
    _domain,
    _organicsharetotal,
    _organiccommenttotal,
    _organicfollowtotal,
    _organicvisittotal,
    _organicclick_liketotal,
    SUM(DISTINCT (IF(_organicsharetotal >= 1, 1 * 15, 0))) AS _organischarescore,
    SUM(DISTINCT (IF(_organiccommenttotal >= 1, 1 * 10, 0))) AS _organiccommentscore,
    SUM(DISTINCT (IF(_organicfollowtotal >= 1, 1 * 4, 0))) AS _organicfollowscore,
    SUM(DISTINCT (IF(_organicvisittotal >= 1, 1 * 5, 0))) AS _organicvisitscore,
    SUM(DISTINCT (IF(_organicclick_liketotal >= 1, 1 * 5, 0))) AS _organicclick_likescore,
  FROM organic_social_total
  GROUP BY 1, 2, 3, 4, 5, 6
),
organic_social_engagements AS (
  SELECT
    *,
    CASE
      WHEN _organischarescore + _organiccommentscore + _organicfollowscore + _organicvisitscore + _organicclick_likescore >= 35 THEN 35
      ELSE _organischarescore + _organiccommentscore + _organicfollowscore + _organicvisitscore + _organicclick_likescore
    END AS _organic_ads_score_total --EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
  FROM organic_social_score
),
organic_social_last_engagement AS (
  SELECT
    organic_social_engagements.*,
    _last_engagement._last_engagement_TS AS _last_engagement_organc_social,
    EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organc_social_week,
    EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organc_social_year
  FROM temp_paid_social_last_engagement_dates AS _last_engagement
  RIGHT JOIN organic_social_engagements
    ON organic_social_engagements._domain = _last_engagement._domain
)
SELECT
  *
FROM organic_social_last_engagement;

-------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `x-marketing.logicsource.account_engagement_scoring` AS 
WITH account AS (
  SELECT
    temp_account.*,
    "Target" AS _source
  FROM temp_account
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _company DESC) = 1
),
zoominfo AS (
  SELECT DISTINCT
    * EXCEPT (_industry),
    CAST(NULL AS STRING) AS _industry
  FROM temp_zoominfo
),
contacts AS (
  SELECT
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo._domain
      ELSE mainAcc._domain
    END AS _domain,
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo._company
      ELSE mainAcc._company
    END AS _company,
    mainAcc.* EXCEPT (_source, _domain, _company),
    CASE
      WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source
      ELSE "Bombora"
    END AS _source,
  FROM account AS mainAcc
  LEFT JOIN zoominfo
    USING (_domain)
),
email_last_engagementdate AS (
  SELECT
    *
  FROM temp_email_last_engagementdate
),
form_filled AS (
  SELECT
    _domain,
    SUM(
      CASE
        WHEN (
          _engagement = 'Form Filled'
          AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo')
        ) THEN 1
        ELSE 0
      END
    ) AS _formFilled_contact_form,
    SUM(
      CASE
        WHEN (
          _engagement = 'Form Filled'
          AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar')
        )
        OR (
          _engagement = 'Form Filled'
          AND _contentTitle = "Other Content Engagement"
        )
        OR (
          _engagement = 'Form Filled'
          AND _description = "Visited booth"
        ) THEN 1
        ELSE 0
      END
    ) AS _distinctGatedContent,
    SUM(
      CASE
        WHEN _engagement = 'Form Filled'
        AND _description = "Registered" THEN 1
        ELSE 0
      END
    ) AS _distinctWebinarForm,
    SUM(
      CASE
        WHEN _engagement = 'Form Filled'
        AND _description = "Attended event" THEN 1
        ELSE 0
      END
    ) AS _distinctWebinarattended,
    -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
    -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Form Filled') --WHERE _domain = 'fedex.com'
  GROUP BY 1
),
form_filled_total AS (
  SELECT
    _domain,
    SUM(_formFilled_contact_form) AS _formFilled_total,
    SUM(_distinctGatedContent) AS _distinctGatedContenttotal,
    SUM(_distinctWebinarForm) AS _distinctWebinarFormtotal,
    SUM(_distinctWebinarattended) AS _distinctWebinarattendedtotal,
  FROM form_filled --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
),
form_filled_score AS (
  SELECT
    _domain,
    _formFilled_total,
    _distinctGatedContenttotal,
    _distinctWebinarFormtotal,
    _distinctWebinarattendedtotal,
    SUM(DISTINCT (IF(_formFilled_total >= 1, 1 * 50, 0))) AS _formFilled_score,
    SUM(DISTINCT (IF(_distinctGatedContenttotal >= 1, 1 * 20, 0))) AS _GatedContentscore,
    SUM(DISTINCT (IF(_distinctWebinarFormtotal >= 1, 1 * 5, 0))) AS _distinctWebinarFormscore,
    SUM(DISTINCT (IF(_distinctWebinarattendedtotal >= 1, 1 * 15, 0))) AS _distinctWebinarattendedscore,
  FROM form_filled_total
  GROUP BY 1, 2, 3, 4, 5
),
formfilled_engagements AS (
  SELECT
    *,
    CASE
      WHEN _GatedContentscore + _distinctWebinarFormscore + _distinctWebinarattendedscore >= 30 THEN 30
      ELSE _GatedContentscore + _distinctWebinarFormscore + _distinctWebinarattendedscore
    END AS _GatedContentscore_total,
    CASE
      WHEN _formFilled_score >= 50 THEN 50
      ELSE _formFilled_score
    END AS _formFilled_webinarscore_total,
    CASE
      WHEN _GatedContentscore + _formFilled_score + _distinctWebinarFormscore + _distinctWebinarattendedscore >= 80 THEN 80
      ELSE _GatedContentscore + _formFilled_score + _distinctWebinarFormscore + _distinctWebinarattendedscore
    END AS _form_fill_score_total --EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
  FROM form_filled_score
),
form_filled_last_engagements AS (
  SELECT
    _domain,
    MAX(_date) OVER (PARTITION BY _domain) AS _last_engagement_TS
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Form Filled')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _date DESC) = 1
),
formfill_last_engagementdate AS (
  SELECT
    formfilled_engagements.*,
    CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
    EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
    EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM form_filled_last_engagements AS _last_engagement
  RIGHT JOIN formfilled_engagements
    ON formfilled_engagements._domain = _last_engagement._domain
),
paid_social_last_engagement AS (
  SELECT
    *
  FROM temp_paid_social_last_engagement
),
organic_social_last_engagement AS (
  SELECT
    *
  FROM temp_organic_social_last_engagement
),
mouseflow_kickfire_timestamps AS (
  SELECT
    _domain,
    _visitorid,
    DATETIME(_timestamp) AS _timestamp,
    EXTRACT(WEEK FROM _timestamp) AS _week,
    EXTRACT(YEAR FROM _timestamp) AS _year,
    _entrypage AS _pageName,
    -- "Web Visit" AS _engagement, 
    CAST(_engagementtime AS INT64) AS _website_time_spent,
    _totalPages AS _website_page_view
  FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire` web --WHERE 
    --NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
    --AND _webactivity IS NOT NULL
),
weekly_web_data AS (
  SELECT
    _domain,
    -- _week,
    -- _year,
    -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
    COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
    COALESCE(SUM(CASE WHEN _pageName IS NOT NULL THEN 1 END ), 0) AS _website_page_view,
    COALESCE(COUNT(DISTINCT _visitorid), 0) AS _website_visitor_count,
    COALESCE(COUNT(DISTINCT CASE WHEN _pageName LIKE "%careers%" THEN _visitorid  END ), 0) AS _career_page,
    TRUE AS _visited_website,
    -- MAX(_timestamp) AS last_engaged_date
  FROM mouseflow_kickfire_timestamps
  WHERE LENGTH(_domain) > 2
  GROUP BY 1
),
weekly_web_data_score AS (
  SELECT
    *,
    COALESCE((_website_time_spent), 0) AS website_time_spent_score,
    ( 
      IF(_website_page_view >= 5, 15, 0) + 
      IF(_website_page_view <= 5, 10, 0)
    ) AS website_page_view_score,
    (
      IF(_website_visitor_count >= 3, 10, 0) + 
      IF(_website_visitor_count < 3, 5, 0)
    ) AS website_visitor_count_score,
    IF(_career_page > 1, -5, 0) AS career_page_score,
    5 AS visited_website_score
  FROM weekly_web_data
),
weekly_web_score AS (
  SELECT
    * EXCEPT (
      website_time_spent_score,
      website_page_view_score,
      website_visitor_count_score,
      visited_website_score
    ),
    website_time_spent_score AS _website_time_spent_score,
    website_page_view_score AS _website_page_view_score,
    website_visitor_count_score AS _website_visitor_count_score,
    visited_website_score AS _visited_website_score,
    CASE
      WHEN (
        website_time_spent_score + website_page_view_score +
        website_visitor_count_score + visited_website_score + career_page_score
      ) > 40 THEN 40
      ELSE (
        website_time_spent_score + website_page_view_score +
        website_visitor_count_score + visited_website_score + career_page_score
      )
    END AS _web_score_total
  FROM weekly_web_data_score
),
web_last_engagement_dates AS (
  SELECT
    _domain AS _domain,
    _visitorid,
    DATETIME(_timestamp) AS _timestamp,
    _engagementtime AS _website_time_spent,
    _totalPages AS _website_page_view
  FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire` --WHERE 
    --NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
    -- AND _webactivity IS NOT NULL
    --AND (_domain IS NOT NULL AND _domain != '')
),
web_last_engagement_timestamps AS (
  SELECT
    /* _website, */
    _domain,
    MAX(_timestamp) AS last_engaged_date
  FROM web_last_engagement_dates -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
  GROUP BY 1
),
web_last_engagement AS (
  SELECT
    web_data._domain,
    web_data.* EXCEPT (_domain),
    _last_engagement.last_engaged_date AS _last_engagement_web,
    EXTRACT(WEEK FROM _last_engagement.last_engaged_date) AS week_web,
    EXTRACT(YEAR FROM _last_engagement.last_engaged_date) AS year_web
  FROM web_last_engagement_timestamps AS _last_engagement
  RIGHT JOIN weekly_web_score web_data
    ON web_data._domain = _last_engagement._domain
),
all_last_engagements AS (
  SELECT
    main.*,
    email_engagement.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS _last_engagement_email_date,
    --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
    -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
    formfill.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01')) AS formfilled_last_engaged_date,
    cs.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS organic_social_last_engagement,
    search_ads.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS paid_social_engaged_date,
    web_data.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_web AS DATE), DATE('2000-01-01')) AS engagement_web_date,
  FROM contacts AS main
  LEFT JOIN email_last_engagementdate AS email_engagement
    ON (main._domain = email_engagement._domain)
  LEFT JOIN formfill_last_engagementdate AS formfill
    ON (main._domain = formfill._domain)
  LEFT JOIN organic_social_last_engagement AS cs
    ON (main._domain = cs._domain)
  LEFT JOIN paid_social_last_engagement AS search_ads
    ON (main._domain = search_ads._domain)
  LEFT JOIN web_last_engagement AS web_data
    ON (main._domain = web_data._domain)
),
combine_all AS (
  #combine all channel data and calculate into the max data. 
  SELECT
    *,
    (
      COALESCE(_GatedContentscore_total, 0) + COALESCE(_formFilled_webinarscore_total, 0) +
      COALESCE(_email_score, 0) + COALESCE(_paid_ads_score_total, 0) +
      COALESCE(_organic_ads_score_total, 0) + COALESCE(_web_score_total, 0)
    ) AS _total_score,
    CASE
      WHEN (
        _last_engagement_email_date >= formfilled_last_engaged_date
        AND _last_engagement_email_date >= organic_social_last_engagement
        AND _last_engagement_email_date >= paid_social_engaged_date
        AND _last_engagement_email_date >= engagement_web_date
      ) THEN _last_engagement_email_date
      WHEN (
        formfilled_last_engaged_date >= _last_engagement_email_date
        AND formfilled_last_engaged_date >= organic_social_last_engagement
        AND formfilled_last_engaged_date >= paid_social_engaged_date
        AND formfilled_last_engaged_date >= engagement_web_date
      ) THEN formfilled_last_engaged_date
      WHEN (
        paid_social_engaged_date >= _last_engagement_email_date
        AND paid_social_engaged_date >= formfilled_last_engaged_date
        AND paid_social_engaged_date >= organic_social_last_engagement
        AND paid_social_engaged_date >= engagement_web_date
      ) THEN paid_social_engaged_date
      WHEN (
        organic_social_last_engagement >= _last_engagement_email_date
        AND organic_social_last_engagement >= formfilled_last_engaged_date
        AND organic_social_last_engagement >= paid_social_engaged_date
        AND organic_social_last_engagement >= engagement_web_date
      ) THEN organic_social_last_engagement
      WHEN (
        engagement_web_date >= _last_engagement_email_date
        AND engagement_web_date >= formfilled_last_engaged_date
        AND engagement_web_date >= paid_social_engaged_date
        AND engagement_web_date >= organic_social_last_engagement
      ) THEN engagement_web_date
    END AS _last_engagement_date
  FROM all_last_engagements
),
icp_score AS (
  SELECT
    _domain AS _domain,
    total_employee,
    total_score_divide_2,
    total_score,
    max_score
  FROM `x-marketing.logicsource.account_icp_score`
),
all_data AS (
  SELECT
    *,
    EXTRACT(YEAR FROM _last_engagement_date) AS _last_engagemtn_year,
    EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,
    DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 180 THEN (_total_score - 50)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90 THEN (_total_score - 25)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30 THEN (_total_score - 15)
      ELSE _total_score
    END AS _score_new
  FROM combine_all --WHERE _domain =  'pepsico.com'
)
SELECT
  all_data.*,
  icp_score.* EXCEPT (_domain),
  COALESCE(_total_score, 0) + COALESCE(max_score, 0) AS _total_score_icp_intent,
  CASE
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 10
    AND COALESCE(_total_score, 0) + COALESCE(max_score, 0) <= 20 THEN 'Low'
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 21
    AND COALESCE(_total_score, 0) + COALESCE(max_score, 0) <= 49 THEN 'Medium'
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 50 THEN 'High'
    ELSE "Low"
  END AS legend
FROM all_data
LEFT JOIN icp_score
ON all_data._domain = icp_score._domain;

CREATE OR REPLACE TABLE `x-marketing.logicsource.zoominfo_account_engagement_scoring` AS 
WITH account AS (
  SELECT
    temp_account.*,
    "Target" AS _source,
    "Hubspot" AS source_zi_intent,
  FROM temp_account
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _company DESC) = 1
),
zoominfo AS (
  SELECT DISTINCT
    *,
    "Zoominfo" AS _zi_intent
  FROM temp_zoominfo
),
zoominfo_domain AS (
  SELECT
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo._domain
      ELSE mainAcc._domain
    END AS _domain,
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo._company
      ELSE mainAcc._company
    END AS _company,
    CASE
      WHEN mainAcc._industry IS NULL THEN zoominfo._industry
      ELSE mainAcc._industry
    END AS _industry,
    mainAcc.* EXCEPT (_source, _domain, _company, _industry),
    CASE
      WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source
      ELSE "Net New"
    END AS _source,
    CASE
      WHEN zoominfo._domain IS NOT NULL THEN _zi_intent
      ELSE source_zi_intent
    END AS _zi_intent,
  FROM zoominfo
  LEFT JOIN account mainAcc
    USING (_domain)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY zoominfo._company DESC) = 1
),
hubspot_domain AS (
  SELECT
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo_domain._domain
      ELSE mainAcc._domain
    END AS _domain,
    CASE
      WHEN mainAcc._domain IS NULL THEN zoominfo_domain._company
      ELSE mainAcc._company
    END AS _company,
    CASE
      WHEN mainAcc._industry IS NULL THEN zoominfo_domain._industry
      ELSE mainAcc._industry
    END AS _industry,
    mainAcc.* EXCEPT (_source, _domain, _company, _industry),
    CASE
      WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source
      ELSE "Net New"
    END AS _source,
    CASE
      WHEN zoominfo_domain._domain IS NOT NULL THEN _zi_intent
      ELSE mainAcc.source_zi_intent
    END AS _zi_intent,
  FROM account mainAcc
  LEFT JOIN zoominfo_domain
    USING (_domain)
  WHERE zoominfo_domain._domain IS NULL --WHERE _domain NOT IN (SELECT DISTINCT _domain FROM zoominfo_domain)
),
contacts AS (
  SELECT
    *
  FROM zoominfo_domain
  UNION ALL
  SELECT
    *
  FROM hubspot_domain
),
email_last_engagementdate AS (
  SELECT
    *
  FROM temp_email_last_engagementdate
),
form_filled AS (
  SELECT
    _domain,
    SUM(
      CASE
        WHEN (
          _engagement = 'Form Filled'
          AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar')
        ) THEN 1
        ELSE 0
      END
    ) AS _formFilled_webinar,
    SUM(
      CASE
        WHEN (
          _engagement = 'Form Filled'
          AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn')
        )
        OR (
          _engagement = 'Form Filled'
          AND _contentTitle = "Other Content Engagement"
        )
        OR (
          _engagement = 'Form Filled'
          AND _description = "Visited booth"
        ) THEN 1
        ELSE 0
      END
    ) AS _distinctGatedContent,
    SUM(
      CASE
        WHEN _engagement = 'Form Filled'
        AND _description = "Registered" THEN 1
        ELSE 0
      END
    ) AS _distinctWebinarForm,
    SUM(
      CASE
        WHEN _engagement = 'Form Filled'
        AND _description = "Attended event" THEN 1
        ELSE 0
      END
    ) AS _distinctWebinarattended,
    -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
    -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
  FROM `x-marketing.logicsource.db_consolidated_engagements_log`
  WHERE _engagement IN ('Form Filled') --WHERE _domain = 'fedex.com'
  GROUP BY 1
),
form_filled_total AS (
  SELECT
    _domain,
    SUM(_formFilled_webinar) AS _formFilled_webinartotal,
    SUM(_distinctGatedContent) AS _distinctGatedContenttotal,
  FROM form_filled --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
),
form_filled_score AS (
  SELECT
    _domain,
    _formFilled_webinartotal,
    _distinctGatedContenttotal,
    SUM(DISTINCT (IF(_formFilled_webinartotal >= 1, 1 * 50, 0))) AS _formFilled_webinarscore,
    SUM(DISTINCT (IF(_distinctGatedContenttotal >= 1, 1 * 20, 0))) AS _GatedContentscore,
  FROM form_filled_total
  GROUP BY 1, 2, 3
),
formfilled_engagements AS (
  SELECT
    *,
    CASE
      WHEN _GatedContentscore >= 20 THEN 20
      ELSE _GatedContentscore
    END AS _GatedContentscore_total,
    CASE
      WHEN _formFilled_webinarscore >= 50 THEN 50
      ELSE _formFilled_webinarscore
    END AS _formFilled_webinarscore_total,
    CASE
      WHEN _GatedContentscore + _formFilled_webinarscore >= 80 THEN 80
      ELSE _GatedContentscore + _formFilled_webinarscore
    END AS _form_fill_score_total --EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
  FROM form_filled_score
),
form_filled_last_engagements AS (
  SELECT
        _domain,
        MAX(_date) OVER (PARTITION BY _domain ) AS _last_engagement_TS,
      FROM `x-marketing.logicsource.db_consolidated_engagements_log`
      WHERE _engagement IN ('Form Filled')
      QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _date DESC) = 1
),
formfill_last_engagementdate AS (
  SELECT
    formfilled_engagements.*,
    CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
    EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
    EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM form_filled_last_engagements AS _last_engagement
  RIGHT JOIN formfilled_engagements
    ON formfilled_engagements._domain = _last_engagement._domain
),
paid_social_last_engagement AS (
  SELECT
    *
  FROM temp_paid_social_last_engagement
),
organic_social_last_engagement AS (
  SELECT
    *
  FROM temp_organic_social_last_engagement
),
mouseflow_kickfire_timestamps AS (
  SELECT
    _domain,
    _visitorid,
    DATETIME(_timestamp) AS _timestamp,
    EXTRACT(WEEK FROM _timestamp) AS _week,
    EXTRACT(YEAR FROM _timestamp) AS _year,
    _entrypage AS _pageName,
    -- "Web Visit" AS _engagement, 
    CAST(_engagementtime AS INT64) AS _website_time_spent,
    _totalPages AS _website_page_view
  FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire` web
  WHERE NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email')
    AND _webactivity IS NOT NULL
),
weekly_web_data AS (
  SELECT
    _domain,
    -- _week,
    -- _year,
    -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
    COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
    COALESCE(SUM(CASE WHEN _pageName IS NOT NULL THEN 1 END ), 0 ) AS _website_page_view,
    COALESCE(COUNT(DISTINCT _visitorid), 0) AS _website_visitor_count,
    COALESCE(COUNT(DISTINCT CASE WHEN _pageName LIKE "%careers%" THEN _visitorid END ), 0) AS _career_page,
    TRUE AS _visited_website,
    -- MAX(_timestamp) AS last_engaged_date
  FROM mouseflow_kickfire_timestamps
  WHERE LENGTH(_domain) > 2
  GROUP BY 1
),
weekly_web_data_score AS (
  SELECT
    *,
    COALESCE((_website_time_spent), 0) AS website_time_spent_score,
    (
      IF(_website_page_view >= 5, 15, 0) +
      IF(_website_page_view >= 5, 15, 0)
    ) AS website_page_view_score,
    (
      IF(_website_visitor_count >= 3, 10, 0) +
      IF(_website_visitor_count < 3, 5, 0)
    ) AS website_visitor_count_score,
    IF(_career_page > 1, -5, 0) AS career_page_score,
    5 AS visited_website_score
  FROM weekly_web_data
),
weekly_web_score AS (
  SELECT
    * EXCEPT (
      website_time_spent_score,
      website_page_view_score,
      website_visitor_count_score,
      visited_website_score
    ),
    website_time_spent_score AS _website_time_spent_score,
    website_page_view_score AS _website_page_view_score,
    website_visitor_count_score AS _website_visitor_count_score,
    visited_website_score AS _visited_website_score,
    CASE
      WHEN (
        website_time_spent_score + website_page_view_score +
        website_visitor_count_score + visited_website_score + career_page_score
      ) > 40 THEN 40
      ELSE (
        website_time_spent_score + website_page_view_score +
        website_visitor_count_score + visited_website_score + career_page_score
      )
    END AS _web_score_total
  FROM weekly_web_data_score
),
web_last_engagement_dates AS (
  SELECT
    _domain AS _domain,
    _visitorid,
    DATETIME(_timestamp) AS _timestamp,
    _engagementtime AS _website_time_spent,
    _totalPages AS _website_page_view
  FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire`
  WHERE NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email')
    AND _webactivity IS NOT NULL
    AND (
      _domain IS NOT NULL
      AND _domain != ''
    )
),
web_last_engagement_timetamps AS (
  SELECT
    /* _website, */
    _domain,
    MAX(_timestamp) AS last_engaged_date
  FROM web_last_engagement_dates -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
  GROUP BY 1
),
web_last_engagement AS (
  SELECT
    web_data._domain,
    web_data.* EXCEPT (_domain),
    _last_engagement.last_engaged_date AS _last_engagement_web,
    EXTRACT(WEEK FROM _last_engagement.last_engaged_date) AS week_web,
    EXTRACT(YEAR FROM _last_engagement.last_engaged_date) AS year_web
  FROM web_last_engagement_timetamps AS _last_engagement
  RIGHT JOIN weekly_web_score web_data
    ON web_data._domain = _last_engagement._domain
),
all_last_engagements AS (
  SELECT
    main.*,
    email_engagement.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS _last_engagement_email_date,
    --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
    -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
    formfill.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01') ) AS formfilled_last_engaged_date,
    cs.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS organic_social_last_engagement,
    search_ads.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS paid_social_engaged_date,
    web_data.* EXCEPT (_domain),
    COALESCE(CAST(_last_engagement_web AS DATE), DATE('2000-01-01')) AS engagement_web_date,
  FROM contacts AS main
  LEFT JOIN email_last_engagementdate AS email_engagement
    ON (main._domain = email_engagement._domain)
  LEFT JOIN formfill_last_engagementdate AS formfill
    ON (main._domain = formfill._domain)
  LEFT JOIN organic_social_last_engagement AS cs
    ON (main._domain = cs._domain)
  LEFT JOIN paid_social_last_engagement AS search_ads
    ON (main._domain = search_ads._domain)
  LEFT JOIN web_last_engagement AS web_data
    ON (main._domain = web_data._domain)
),
combine_all AS (
  #combine all channel data and calculate into the max data. 
  SELECT
    *,
    (
      COALESCE(_GatedContentscore_total, 0) + COALESCE(_formFilled_webinarscore_total, 0) +
      COALESCE(_email_score, 0) + COALESCE(_paid_ads_score_total, 0) +
      COALESCE(_organic_ads_score_total, 0) + COALESCE(_web_score_total, 0)
    ) AS _total_score,
    CASE
      WHEN (
        _last_engagement_email_date >= formfilled_last_engaged_date
        AND _last_engagement_email_date >= organic_social_last_engagement
        AND _last_engagement_email_date >= paid_social_engaged_date
        AND _last_engagement_email_date >= engagement_web_date
      ) THEN _last_engagement_email_date
      WHEN (
        formfilled_last_engaged_date >= _last_engagement_email_date
        AND formfilled_last_engaged_date >= organic_social_last_engagement
        AND formfilled_last_engaged_date >= paid_social_engaged_date
        AND formfilled_last_engaged_date >= engagement_web_date
      ) THEN formfilled_last_engaged_date
      WHEN (
        paid_social_engaged_date >= _last_engagement_email_date
        AND paid_social_engaged_date >= formfilled_last_engaged_date
        AND paid_social_engaged_date >= organic_social_last_engagement
        AND paid_social_engaged_date >= engagement_web_date
      ) THEN paid_social_engaged_date
      WHEN (
        organic_social_last_engagement >= _last_engagement_email_date
        AND organic_social_last_engagement >= formfilled_last_engaged_date
        AND organic_social_last_engagement >= paid_social_engaged_date
        AND organic_social_last_engagement >= engagement_web_date
      ) THEN organic_social_last_engagement
      WHEN (
        engagement_web_date >= _last_engagement_email_date
        AND engagement_web_date >= formfilled_last_engaged_date
        AND engagement_web_date >= paid_social_engaged_date
        AND engagement_web_date >= organic_social_last_engagement
      ) THEN engagement_web_date
    END AS _last_engagement_date
  FROM all_last_engagements
),
icp_score AS (
  SELECT
    _domain AS _domain,
    total_employee,
    total_score_divide_2,
    total_score,
    max_score
  FROM `x-marketing.logicsource.account_icp_score`
),
all_data AS (
  SELECT
    *,
    EXTRACT(YEAR FROM _last_engagement_date) AS _last_engagemtn_year,
    EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,
    DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 180 THEN (_total_score - 50)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90 THEN (_total_score - 25)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
      WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30 THEN (_total_score - 15)
      ELSE _total_score
    END AS _score_new
  FROM combine_all --WHERE _domain =  'pepsico.com'
)
SELECT
  all_data.*,
  icp_score.* EXCEPT (_domain),
  COALESCE(_total_score, 0) + COALESCE(max_score, 0) AS _total_score_icp_intent,
  CASE
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 10
    AND COALESCE(_total_score, 0) + COALESCE(max_score, 0) <= 20 THEN 'Low'
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 21
    AND COALESCE(_total_score, 0) + COALESCE(max_score, 0) <= 49 THEN 'Medium'
    WHEN COALESCE(_total_score, 0) + COALESCE(max_score, 0) >= 50 THEN 'High'
    ELSE "Low"
  END AS legend
FROM all_data
LEFT JOIN icp_score
  ON all_data._domain = icp_score._domain;