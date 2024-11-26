
-- CREATE OR REPLACE TABLE `logicsource.contact_engagement_scoring` AS
TRUNCATE TABLE `x-marketing.logicsource.contact_engagement_scoring`;

INSERT INTO `x-marketing.logicsource.contact_engagement_scoring` (
  _id,
  _email,
  _name,
  _domain,
  jobtitle,
  _function,
  _jobrole,
  _mqldate,
  _source,
  _latest_source,
  _seniority,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _persona,
  _lifecycleStage,
  leadscore,
  _leadstatus,
  _ipqc_check,
  _hubspotscore,
  company_id,
  _company_segment,
  _lead_segment,
  _segment,
  _property_leadstatus,
  _companylinkedinbio,
  _company_linkedin,
  _employee_range,
  _employee_range_c,
  _numberofemployees,
  _annualrevenue,
  _annual_revenue_range,
  _annual_revenue_range_c,
  salesforceaccountid,
  salesforceleadid,
  salesforcecontactid,
  _emailOpentotal,
  _emailClickedtotal,
  _emailopenscore_more,
  _emailopenscore,
  _emailclickscore_more,
  _email_score,
  _last_engagement_email,
  _email_week,
  _email_year,
  _last_engagement_email_date,
  _formFilled_webinartotal,
  _distinctGatedContenttotal,
  _formFilled_webinarscore,
  _GatedContentscore,
  _GatedContentscore_total,
  _formFilled_webinarscore_total,
  _form_fill_score_total,
  _last_engagement_formfilled,
  _formfilled_week,
  _formfilled_year,
  formfilled_last_engaged_date,
  _organicsharetotal,
  _organiccommenttotal,
  _organicfollowtotal,
  _organicvisittotal,
  _organicclick_liketotal,
  _organischarescore,
  _organiccommentscore,
  _organicfollowscore,
  _organicvisitscore,
  _organicclick_likescore,
  _organic_ads_score_total,
  _last_engagement_organc_social,
  _organc_social_week,
  _organc_social_year,
  organic_social_last_engagement,
  _paidadssharetotal,
  _paidadscommenttotal,
  _paidadsfollowtotal,
  _paidadsvisittotal,
  _paidadsclick_liketotal,
  _paidadssharescore,
  _paidadscommentscore,
  _paidadsfollowscore,
  _paidadsvisitscore,
  _paidadsclick_likescore,
  _paid_ads_score_total,
  _last_engagement_paid_social,
  _paid_social_week,
  _paid_social_year,
  paid_social_engaged_date,
  _total_score,
  _last_engagement_date,
  _last_engagemtn_year,
  _last_engagement_weekt,
  days_since_last_engaged,
  _score_new,
  hubspot_score,
  total_score_ICP,
  _total_score_icp_intent,
  legend
)

WITH contacts AS (
SELECT
      CAST(vid AS STRING) AS _id,
      property_email.value AS _email,
      COALESCE(CONCAT(property_firstname.value, ' ', property_lastname.value),property_firstname.value) AS _name,
      COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value , RIGHT(property_email.value, LENGTH(property_email.value)-STRPOS(property_email.value, '@'))) AS _domain, 
      properties.jobtitle.value AS jobtitle,
      properties.job_function.value AS _function,
      CASE WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value ELSE property_job_role.value END AS _jobrole,
      properties.hs_lifecyclestage_marketingqualifiedlead_date.value AS _mqldate,
      properties.hs_analytics_source.value AS _source,
      properties.hs_latest_source.value AS _latest_source,
      CASE WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value ELSE property_management_level.value END AS _seniority,
      property_phone.value AS _phone,
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
      CAST(l.lead_score__c AS INT64) AS leadscore,
      properties.hs_lead_status.value AS _leadstatus,
      properties.ipqc_check.value AS _ipqc_check,
      property_hubspotscore.value AS _hubspotscore,
      associated_company.company_id,
      associated_company.properties.segment__c.value AS _company_segment,
      property_lead_segment.value AS _lead_segment, 
      property_segment__c.value AS _segment, 
      property_leadstatus.value AS _property_leadstatus, 
      associated_company.properties.linkedinbio.value AS _companylinkedinbio, 
      associated_company.properties.linkedin_company_page.value AS _company_linkedin, 
      associated_company.properties.employee_range.value AS _employee_range, 
      associated_company.properties.employee_range_c.value AS _employee_range_c, 
      CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees, 
      CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue, 
      associated_company.properties.annual_revenue_range.value AS _annual_revenue_range, 
      associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
      associated_company.properties.salesforceaccountid.value AS salesforceaccountid, 
      properties.salesforceleadid.value AS salesforceleadid,
      properties.salesforcecontactid.value AS salesforcecontactid,
      
    FROM
      `x-marketing.logicsource_hubspot.contacts` k
      LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
    WHERE
     property_email.value IS NOT NULL
      AND property_email.value NOT LIKE '%2x.marketing%'
      AND property_email.value NOT LIKE '%logicsource%'
    QUALIFY ROW_NUMBER() OVER( PARTITION BY property_email.value,CAST(vid AS STRING) ORDER BY properties.createdate.value DESC) = 1
)
,
email_opened_clicked AS (
  SELECT
      _domain,
      _email,
      _id,
      SUM(CASE WHEN _engagement = 'Email Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
      SUM( CASE WHEN _engagement = 'Email Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      FROM`logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
),

email_engagements_total AS (
  SELECT  
     _domain,
     _email,
     _id,
     SUM(_emailOpened) AS _emailOpentotal, 
     SUM(_emailClicked) AS _emailClickedtotal, 
     FROM email_opened_clicked
    --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC
),

email_engagements_score AS (
      SELECT _domain,_email,_id,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 5 THEN  1 * 5 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
  
  ((CASE WHEN _emailOpentotal >= 10 THEN 1 * 10 ELSE 0 END)+(CASE WHEN _emailOpentotal >= 5 THEN  1 * 5 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 2 THEN 1 * 10 ELSE 0 END) ) AS _email_score
    FROM email_engagements_total
  GROUP BY 1,2,3,4,5
  ORDER BY _emailOpentotal DESC
),

email_engagements AS (
  SELECT * EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score  
  FROM email_engagements_score
)
, 
email_last_engagements AS (
    SELECT 
    _domain,
    _email,
    _id,
    MAX(_date) OVER(PARTITION BY _domain,_email,_id)  AS _last_engagement_TS
    FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Email Opened', 'Email Clicked')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _domain,_email,_id  ORDER BY _date DESC) = 1
), 

email_last_engagementdate AS(
 SELECT email_engagements.*,
 CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_email,
 EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
 EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
 FROM email_last_engagements AS _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._id = _last_engagement._id
)
,

form_filled AS (
  SELECT
      _domain,
      _email,
      _id,
       SUM( CASE WHEN (_engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar'))  THEN 1 ELSE 0 END ) AS _formFilled_webinar,  
     SUM( CASE WHEN (_engagement = 'Form Filled' AND  NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn')) OR (_engagement = 'Form Filled' AND _contentTitle = "Other Content Engagement") OR (_engagement = 'Form Filled' AND _description = "Visited booth")THEN 1 ELSE 0 END) AS _distinctGatedContent,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Registered" THEN 1 ELSE 0 END) AS _distinctWebinarForm,
      SUM( CASE WHEN _engagement = 'Form Filled' AND _description = "Attended event" THEN 1 ELSE 0 END)  AS _distinctWebinarattended,
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
),

form_filled_total AS (
  SELECT  
     _domain,
     _email,
     _id,
     SUM(_formFilled_webinar) AS _formFilled_webinartotal, 
     SUM(_distinctGatedContent) AS _distinctGatedContenttotal, 
     FROM form_filled

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
),

form_filled_score AS (
    SELECT 
  _domain,
  _email,_id,
  _formFilled_webinartotal,
  _distinctGatedContenttotal, 
    SUM(DISTINCT(CASE WHEN _formFilled_webinartotal >= 1 THEN 1 * 50 ELSE 0 END)) AS _formFilled_webinarscore,
    SUM(DISTINCT(CASE WHEN _distinctGatedContenttotal >= 1 THEN  1 * 20 ELSE 0 END)) AS _GatedContentscore,
    
    FROM form_filled_total
  GROUP BY 1,2,3,4,5
),

formfilled_engagements AS (
  SELECT *,CASE WHEN  _GatedContentscore  >= 20 THEN 20 ELSE _GatedContentscore END AS _GatedContentscore_total,
  CASE WHEN  _formFilled_webinarscore  >= 50 THEN 50 ELSE _formFilled_webinarscore END AS _formFilled_webinarscore_total,
  CASE WHEN  _GatedContentscore + _formFilled_webinarscore >= 80 THEN 80 ELSE _GatedContentscore + _formFilled_webinarscore END AS _form_fill_score_total 
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM form_filled_score
)
, 

form_filled_last_engagements AS (
      SELECT _domain,_email,_id,
      MAX(_date) OVER(PARTITION BY _domain,_email,_id)  AS _last_engagement_TS,
      
      FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Form Filled')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _domain,_email,_id  ORDER BY _date DESC) = 1
),

formfill_last_engagementdate AS(
  SELECT formfilled_engagements.*,
  CAST(_last_engagement._last_engagement_TS AS DATE) AS _last_engagement_formfilled,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _formfilled_week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _formfilled_year
  FROM form_filled_last_engagements AS _last_engagement 
      RIGHT JOIN formfilled_engagements ON  formfilled_engagements._id = _last_engagement._id
),

paid_social AS (
  SELECT  _domain,_email,_id,
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _paidadsshare,  
     SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _paidadscomment,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _paidadsfollow,  
    SUM( CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _paidadsvisit,  
    SUM( CASE WHEN  _engagement = 'Paid Ads'  AND REGEXP_CONTAINS(LOWER(_contentTitle), 'click|like') THEN 1 ELSE 0 END ) AS  _paidadsclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Paid Ads')
    
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
),

paid_social_total AS (
  SELECT  
     _domain,_email,_id,
     SUM(_paidadsshare) AS _paidadssharetotal, 
    SUM(_paidadscomment) AS _paidadscommenttotal,
    SUM(_paidadsfollow) AS _paidadsfollowtotal,
    SUM(_paidadsvisit) AS _paidadsvisittotal,
    SUM(_paidadsclick_like) AS _paidadsclick_liketotal,
  FROM paid_social

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
),

paid_social_score AS (
  SELECT _domain,_email,_id,_paidadssharetotal,_paidadscommenttotal,_paidadsfollowtotal,_paidadsvisittotal,_paidadsclick_liketotal,
    SUM(DISTINCT(CASE WHEN _paidadssharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _paidadssharescore,
    SUM(DISTINCT(CASE WHEN _paidadscommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _paidadscommentscore,
    SUM(DISTINCT(CASE WHEN _paidadsfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _paidadsfollowscore,
    SUM(DISTINCT(CASE WHEN _paidadsvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsvisitscore,
    SUM(DISTINCT(CASE WHEN _paidadsclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _paidadsclick_likescore,
    
    FROM paid_social_total
  GROUP BY 1,2,3,4,5,6,7,8
)

,paid_social_engagements AS (
  SELECT *,
  CASE WHEN  _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore >= 35 THEN 35 ELSE _paidadssharescore + _paidadscommentscore + _paidadsfollowscore +  _paidadsvisitscore + _paidadsclick_likescore  END AS _paid_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM paid_social_score
)
, 

paid_social_last_engagement_dates AS (
  SELECT _domain,_email,CASE WHEN _email IS NULL THEN _domain
  WHEN _domain  IS NULL THEN _email
  ELSE CONCAT(_email, " ",_domain) END AS _id,
    MAX(_date) OVER(PARTITION BY _domain,_email,CASE WHEN _email IS NULL THEN _domain
  WHEN _domain  IS NULL THEN _email
  ELSE CONCAT(_email, " ",_domain) END )  AS _last_engagement_TS
   FROM  `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ("Paid Ads")
    QUALIFY ROW_NUMBER() OVER(PARTITION BY 
    _domain,_email,_id  ORDER BY _date DESC) = 1
),

paid_social_last_engagement AS(
 SELECT paid_social_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _paid_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _paid_social_year
            FROM paid_social_last_engagement_dates AS _last_engagement 
  RIGHT JOIN paid_social_engagements ON  paid_social_engagements._id = _last_engagement ._id
),

organic_social AS (
  SELECT  _domain,_email,_id,
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Share' THEN 1 ELSE 0 END ) AS  _organicsshare,  
     SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Comment' THEN 1 ELSE 0 END ) AS  _organiccomment,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Follow' THEN 1 ELSE 0 END ) AS  _organicfollow,  
    SUM( CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Visit'THEN 1 ELSE 0 END ) AS  _organicvisit,  
    SUM( CASE WHEN  _engagement = 'Organic Social'  AND REGEXP_CONTAINS(LOWER(_contentTitle), 'click|like') THEN 1 ELSE 0 END ) AS  _organicclick_like,  
    
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM `logicsource.db_consolidated_engagements_log`
    WHERE
      _engagement IN ('Organic Social')
    --WHERE _domain = 'fedex.com'
    GROUP BY 1,2,3
),

organic_social_total AS (
  SELECT  
     _domain,_email,_id,
     SUM(_organicsshare) AS _organicsharetotal, 
    SUM(_organiccomment) AS _organiccommenttotal,
    SUM(_organicfollow) AS _organicfollowtotal,
    SUM(_organicvisit) AS _organicvisittotal,
    SUM(_organicclick_like) AS _organicclick_liketotal,
  FROM organic_social

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2,3
  ORDER BY 1, 3 DESC, 2 DESC
),

organic_social_score AS (
  SELECT _domain,_email,_id,_organicsharetotal,_organiccommenttotal,_organicfollowtotal,_organicvisittotal,_organicclick_liketotal,
    SUM(DISTINCT(CASE WHEN _organicsharetotal >= 1 THEN 1 * 15 ELSE 0 END)) AS _organischarescore,
    SUM(DISTINCT(CASE WHEN _organiccommenttotal >= 1 THEN 1 * 10 ELSE 0 END)) AS _organiccommentscore,
    SUM(DISTINCT(CASE WHEN _organicfollowtotal >= 1 THEN 1 * 4 ELSE 0 END)) AS _organicfollowscore,
    SUM(DISTINCT(CASE WHEN _organicvisittotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicvisitscore,
    SUM(DISTINCT(CASE WHEN _organicclick_liketotal >= 1 THEN 1 * 5 ELSE 0 END)) AS _organicclick_likescore,
    
    FROM organic_social_total
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY _organicsharetotal  DESC
)

,organic_social_engagements AS (
  SELECT *,
  CASE WHEN  _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore >= 35 THEN 35 ELSE _organischarescore + _organiccommentscore + _organicfollowscore +  _organicvisitscore + _organicclick_likescore  END AS _organic_ads_score_total
--EXCEPT (_email_score), CASE WHEN _email_score >= 15 THEN 15 ELSE _email_score END AS _email_score 
FROM organic_social_score
),





organic_social_last_engagement AS(
 SELECT organic_social_engagements.*, 
 _last_engagement._last_engagement_TS AS _last_engagement_organc_social,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organc_social_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organc_social_year
            FROM paid_social_last_engagement_dates AS _last_engagement 
  RIGHT JOIN organic_social_engagements ON  organic_social_engagements._id = _last_engagement ._id
), 

all_last_engagements AS (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      --paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
     -- organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      formfill.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_formfilled AS DATE), DATE('2000-01-01')) AS  formfilled_last_engaged_date,
     
      cs.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_organc_social AS DATE), DATE('2000-01-01')) AS  organic_social_last_engagement,
      search_ads.* EXCEPT(_domain,_email,_id), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  paid_social_engaged_date,
      FROM contacts AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._id = email_engagement._id )
    LEFT JOIN formfill_last_engagementdate AS formfill ON (main._id = formfill._id)
    LEFT JOIN organic_social_last_engagement AS cs ON (main._id = cs._id)
    LEFT JOIN paid_social_last_engagement AS search_ads ON (main._id = search_ads._id) 
),


combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(_GatedContentscore_total ,0) + COALESCE(_formFilled_webinarscore_total,0)  + COALESCE(_email_score,0) 
  + COALESCE(_paid_ads_score_total,0) + COALESCE(_organic_ads_score_total,0) 
   ) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= formfilled_last_engaged_date
     AND
  
     _last_engagement_email_date >= organic_social_last_engagement
     AND 
     _last_engagement_email_date >= paid_social_engaged_date
     ) THEN _last_engagement_email_date
     WHEN (
      formfilled_last_engaged_date >= _last_engagement_email_date
      AND 
      formfilled_last_engaged_date >= organic_social_last_engagement
      AND 
      formfilled_last_engaged_date >= paid_social_engaged_date
    
    ) THEN formfilled_last_engaged_date
    WHEN (
     paid_social_engaged_date >= _last_engagement_email_date
      AND 
      paid_social_engaged_date >= formfilled_last_engaged_date
      AND 
      paid_social_engaged_date >= organic_social_last_engagement
    
    ) THEN paid_social_engaged_date
     WHEN (
     organic_social_last_engagement >= _last_engagement_email_date
      AND 
      organic_social_last_engagement>= formfilled_last_engaged_date
      AND 
       organic_social_last_engagement >= paid_social_engaged_date
    
    ) THEN organic_social_last_engagement
  
    END AS _last_engagement_date
    FROM all_last_engagements
), icp_score AS (
  SELECT 
  _prospectid AS _id, 
  hubspot_score, 
  total_score_ICP 
  FROM `x-marketing.logicsource.contact_icp_score` 
),all_data AS (
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 25)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 20)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 15)
  ELSE _total_score
  END AS _score_new
FROM combine_all
--WHERE _domain =  'pepsico.com'
ORDER BY _last_engagement_date DESC,_total_score DESC
) 
SELECT all_data.*,
icp_score.* EXCEPT(_id),
COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)   AS _total_score_icp_intent,
CASE WHEN COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 10 AND COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  <= 20 THEN 'Low'
WHEN  COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 21 AND COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  <= 49 THEN 'Medium'
WHEN COALESCE(_total_score,0)   + COALESCE(total_score_ICP,0)  >= 50  THEN 'High' ELSE "Low" END AS legend
FROM all_data
LEFT JOIN icp_score on all_data._id = icp_score._id ; 