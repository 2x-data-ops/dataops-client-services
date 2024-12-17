TRUNCATE TABLE `x-marketing.corcentric.db_email_activity_log`;
INSERT INTO `x-marketing.corcentric.db_email_activity_log` (
  _sdc_sequence,	
  _campaignID,	
  _emailcampaignID,	
  _email_campaign_name,	
  _campaign_name,	
  _subject,	
  _timestamp,	
  _engagement,	
  _description,	
  _link,	
  _device,	
  _utm_source,	
  _utm_medium,	
  _utm_content,	
  _id,	
  _name,	
  _domain,	
  _job_title,	
  _phone,	
  _company,	
  _revenue,	
  _industry,	
  _city,	
  _state,	
  _country,	
  _persona,	
  _leadsourcedetail,	
  _most_recent_lead_source,	
  _most_recent_lead_source_detail,	
  person_status,
  _last_qp_date,
  _last_discovery_date,
  _last_mql_date,
  _last_sal_date,
  _last_sql_date,
  _last_opportunity_date,
  _zoominfo_management_level,
  _job_function,
  _zoominfo_job_function,
  _annual_revenue_ranges_segment,
  _industry_segment,
  _job_function_segment,
  _email,	
  _assettitle,	
  _segment,	
  _2x_campaigns
)

WITH prospect_info AS (
  SELECT DISTINCT 
    CAST(marketo.id AS STRING) AS _id,
    marketo.email AS _email,
    CONCAT(marketo.firstname,' ', marketo.lastname) AS _name,
    RIGHT(marketo.email, LENGTH(marketo.email) - STRPOS(marketo.email, '@')) AS _domain, 
    marketo.title AS _job_title,
    marketo.phone AS _phone,
    company AS _company,
    CAST(annualrevenue AS STRING) AS _revenue,
    industry AS _industry,
    city AS _city,
    state AS _state, 
    marketo.country AS _country,
    "" AS _persona,
    marketo.lead_source_original_detail__c AS _leadsourcedetail,
    marketo.leadsource AS _most_recent_lead_source,
    marketo.lead_source_detail__c AS _most_recent_lead_source_detail,
    contact.status__c AS person_status,
    ds_03_last_qp AS _last_qp_date,
    ds_04_last_discovery AS _last_discovery_date,
    ds_05_last_mql AS _last_mql_date,
    ds_06_last_sal AS _last_sal_date,
    ds_07_last_sql AS _last_sql_date,	
    ds_08_last_opportunity AS _last_opportunity_date,
    marketo.zoominfo_management_level__c AS _zoominfo_management_level,
    dscorgpkg__job_function__c AS _job_function,
    marketo.zoominfo_job_function__c AS _zoominfo_job_function,
    CASE
      WHEN annualrevenue > 999999999.99 THEN 'High - $1B+'
      WHEN annualrevenue BETWEEN 499999999.99 AND 999999999.99 THEN 'Medium - $500M - $1B'
      WHEN annualrevenue BETWEEN 249999999.99 AND 499999999.99 THEN 'Low - $250M - $500M'
      WHEN annualrevenue BETWEEN 25000000.00 AND 249999999.99 THEN 'Below ICP - $25M - $250M'
      ELSE NULL
    END AS _annual_revenue_ranges_segment,
    _industry_segment,
    _job_function_segment
  FROM `x-marketing.corcentric_marketo.leads` marketo
  LEFT JOIN `x-marketing.corcentric.salesforce_qp_and_mql` qp
    ON qp.email = marketo.email
  LEFT JOIN `x-marketing.corcentric_salesforce.Contact` contact
    ON contact.email = marketo.email
  LEFT JOIN `x-marketing.corcentric.db_industry_segment` segment
    ON segment._id = marketo.id
  WHERE marketo.email IS NOT NULL
    AND marketo.email NOT LIKE '%2x.marketing%'
    AND marketo.email NOT LIKE '%corcentric.com%'
    --AND marketo.email = 'tom.martin@bordendairy.com'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY marketo.email 
    ORDER BY ds_05_last_mql DESC) = 1
),
email_sent AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Sent' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_send_email` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%' 
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
email_delivered AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Delivered' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_email_delivered` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1 
),
email_open AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Opened' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    device AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_open_email` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01' --AND leadid = 3277762
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
email_click AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Clicked' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    link AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_click_email` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
open_click AS ( --merge open and click data
  SELECT * FROM email_open
  UNION ALL
  SELECT * FROM email_click
),
new_open AS ( --to populate the data in Clicked but not appear in Opened list
  SELECT 
    _sdc_sequence,
    _campaignID,
    _emailcampaignID,
    _email_campaign_name,
    _campaign_name,
    _subject,
    _email,
    _timestamp,
    'Opened' AS _engagement,
    _description,
    _leadid,
    _link,
    _device
  FROM open_click
  WHERE _engagement <> 'Opened' 
    AND _engagement = 'Clicked'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY _leadid, _campaignID 
    ORDER BY _timestamp DESC) = 1
), 
new_open_consolidate AS (
  SELECT 
    * 
  FROM email_open
  UNION ALL
  SELECT 
    * 
  FROM new_open
),
final_open AS (
  SELECT 
    *
  FROM new_open_consolidate
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY _leadid, _campaignID 
    ORDER BY _timestamp DESC) = 1
),
email_hard_bounce AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Hard Bounced' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_email_bounced` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
email_soft_bounce AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Soft Bounced' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_email_bounced_soft` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
email_download AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    REGEXP_REPLACE(primary_attribute_value, r'\..*$', '') AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Fill Out Form' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_fill_out_form` activity
  WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
    AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
    AND CAST(activitydate AS DATE) >= '2024-01-01'
    AND primary_attribute_value NOT LIKE '%Newsletter%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
linkedin_lead_gen_form AS (
  SELECT
    activity._sdc_sequence,
    CAST(NULL AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    '' AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'LI Lead Gen Form' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_fill_out_linkedin_lead_gen_form` activity
  WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
    AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
    AND CAST(activitydate AS DATE) >= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
new_drift_conversation AS (
  SELECT
    activity._sdc_sequence,
    CAST(NULL AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    '' AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Drift Conversation' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_new_drift_conversation` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
new_drift_meeting AS (
  SELECT
    activity._sdc_sequence,
    CAST(NULL AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    '' AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Drift Meeting' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_new_drift_meeting` activity
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
visit_webpage AS (
  SELECT
    activity._sdc_sequence,
    CAST(campaignid AS STRING) AS _campaignID,
    CAST(primary_attribute_value_id AS STRING) AS _emailcampaignID,
    primary_attribute_value AS _email_campaign_name,
    campaign.name AS _campaign_name,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    CONCAT('Webpage Visit', " ", search_engine) AS _engagement,
    description AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_marketo_v2.activities_visit_webpage` activity
  LEFT JOIN `x-marketing.corcentric_marketo_v2.campaigns` campaign
    ON campaign.id = campaignid 
  WHERE CAST(activitydate AS DATE) >= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC) = 1
),
activity_log_list AS (
  SELECT 
    * 
  FROM email_open
  UNION ALL
  SELECT 
    * 
  FROM email_click
  UNION ALL
  SELECT 
    * 
  FROM linkedin_lead_gen_form
  UNION ALL
  SELECT 
    * 
  FROM new_drift_conversation
  UNION ALL
  SELECT 
    * 
  FROM new_drift_meeting
  UNION ALL
  SELECT 
    * 
  FROM visit_webpage
),
qp_last_touch AS (
  SELECT 
    _sdc_sequence,
    _campaignID,
    _emailcampaignID,
    _email_campaign_name,
    _campaign_name,
    _subject,
    prospect_info._email,
    _timestamp,
    CONCAT('QP', ' ', _engagement) AS _engagement,
    _description,
    _leadid,
    _link,
    _device
  FROM activity_log_list AS a
  JOIN prospect_info
    ON a._leadid = prospect_info._id
  WHERE _last_qp_date IS NOT NULL 
    AND _engagement IS NOT NULL
  --AND TIMESTAMP_DIFF(_timestamp, _last_qp_date, DAY) = 0
    AND TIMESTAMP_DIFF(_timestamp, _last_qp_date, HOUR) <= 48
  --AND prospect_info._email IN ('vinod.pasi@seagate.com')
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY _email 
    ORDER BY _timestamp DESC) = 1
),
-- select * from qp_last_touch
-- WHERE CAST(_timestamp AS DATE) >= '2024-07-01',
conversions AS (
  SELECT
    activity._sdc_sequence,
    campaignid AS _campaignID,
    '' AS _emailcampaignID,
    '' AS _email_campaign_name,
    campaign.name AS _campaign_name,
    '' AS _subject,
    l.email AS _email,
    activity.lastmodifieddate AS _timestamp,
    'Conversion' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link,
    '' AS _device
  FROM `x-marketing.corcentric_salesforce.CampaignMember` activity
  LEFT JOIN `x-marketing.corcentric_salesforce.Campaign` campaign
    ON campaign.id = campaignid
  JOIN `x-marketing.corcentric_salesforce.Lead` l
    ON l.id = activity.leadid
  WHERE activity.status = 'Converted' 
    AND CAST(activity.lastmodifieddate AS DATE) >= '2024-01-01'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY l.email, campaignid 
    ORDER BY activity.lastmodifieddate DESC) = 1
),
engagements_combined AS (
  SELECT 
    * 
  FROM email_sent
  UNION ALL
  SELECT 
    * 
  FROM email_delivered
  UNION ALL
  SELECT 
    * 
  FROM email_open
  UNION ALL
  SELECT 
    * 
  FROM email_click
  UNION ALL
  SELECT 
    * 
  FROM email_hard_bounce
  UNION ALL
  SELECT 
    * 
  FROM email_soft_bounce
  UNION ALL
  SELECT 
    * 
  FROM linkedin_lead_gen_form
  UNION ALL
  SELECT 
    * 
  FROM new_drift_conversation
  UNION ALL
  SELECT 
    * 
  FROM new_drift_meeting
  UNION ALL
  SELECT 
    * 
  FROM visit_webpage
),
final_engagements AS (
  SELECT
    engagements.* EXCEPT(_leadid, _email),
    COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
    REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
    REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utm_content,
    prospect_info.* EXCEPT(_email),
    prospect_info._email
  FROM engagements_combined AS engagements
  JOIN prospect_info
    ON engagements._leadid = prospect_info._id
),
final_engagements_plus_qp_touch AS (
  SELECT
    engagements.* EXCEPT(_leadid, _email),
    COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
    REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
    REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utm_content,
    prospect_info.* EXCEPT(_email),
    engagements._email
  FROM qp_last_touch AS engagements
  JOIN prospect_info
    ON engagements._email = prospect_info._email
),
final_engagements_plus_conversions AS (
  SELECT
    engagements.* EXCEPT(_leadid, _email),
    COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
    REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
    REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utm_content,
    prospect_info.* EXCEPT(_email),
    engagements._email
  FROM conversions AS engagements
  JOIN prospect_info
    ON engagements._email = prospect_info._email
),
engagements_combined_conversions AS (
  SELECT 
    * 
  FROM final_engagements
  UNION ALL
  SELECT 
    * 
  FROM final_engagements_plus_qp_touch
  UNION ALL
  SELECT 
    * 
  FROM final_engagements_plus_conversions
),
airtable_info AS (
  SELECT DISTINCT 
    REGEXP_REPLACE(_code, r'\..*$', '') AS _campaign_name,
    _assettitle,
    _segment
  FROM `x-marketing.corcentric_mysql.db_airtable_email`
)
SELECT 
  engagements.*,
  _assettitle,
  _segment,
  IF(airtable_info._campaign_name IS NOT NULL, TRUE, FALSE) AS _2x_campaigns
FROM engagements_combined_conversions AS engagements
LEFT JOIN airtable_info
  ON engagements._campaign_name = airtable_info._campaign_name;

--change program member data
-- has attended event
TRUNCATE TABLE `x-marketing.corcentric.db_consolidate_ads`;
INSERT INTO `x-marketing.corcentric.db_consolidate_ads` (
  _adid,
  _campaignID,
  _campaign_name,
  _timestamp,
  _spend,
  _clicks,
  _impressions,
  _conversions,
  _platform 
)
WITH linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS TIMESTAMP) AS _timestamp,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions,
    SUM(external_website_conversions) AS _conversions
  FROM `x-marketing.corcentric_linkedin_ads.ad_analytics_by_creative`
  WHERE start_at IS NOT NULL
  GROUP BY creative_id, start_at
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.corcentric_linkedin_ads.creatives`
),
campaigns AS (
  SELECT 
    id AS _campaignID,
    name AS _campaign_name
  FROM `x-marketing.corcentric_linkedin_ads.campaigns`
),
linkedin_combined AS (
  SELECT
    linkedin_ads._adid,
    CAST(campaigns._campaignID AS STRING) AS _campaignID,
    campaigns._campaign_name,
    linkedin_ads._timestamp,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions,
    'LinkedIn' AS _platform
  FROM linkedin_ads
  RIGHT JOIN ads_title 
    ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN campaigns 
    ON campaigns._campaignID = ads_title.campaign_id
),
--Google Display
ad_counts AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM `x-marketing.corcentric_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.corcentric_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
  WHERE ad.name IS NOT NULL
  GROUP BY ad.ad_group_id, ad_group_name, campaign_name, date
),
adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    '' AS _adcopy,
    '' AS _ctacopy,
    report.ad_group_id, 
    report.ad_group_name, 
    report.campaign_id,
    report.campaign_name, 
    ad.name AS ad_name, 
    report.date AS _date,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent, 
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks, 
    impressions / c.ad_count AS adjusted_impressions,
    ad_count
  FROM `x-marketing.corcentric_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.corcentric_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
  JOIN ad_counts AS c 
    ON ad.ad_group_id = c.ad_group_id 
    AND report.date = c.date
  WHERE ad.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ad.id, campaign_id, report.date 
    ORDER BY report.date DESC) = 1
),
google_display_combined AS (
  SELECT
    _adid,
    CAST(campaign_id AS STRING) AS _campaignID,
    campaign_name,
    _date,
    SUM(CAST(adjusted_spent AS NUMERIC)) AS total_spent,
    SUM(CAST(adjusted_clicks AS NUMERIC)) AS total_clicks,
    SUM(CAST(adjusted_impressions AS NUMERIC)) AS total_impressions,
    SUM(CAST(adjusted_conversions AS NUMERIC)) AS total_conversions,
    'Google Display' AS _platform
  FROM adjusted_metrics
  GROUP BY ALL
),
-- Google SEM
google_overview AS (
  SELECT
    CAST(id AS STRING) AS _adid,
    CAST(campaign_id AS STRING) AS _campaignID,
    campaign_name,
    date AS _date,
    CAST(cost_micros / 1000000 AS NUMERIC) AS _spent,
    clicks AS _clicks,
    impressions AS _impressions,
    conversions AS _conversions
  FROM `x-marketing.corcentric_google_ads.ad_performance_report` report
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id, id 
    ORDER BY _sdc_received_at DESC) = 1
),
aggregated AS (
  SELECT 
    * EXCEPT (_spent, _clicks, _impressions, _conversions),
    SUM(_spent) AS _spent,
    SUM(_clicks) AS _clicks,
    SUM(_impressions) AS _impressions,
    SUM(_conversions) AS _conversions,
    'Google SEM' AS _platform
  FROM google_overview
  GROUP BY ALL
),
google_sem AS (
  SELECT 
    *
  FROM aggregated
)
SELECT 
  * 
FROM linkedin_combined
UNION ALL
SELECT 
  * 
FROM google_display_combined
UNION ALL
SELECT 
  * 
FROM google_sem;


TRUNCATE TABLE `x-marketing.corcentric.db_opportunity_influenced_sourced`;
INSERT INTO `x-marketing.corcentric.db_opportunity_influenced_sourced` (
  _emailcampaignID,
  _email_campaign_name,
  _timestamp,
  _device,
  _leadid,
  _country,
  _engagement,
  _2x_campaigns,
  stagename,
  opportunity_amount,
  amount_converted_text__c,
  opportunity_id,
  opportunity_name,
  type,
  createddate,
  closedate,
  opportunity_source,
  opp_type,
  opp_record_type,
  leadsource,
  contactid,
  firstname,
  lastname,
  title,
  email,
  ownerid,
  accountid,
  person_status,
  _last_qp_date,
  _last_discovery_date,
  _last_mql_date,
  _last_sal_date,
  _last_sql_date,
  _last_opportunity_date,
  is_primary,
  owner_name,
  account_record_type,
  region,
  from_stage,
  opp_count,
  average_amount,
  _opps_activity_status,
  marketing_influence_opps,
  marketing_influence_won_opps,
  marketing_influence_open_opps,
  marketing_influence_lost_opps,
  marketing_source_opps,
  marketing_source_won_opps,
  marketing_source_open_opps,
  marketing_sourced_lost_opps
)
WITH oppscontact AS (
  SELECT 
    contactid, 
    opportunityid,
    isprimary
  FROM `x-marketing.corcentric_salesforce.OpportunityContactRole`
  WHERE isdeleted IS FALSE
    AND isprimary IS TRUE
    --AND contactid = '003RQ00000BidSPYAZ'
), 
opps AS (
  SELECT 
    opp.stagename,
    CASE WHEN 
      currencyisocode = "USD" THEN opp.amount
      ELSE CAST(REGEXP_EXTRACT(amount_converted_text__c, r'\((?:USD|GBP|EUR|...) (\d+\.\d+)\)') AS FLOAT64)
    END AS opportunity_amount,
    amount_converted_text__c,
    opp.id AS opportunity_id,
    opp.name AS opportunity_name,
    opp.type,
    contactid,
    opp.createddate,
    opp.closedate,
    opportunity_source__c AS opportunity_source,
    type AS opp_type,
    r.name AS opp_record_type,
    leadsource
  FROM `x-marketing.corcentric_salesforce.Opportunity` opp
  LEFT JOIN `x-marketing.corcentric_salesforce.RecordType` r
    ON r.id = opp.recordtypeid
  --WHERE opp.id LIKE '006RQ000003EzEq%'
), 
contact AS (
  SELECT 
    id AS contactid,
    firstname,
    lastname,
    title,
    email,
    ownerid,
    accountid,
    status__c AS person_status,
    --region2__c AS region, --CHANGE TO ACCOUNT
    ds_03_last_qp__c AS _last_qp_date, 
    ds_04_last_discovery__c AS _last_discovery_date, 
    ds_05_last_mql__c AS _last_mql_date, 
    ds_06_last_sal__c AS _last_sal_date, 
    ds_07_last_sql__c AS _last_sql_date, 
    ds_08_last_opportunity__c AS _last_opportunity_date
  FROM `x-marketing.corcentric_salesforce.Contact` contact
  --WHERE email = 'amanda.rodriguez@cgiar.org'
), 
opp_base AS (
  SELECT 
    opps.* EXCEPT (contactid),
    contact.*,
    oppscontact.isprimary AS is_primary,
    user.name AS owner_name,
    acc.type AS account_record_type,
    acc.region__c AS region,
    LAG(side.stagename) OVER (PARTITION BY opps.opportunity_id,opps.createddate ORDER BY side.createddate) AS from_stage
  FROM oppscontact 
  JOIN  opps 
    ON opps.opportunity_id = oppscontact.opportunityid
  JOIN contact 
    ON contact.contactid = oppscontact.contactid
  LEFT JOIN `x-marketing.corcentric_salesforce.User` user 
    ON user.id = contact.ownerid
  LEFT JOIN `x-marketing.corcentric_salesforce.Account` acc 
    ON acc.id = contact.accountid
  LEFT JOIN `x-marketing.corcentric_salesforce.OpportunityHistory` side 
    ON side.opportunityid = opps.opportunity_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY opps.stagename, opps.opportunity_id, contact.contactid, opps.createddate 
    ORDER BY side.createddate DESC) = 1
), 
final_opp_base AS (
  SELECT 
    *,
    COUNT(opportunity_id) OVER (PARTITION BY opportunity_id) AS opp_count
  FROM opp_base
), 
final_base AS (
  SELECT 
    *, 
    CASE 
      WHEN opp_count > 0 THEN SAFE_DIVIDE(opportunity_amount, opp_count) 
      ELSE 0 
    END AS average_amount
  FROM final_opp_base
),
-- SELECT *
-- FROM final_base
-- WHERE opportunity_id like '006RQ00000DTyED%'
qp_list AS (
  SELECT 
    _emailcampaignID,
    _email_campaign_name,
    _timestamp,
    _device,
    _id AS _leadid,
    _email,
    _country,
    _engagement,
    _2x_campaigns
  FROM `x-marketing.corcentric.db_email_activity_log` qp
  WHERE _engagement LIKE 'QP%'
), 
consolidate_qp_opps AS (
  SELECT 
    qp_list.* EXCEPT(_email),
    final_base.*
  FROM qp_list
  RIGHT JOIN final_base
    ON final_base.email = qp_list._email
  WHERE opportunity_id IS NOT NULL
)
SELECT 
  c.*,
  IF(_timestamp IS NOT NULL, TRUE, FALSE) AS _opps_activity_status,
  IF(
      type = 'Inbound' 
      AND opportunity_source <> 'CS Generated' 
      AND opp_record_type = 'New - Direct', 
      TRUE, 
      FALSE
  ) AS marketing_influence_opps,
  IF(
      type = 'Inbound' 
      AND opportunity_source <> 'CS Generated' 
      AND opp_record_type = 'New - Direct' 
      AND stagename = 'Closed Won', 
      TRUE, 
      FALSE
  ) AS marketing_influence_won_opps,
  IF(
      type = 'Inbound' 
      AND opportunity_source <> 'CS Generated' 
      AND opp_record_type = 'New - Direct' 
      AND stagename NOT IN ('Closed Won', 'Closed Lost'), 
      TRUE, 
      FALSE
  ) AS marketing_influence_open_opps,
  IF(
      type = 'Inbound' 
      AND opportunity_source <> 'CS Generated' 
      AND opp_record_type = 'New - Direct' 
      AND stagename = 'Closed Lost', 
      TRUE, 
      FALSE
  ) AS marketing_influence_lost_opps,
  IF(
      type = 'Inbound' 
      AND opp_record_type = 'New - Direct' 
      AND owner_name NOT LIKE '%Cochran%' 
      AND leadsource NOT IN ('Partner', 'Referral', 'Sales Generated', ''), 
      TRUE, 
      FALSE
  ) AS marketing_source_opps,
  IF(
      type = 'Inbound' 
      AND opp_record_type = 'New - Direct' 
      AND opportunity_source IN ('Marketing Generated (BDR)', 'Marketing Generated (Non-BDR)', 'BDR Generated') 
      AND (leadsource NOT IN ('Partner', 'Sales Generated') OR leadsource IS NULL) 
      AND stagename = 'Closed Won', 
      TRUE, 
      FALSE
  ) AS marketing_source_won_opps,
  IF(
      type = 'Inbound' 
      AND opp_record_type = 'New - Direct' 
      AND leadsource NOT IN ('Partner', 'Referral', 'Sales Generated', '') 
      AND owner_name NOT LIKE '%Cochran%' 
      AND stagename NOT IN ('Closed Won', 'Closed Lost') 
      AND (opportunity_source NOT IN ('Buyers Guide', 'CS Generated') OR opportunity_source IS NULL), 
      TRUE, 
      FALSE
  ) AS marketing_source_open_opps,
  IF(
      type = 'Inbound' 
      AND opp_record_type = 'New - Direct' 
      AND stagename = 'Closed Lost' 
      AND leadsource NOT IN ('Partner', 'Referral', 'Sales Generated', '') 
      AND owner_name NOT LIKE '%Cochran%' 
      AND (opportunity_source NOT IN ('Buyers Guide', 'CS Generated') OR opportunity_source IS NULL), 
      TRUE, 
      FALSE
  ) AS marketing_sourced_lost_opps
FROM consolidate_qp_opps AS c
WHERE region IN ('Southern Europe','North America','Northern Europe');
  --and opportunity_id LIKE '006RQ000003EzEq%'