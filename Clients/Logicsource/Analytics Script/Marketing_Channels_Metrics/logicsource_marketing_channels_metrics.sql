/* 
This script runs for the Success Metric scorecards in the intent-drive marketing dahsboard (overview page)
*/

TRUNCATE TABLE `logicsource.dashboard_marketing_channels_metrics`;
INSERT INTO `logicsource.dashboard_marketing_channels_metrics` (
  _campaignID,
  _campaign,
  _sent_timestamp,
  _channel,
  _2x_campaign
)
WITH activity AS (
  SELECT
      activity.emailcampaignid AS _campaignID,
      campaign.name AS _campaign,
      MIN(activity.created) AS _sent_timestamp
    FROM
      `x-marketing.logicsource_hubspot.email_events` activity
    JOIN
      `x-marketing.logicsource_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
     JOIN `x-marketing.logicsource_mysql.db_airtable_email` email ON CAST(campaign.id AS STRING) =   _pardotid
    WHERE
      activity.type = 'SENT'
      AND campaign.name IS NOT NULL 
    GROUP BY
      1, 2
),
campaigns AS (
  SELECT
    *,
    "Email" AS _channel,
    "2X" _2x_campaign
  FROM activity
),
ads AS (
  SELECT
    DISTINCT creative_id, 
    creative, 
    start_at, 
    "Ads",
    CAST(NULL AS STRING)
  FROM
    `x-marketing.logicsource_linkedin_ads.ad_analytics_by_creative`
    
) ,
contents AS (
  SELECT 
    _id AS _contentid,
    _title AS _contenttitle,
   SAFE_CAST(_created AS  TIMESTAMP) AS _created_timestamp,
    _type AS _channel,
    "2X"
  FROM 
    `logicsource_mysql.content_wise_mapping`
),
combine_all AS (
  SELECT * FROM campaigns UNION ALL
 SELECT * FROM ads UNION ALL
  SELECT * FROM contents
)
SELECT * FROM combine_all
WHERE
    EXTRACT(YEAR FROM _sent_timestamp) IN (2022, 2023)
;
