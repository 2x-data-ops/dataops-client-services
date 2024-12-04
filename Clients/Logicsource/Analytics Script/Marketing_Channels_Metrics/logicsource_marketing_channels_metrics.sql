/* 
This script runs for the Success Metric scorecards in the intent-drive marketing dahsboard (overview page)
*/

TRUNCATE TABLE `logicsource.dashboard_marketing_channels_metrics`;
INSERT INTO `logicsource.dashboard_marketing_channels_metrics`
WITH campaigns AS (
  SELECT
    *,
    "Email" AS _channel,
    "2X" _2x_campaign
  FROM (
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
  )
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
)
SELECT * FROM (
  SELECT * FROM campaigns UNION ALL
 SELECT * FROM ads UNION ALL
  SELECT * FROM contents
)
WHERE
    EXTRACT(YEAR FROM _sent_timestamp) IN (2022, 2023)
;
