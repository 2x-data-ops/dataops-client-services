--CREATE OR REPLACE TABLE `x-marketing.logicsource.email_performance_agg` AS
TRUNCATE TABLE `x-marketing.logicsource.email_performance_agg`;

INSERT INTO `x-marketing.logicsource.email_performance_agg` (
  _campaignID,
  _utm_campaign,	
  contentid,
  _subject,	
  _emailsegment,	
  _campaignsegment,	
  type,	
  subtype,	
  sent,
  suppressed,
  statuschange,
  delivered,
  mta_dropped,
  click,
  processed,
  bounce,
  dropped,
  open,
  unsubscribed,
  deferred,
  _timestamp	
)
WITH send_Date AS (
  SELECT DISTINCT activity.emailcampaignid,
      activity.created AS _timestamp,
      
      FROM `x-marketing.logicsource_hubspot.email_events` activity
      WHERE activity.type = 'SENT' AND recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND recipient NOT LIKE '%2x.marketing' AND recipient NOT LIKE '%logicsource%' AND recipient NOT LIKE '%medifastinc.com' AND recipient NOT LIKE '%@ckr.com%' AND recipient NOT LIKE '%@ircinc.com%' AND recipient NOT LIKE '%finnpartners.com%' AND recipient NOT LIKE '%oceanstatejoblot.com%' AND recipient NOT LIKE '%@osjl.com%'
      QUALIFY ROW_NUMBER() OVER( PARTITION BY activity.emailcampaignid ORDER BY activity.created DESC) = 1
)SELECT  
id AS _campaignID, name AS _utm_campaign ,contentid, _subject, _email AS _emailsegment, _campaign AS _campaignsegment, type, subtype,counters.sent, counters.suppressed, counters.statuschange, counters.delivered, counters.mta_dropped, counters.click, counters.processed, counters.bounce, counters.dropped, counters.open, counters.unsubscribed, counters.deferred,_timestamp
FROM `x-marketing.logicsource_hubspot.campaigns` campaign
LEFT JOIN send_Date ON campaign.id = send_Date.emailcampaignid
JOIN `x-marketing.logicsource_mysql.db_airtable_email` airtable ON CAST(campaign.contentid AS STRING) = _emailid
ORDER BY _timestamp DESC