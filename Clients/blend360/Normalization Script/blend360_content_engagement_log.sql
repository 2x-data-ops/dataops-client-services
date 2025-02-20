-- Content engagement script
TRUNCATE TABLE `x-marketing.blend360.db_content_engagements_log`;
INSERT INTO `x-marketing.blend360.db_content_engagements_log`
WITH content AS (
SELECT DISTINCT 
  *
FROM `x-marketing.blend360_mysql.content_wise_mapping`
WHERE _url != ''
),
web AS (
SELECT DISTINCT
  _visitorid,
  _domain,
  _timestamp,
  _fullurl
FROM `x-marketing.blend360.db_web_engagements_log`
)
SELECT DISTINCT 
  content.* EXCEPT (_id,_sdc_batched_at, _sdc_received_at,_sdc_sequence, _sdc_table_version),
  content._id AS _contentID,
  web._timestamp AS _visitDate,
  1 AS _pageviews,
  web._visitorid AS _visitorID,
  web._domain
FROM content
LEFT JOIN web 
  ON content._url = web._fullurl;
-- Extracting hot intent topics based on the content wise mapping
TRUNCATE TABLE `blend360.report_content_topic_metrics`;
INSERT INTO `blend360.report_content_topic_metrics`
SELECT DISTINCT 
  _domain, 
  LOWER(keywords) AS _keywords, 
  _visitDate, 
  _contentID, 
  _url, 
  _pageviews
FROM `blend360.db_content_engagements_log` content,
  UNNEST(SPLIT(_intentkeyword, ', ')) AS keywords
WHERE _intentkeyword IS NOT NULL 
  AND _intentkeyword !='' 
  AND _visitdate IS NOT NULL
ORDER BY 1;