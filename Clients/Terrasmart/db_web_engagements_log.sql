TRUNCATE TABLE `x-marketing.terrasmart.db_web_engagements_log` ; 
INSERT INTO  `x-marketing.terrasmart.db_web_engagements_log` 
SELECT * EXCEPT(_order)
FROM (
 SELECT	
'' AS _recordingurl,
'' AS _recordingid,
'' AS _visitorid,
user_pseudo_id,
 _userstatus,
page_location,
page AS _page,
MAX(page_type)AS _pagegroup,
page_location AS _fullurl,
'' AS _cleanpage,
MAX(pagepath_level_1) AS _nextpage,
MAX(pagepath_level_2) AS _nextpage_2,
MAX(pagepath_level_3) AS _nextpage_3, 
--COUNT(DISTINCT(user_pseudo_id)) AS sessions,
 --* EXCEPT (engagement_time_seconds),
 SUM(engagement_time_seconds) AS _engagementtime,
 '' AS _timespent,
 _timestamp,
 "Awareness" AS _stage,
 _source ,
 _medium,
 campaign_name AS _utmcampaign,
 _page_description,
 page_title AS _page_title,
 demandbase_website_domain AS _domain,
 demandbase_hq_country AS _country,
 demandbase_company_name AS _name,
 demandbase_industry AS _industry,
 session_engaged AS session_engaged,
 session_ids,
 COUNT( DISTINCT session_ids) AS _uniquesessionviews,
 ROW_NUMBER() OVER(PARTITION BY user_pseudo_id, page_location, EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
 
  
 FROM (
  SELECT 
  traffic_source.name AS campaign_name,
  PARSE_TIMESTAMP('%Y%m%d',event_date) AS _timestamp,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_title') AS page_title,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
  split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(3)],'?')[safe_ordinal(1)] AS page,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS _source,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_description') AS _page_description,
  CASE
  WHEN event_name = 'session_start' then CONCAT (user_pseudo_id, CAST(event_timestamp as string)
  )END AS session_id,
  user_pseudo_id,
  (SELECT CASE WHEN ga_session_number > 1 THEN "Returning" ELSE "New" END as _userstatus 
  FROM
  ( SELECT value.int_value AS ga_session_number 
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_number'
  ))  AS _userstatus,
  CASE WHEN split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
  CASE WHEN split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
  CASE WHEN split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
  COUNTIF(event_name = 'page_view') as page_views,
  (select value.string_value from unnest(event_params) where key = 'medium') as _medium,
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_ids,
  max((select value.string_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
  sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engagement_time_seconds,
  MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demandbase_website_domain' ) )AS demandbase_website_domain,
  MAX(( SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demandbase_company_name'))AS demandbase_company_name,
  MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demandbase_hq_country')) AS demandbase_hq_country,
  MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demandbase_industry')) AS demandbase_industry,
  FROM  `x-marketing.analytics_264099206.events_*` AS events
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15, pagepath_level_1,
    pagepath_level_2,
    pagepath_level_3,17,18
 ) WHERE engagement_time_seconds IS NOT NULL 
 GROUP BY 1,2,3,4,5,6,7,9,10,15,16,17,18,19,20,21,22,23,24,25,26,27,28
) WHERE _order = 1