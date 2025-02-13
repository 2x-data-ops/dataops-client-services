-- CREATE OR REPLACE TABLE `x-marketing.corcentric.ad_metrics` AS
TRUNCATE TABLE `x-marketing.corcentric.ad_metrics`;

INSERT INTO `x-marketing.corcentric.ad_metrics` (
  _status,
  _advariation,
  _content,
  _screenshot,
  _reportinggroup,
  _source,
  _medium,
  _id,
  _adtype,
  _platform,
  _asset,
  _landingpageurl,
  _campaignname,
  _stage,
  ad_id,
  campaign_id,
  day,
  spent,
  impressions,
  clicks,
  _platform_type,
  pageviews,
  reduced_pageviews,
  visitors,
  reduced_visitors
)
WITH airtable AS (
  SELECT
    * EXCEPT (
      _sdc_batched_at,
      _sdc_received_at,
      _sdc_sequence,
      _sdc_table_version
    ),
    -- Stage is set over here
    'Awareness' AS _stage,
  FROM `x-marketing.Demo.db_airtable_ads`
  WHERE _platform != ''
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid ORDER BY _sdc_received_at DESC) = 1
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:') + 18)) [ORDINAL(1)] AS cID,
    campaign_id,
    c.account_id,
    intended_status,
    name AS _advariation,
    content_reference AS _content
  FROM `x-marketing.corcentric_linkedin_ads.creatives` c
  LEFT JOIN `x-marketing.corcentric_linkedin_ads.video_ads` v
    ON content.reference = v.content_reference
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignName,
    status,
    cost_type,
    total_budget.amount AS total_budget,
    campaign_group_id,
    run_schedule.end,
    run_schedule.start,
    TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) AS date_diffs,
    type
  FROM `x-marketing.corcentric_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM `x-marketing.corcentric_linkedin_ads.campaign_groups`
),
linkedin AS (
  SELECT
    CAST(campaignID AS STRING) AS _campaign,
    cID AS _adid,
    NULL AS ad_group_id,
    campaigns.status AS _status,
    ads_title._advariation AS _advariation,
    ads_title._content AS _content,
    _screenshot AS _screenshot,
    _groupName AS _reportinggroup,
    'Linkedin' AS _source,
    _medium AS _medium,
    "" AS _id,
    type AS _adtype,
    'Linkedin' AS _platform,
    "" AS _asset,
    _landingpageurl AS _landingpageurl,
    campaigns._campaignName AS _campaignname,
    "Awareness" AS _stage
  FROM ads_title
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
  LEFT JOIN airtable
    ON ads_title.cID = CAST(airtable._adid AS STRING)
),
google AS (
  SELECT DISTINCT
    CAST(campaign_id AS STRING) AS _campaign,
    '' AS _adid,
    adgroup.id AS ad_group_id,
    campaign.status AS _status,
    '' AS _advariation,
    "" AS _content,
    "" AS _screenshot,
    adgroup.name AS _reportinggroup,
    '' AS _source,
    "" AS _medium,
    "" AS _id,
    '' AS _adtype,
    'Google' AS _platform,
    '' AS _asset,
    '' AS _landingpageurl,
    campaign.name AS _campaignname,
    "Awareness" AS _stage
  FROM `x-marketing.corcentric_google_ads.ad_groups` adgroup
  LEFT JOIN `x-marketing.corcentric_google_ads.campaigns` campaign
    ON campaign.id = adgroup.campaign_id
),
airtable_ads AS (
  SELECT
    *
  FROM linkedin
  UNION ALL
  SELECT
    *
  FROM google
),
linkedin_ads AS (
  SELECT
    main.creative_id AS ad_id,
    creative.campaign_id,
    campaign_group.id AS ad_group_id,
    main.start_at AS day,
    CAST(main.cost_in_usd AS FLOAT64) AS spent,
    main.impressions AS impressions,
    main.clicks,
    'LinkedIn' AS _platform_type,
  FROM `x-marketing.corcentric_linkedin_ads.ad_analytics_by_creative` main
  JOIN `x-marketing.corcentric_linkedin_ads.creatives` creative
    ON CAST(main.creative_id AS STRING) = REGEXP_EXTRACT(creative.id, r'\d+')
  JOIN `x-marketing.corcentric_linkedin_ads.campaigns` campaign
    ON creative.campaign_id = campaign.id
  JOIN `x-marketing.corcentric_linkedin_ads.campaign_groups` campaign_group
    ON campaign.campaign_group_id = campaign_group.id
  ORDER BY main.start_at DESC
),
google_ads AS (
  SELECT
    NULL AS ad_id,
    campaign.campaign_id,
    ad_group_id AS ad_group_id,
    campaign.date AS day,
    campaign.cost_micros / 1000000 AS spent,
    campaign.impressions AS impressions,
    campaign.clicks,
    'Google' AS _platform_type
  FROM `x-marketing.corcentric_google_ads.ad_group_performance_report` campaign
),
ads_metrics AS (
  SELECT
    *
  FROM linkedin_ads
  UNION ALL
  SELECT
    *
  FROM google_ads
),
airtable_linkedin_ads AS (
  SELECT
    * EXCEPT (_adid, _campaign, ad_group_id)
  FROM airtable_ads
  JOIN ads_metrics 
    ON CAST(ads_metrics.ad_id AS STRING) = airtable_ads._adid
  WHERE _platform = 'Linkedin'
),
airtable_google_ads AS (
  SELECT
    * EXCEPT (_adid, _campaign, ad_group_id)
  FROM airtable_ads
  JOIN ads_metrics
    ON CAST(ads_metrics.campaign_id AS STRING) = airtable_ads._campaign
    AND airtable_ads.ad_group_id = ads_metrics.ad_group_id
  WHERE _platform = 'Google'
),
all_ads AS (
  SELECT
    *
  FROM airtable_linkedin_ads
  UNION ALL
  SELECT
    *
  FROM airtable_google_ads
),
web_engagements AS (
  SELECT DISTINCT
    CAST(_timestamp AS DATE) AS _date,
    _visitorid,
    _fullurl AS _fullpage,
    _totalsessionviews,
    _utmsource
  FROM `x-marketing.corcentric.db_web_engagements_log`
),
ad_counts AS (
  SELECT DISTINCT
    DAY,
    _source,
    _landingpageurl,
    -- Count the number of ads sharing the same URL
    COUNT(DISTINCT ad_id) AS ad_count
  FROM all_ads
  GROUP BY 1, 2, 3
  ORDER BY 4 DESC
),
get_web_page_views AS (
  SELECT
    ad.day,
    ad._landingpageurl,
    ad.ad_count,
    ad._source,
    COUNT(DISTINCT web._visitorid) AS visitors,
    SUM(web._totalsessionviews) AS pageviews
  FROM web_engagements AS web
  JOIN ad_counts AS ad
    ON ad._landingpageurl LIKE CONCAT('%', web._fullpage, '%')
    AND EXTRACT(DATETIME FROM ad.day) = web._date
  WHERE UPPER(ad._source) = UPPER(web._utmsource)
    AND web._utmsource IN ('linkedin', 'LinkedIn', 'Google')
  GROUP BY 1, 2, 3, 4
)
/*  
Ads data and web visits data are tied using the activity day and the URL.
This means that there would be a duplication in numbers if several ads share the same URL on that day.
This duplication of numbers can be handled by dividing the web metric number by the number of ads with the same URL.
  */
SELECT
  main.*,
  side.pageviews,
  side.pageviews / side.ad_count AS reduced_pageviews,
  side.visitors,
  side.visitors / side.ad_count AS reduced_visitors
FROM all_ads AS main
LEFT JOIN get_web_page_views AS side
  ON main.day = side.day
  AND main._landingpageurl = side._landingpageurl
  AND main._source = side._source;