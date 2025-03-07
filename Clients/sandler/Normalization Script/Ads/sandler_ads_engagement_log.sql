-- CREATE OR REPLACE TABLE `x-marketing.sandler.ads_engagement_log` AS
WITH airtable_ads AS (
 
  WITH
    airtable AS (
      --selecting only few fields for airtable
      SELECT
        _ad_id AS _adid,
        _ad_visual AS _screenshot,
        _platform,
        _landing_page_url AS _websiteurl,
        "Awareness" AS _stage
      FROM `x-marketing.sandler_google_sheets.db_ads_optimization`
      WHERE _platform = 'LinkedIn'
        AND _instance = 'Sandler Network'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid) = 1
    ),
    ads_title AS(
      SELECT
        SPLIT(SUBSTR(creative.id, STRPOS(creative.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS cID,
        creative.campaign_id,
        creative.account_id,
        creative.intended_status,
        video.name AS _advariation,
        video.content_reference AS _content
      FROM
        `x-marketing.sandler_linkedin_ads.creatives` creative
      LEFT JOIN
        `x-marketing.sandler_linkedin_ads.video_ads` video
      ON
        creative.content.reference = video.content_reference
    ),
    campaigns AS (
      SELECT
        id AS campaignID,
        name AS _campaignName,
        status,
        cost_type,
        total_budget.amount AS total_budget,
        campaign_group_id,
        TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) AS date_diffs,
        type
      FROM
        `x-marketing.sandler_linkedin_ads.campaigns`
    ),
    campaign_group AS (
      SELECT
        id AS groupID,
        name AS _groupName,
        status
      FROM `x-marketing.sandler_linkedin_ads.campaign_groups`
    ),
    --COMBINE ALL ATTRIBUTES FOR LINKEDIN--
    linkedin AS (
      SELECT
        CAST(campaignID AS STRING) AS _campaign,
        ads_title.cID AS _adid,
        campaigns.campaign_group_id AS ad_group_id,
        campaigns.status,
        ads_title._advariation,
        ads_title._content,
        airtable._screenshot AS _screenshot,
        campaign_group._groupName AS _reportinggroup,
        'LinkedIn' AS _source,
        airtable._platform  AS _medium,
        "" AS _id,
        campaigns.type AS _adtype,
        'LinkedIn' AS _platform,
        "" AS _asset,
        airtable._websiteurl AS _landingpageurl,
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
    --COMBINE ALL ATTRIBUTES FOR GOOGLE--
    google AS (
      SELECT
        CAST(adgroup.campaign_id AS STRING) AS _campaign,
        "" AS _adid,
        adgroup.id AS ad_group_id,
        campaign.status AS _status,
        "" AS _advariation,
        "" AS _content,
        "" AS _screenshot,
        adgroup.name AS _reportinggroup,
        "" AS _source,
        "" AS _medium,
        "" AS _id,
        "" AS _adtype,
        "Google" AS _platform,
        "" AS _asset,
        "" AS _landingpageurl,
        campaign.name AS _campaignname,
        "Awareness" AS _stage   
      FROM
        `x-marketing.sandler_google_ads.ad_groups` adgroup
      LEFT JOIN
        `x-marketing.sandler_google_ads.campaigns` campaign
      ON adgroup.campaign_id = campaign.id
    ), _6sense AS (
        SELECT
          _campaign_id AS _campaign,
          _ad_id AS _adid,
          0 AS ad_group_id,
          "" AS _status,
          _ad_name AS _advariation,
          "" AS _content,
          "" AS _screenshot,
          "" AS _reportinggroup,
          "" AS _source,
          "" AS _medium,
          "" AS _id,
          "" AS _adtype,
          _platform,
          "" AS _asset,
          "" AS _landingpageurl,
          _campaign_name AS _campaignname,
          "Awareness" AS _stage
        FROM `x-marketing.sandler_google_sheets.db_ads_optimization`
        WHERE _platform = '6sense'
          AND _instance = 'Sandler Enterprise'
          AND LENGTH(_ad_id) > 2
          AND _campaign_id IS NOT NULL
    )
    SELECT *
    FROM linkedin
    UNION ALL
    SELECT * FROM google
    UNION ALL
    SELECT * FROM _6sense
),
--METRICS COLUMNS--
ads_metrics AS (
  --METRICS COLUMNS FOR LINKEDIN--
  WITH linkedin_ads AS (
    SELECT
      main.creative_id AS ad_id,
      creative.campaign_id,
      campaign_group.id AS ad_group_id,
      main.start_at AS day,
      CAST(main.cost_in_usd AS FLOAT64) AS spent,
      main.impressions,
      main.clicks,
      "LinkedIn" AS _platform_type
    FROM
      `x-marketing.sandler_linkedin_ads.ad_analytics_by_creative` main
    JOIN
      `x-marketing.sandler_linkedin_ads.creatives` creative
      ON CAST(main.creative_id AS STRING) = SPLIT(SUBSTR(creative.id, STRPOS(creative.id, 'sponsoredCreative:')+18))[ORDINAL(1)]
    JOIN
      `x-marketing.sandler_linkedin_ads.campaigns` campaign
        ON creative.campaign_id = campaign.id
    JOIN
      `x-marketing.sandler_linkedin_ads.campaign_groups` campaign_group
      ON campaign.campaign_group_id = campaign_group.id
    ORDER BY
      main.start_at DESC
  ),
    --METRICS COLUMNS FOR GOOGLE--
  google_ads AS (
    SELECT
      NULL AS ad_id,
      campaign.campaign_id,
      ad_group_id AS ad_group_id,
      campaign.date AS day,
      campaign.cost_micros/1000000 AS spent,
      campaign.impressions AS impressions, 
      campaign.clicks,
      "Google" AS _platform_type,
    FROM
      `x-marketing.sandler_google_ads.ad_group_performance_report` campaign
  ) , _6sense AS (
    SELECT
    CAST(_adid AS INT64) AS ad_id,
    CAST(campaignID AS INT64) AS _campaignid,
    NULL AS ad_group_id, 
    CAST(_date AS TIMESTAMP) AS _date,
    SUM(spend) AS _spend, 
   SUM(impressions) AS _impressions, 
   SUM(clicks) AS _clicks, 
    "6sense"
FROM 
    `x-marketing.sandler.db_6sense_ads_overview`
    WHERE 
    _date IS NOT NULL
    GROUP BY 1,2,3,4
  )
SELECT *
FROM (
  SELECT * FROM linkedin_ads
  UNION ALL
  SELECT * FROM google_ads
  UNION ALL
  SELECT * FROM _6sense
)
),
--COMBINE MAIN ADS WITH METRICS--
all_ads AS (
  WITH linkedin_ads AS (
    SELECT * EXCEPT (_adid, _campaign, ad_group_id)
    FROM airtable_ads
    JOIN ads_metrics
      ON CAST(ads_metrics.ad_id AS STRING) = airtable_ads._adid
    WHERE _platform = "LinkedIn"
  ),
  google_ads AS (
  SELECT * EXCEPT(_adid,_campaign,ad_group_id)
    FROM airtable_ads
    JOIN ads_metrics
      ON CAST(ads_metrics.campaign_id AS STRING) = airtable_ads._campaign
      AND airtable_ads.ad_group_id = ads_metrics.ad_group_id
    WHERE _platform = "Google"
  ),
  _6sense AS (
  SELECT * EXCEPT(_adid,_campaign,ad_group_id)
    FROM airtable_ads
    JOIN ads_metrics
      ON CAST(ads_metrics.ad_id AS STRING) = airtable_ads._adid
    WHERE _platform =  "6sense"
  )
  SELECT *
    FROM (
      SELECT * FROM linkedin_ads
      UNION ALL
      SELECT * FROM google_ads
      UNION ALL
      SELECT * FROM _6sense
    )
),
--ADDITIONAL ATRRIBUTES FROM WEB ENGAGEMENT--
get_web_page_views AS (
  SELECT
    ad.day,
    ad._landingpageurl,
    ad.ad_count,
    ad._source,
    COUNT(DISTINCT web._visitorid) AS visitors,
    SUM(web._totalsessionviews) AS pageviews
    FROM (
      SELECT DISTINCT
          CAST(_timestamp AS DATE) AS _date,
          _visitorid,
          _fullurl AS _fullpage,
          _totalsessionviews,
          _utmsource
      FROM `x-marketing.sandler.db_web_engagements_log`
    ) web
    JOIN (
        SELECT DISTINCT
            day,
            _source,
            _landingpageurl,
            COUNT(DISTINCT ad_id) AS ad_count
        FROM all_ads
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC
    ) ad 
    ON ad._landingpageurl LIKE CONCAT('%', web._fullpage, '%')
    AND EXTRACT(DATETIME FROM ad.day) = web._date
    WHERE UPPER(ad._source) = UPPER(web._utmsource) 
    AND web._utmsource IN('linkedin', 'LinkedIn', 'Google')
    GROUP BY 1, 2, 3, 4
)
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
    AND main._source = side._source



