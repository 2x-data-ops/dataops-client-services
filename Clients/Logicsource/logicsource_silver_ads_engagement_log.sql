TRUNCATE TABLE `x-marketing.logicsource.report_consolidated_ads_metrics`;

INSERT INTO `x-marketing.logicsource.report_consolidated_ads_metrics` (
    _adid,
    _adgroup_id,
    _day,
    _spent,
    _impressions,
    _clicks,
    _advariation,
    _content,
    _screenshot,
    _reportinggroup,
    _campaign,
    _source,
    _medium,
    _platform,
    _asset,
    _landingpageurl,
    _campaignname,
    _stage,
    _adnum,
    _pageviews,
    _visitors
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
      'Awareness' AS _stage
    FROM `x-marketing.logicsource_mysql.db_airtable_ads`
    WHERE _platform != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid ORDER BY _sdc_received_at DESC) = 1
  ),
  ads_title AS (
    SELECT
      SPLIT(
        SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:') + 18)
      ) [ORDINAL(1)] AS cID,
      campaign_id,
      c.account_id,
      intended_status,
      name AS _advariation,
      content_reference AS _content
    FROM `x-marketing.logicsource_linkedin_ads.creatives` c
    LEFT JOIN `x-marketing.logicsource_linkedin_ads.video_ads` v
      ON c.content.reference = v.content_reference
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
    FROM `x-marketing.logicsource_linkedin_ads.campaigns`
  ),
  campaign_group AS (
    SELECT
      id AS groupID,
      name AS _groupName,
      status
    FROM `x-marketing.logicsource_linkedin_ads.campaign_groups`
  ),
  linkedin AS (
    SELECT
      cID AS _adid,
      campaigns.status AS _status,
      ads_title._advariation AS _advariation,
      ads_title._content AS _content,
      _screenshot AS _screenshot,
      _groupName AS _reportinggroup,
      campaignID AS _campaign,
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
  airtable_ads AS (
    SELECT
      *
    FROM linkedin
  ),
  /* 
  Linkedin ads are tied with their statistics using the ad id itself, so no duplicates
   */
  linkedin_ads AS (
    SELECT
      main.creative_id AS ad_id,
      campaign_group.id AS ad_group_id,
      main.start_at AS day,
      CAST(main.cost_in_usd AS FLOAT64) AS spent,
      main.impressions AS impressions,
      main.clicks
    FROM `x-marketing.logicsource_linkedin_ads.ad_analytics_by_creative` main
    JOIN `x-marketing.logicsource_linkedin_ads.creatives` creative
      ON CAST(main.creative_id AS STRING) = SPLIT(
        SUBSTR(
          creative.id,
          STRPOS(creative.id, 'sponsoredCreative:') + 18
        )
      ) [ORDINAL(1)]
    JOIN `x-marketing.logicsource_linkedin_ads.campaigns` campaign
      ON creative.campaign_id = campaign.id
    JOIN `x-marketing.logicsource_linkedin_ads.campaign_groups` campaign_group
      ON campaign.campaign_group_id = campaign_group.id
    ORDER BY main.start_at DESC
  ),
  combined_data AS (
    SELECT
      ads.*,
      airtable.* EXCEPT (_id, _adid, _adtype)
    FROM linkedin_ads AS ads
    LEFT JOIN airtable_ads AS airtable
      ON CAST(ads.ad_id AS STRING) = airtable._adid
  ),
  count_ads AS (
    SELECT
      *,
      COUNT(ad_id) OVER (PARTITION BY day, ad_group_id) AS adnum
    FROM combined_data
  ),
  reduced_numbers_google_ads AS (
    SELECT
      *,
      -- For Google ads, divide by number of ads in ad group to reduce duplicated numbers
      -- CASE 
      --     WHEN _platform LIKE '%Google%' THEN spent / adnum
      --     WHEN _platform LIKE '%LinkedIn%' THEN spent
      -- END AS reduced_spent,
      -- CASE 
      --     WHEN _platform LIKE '%Google%' THEN impressions / adnum
      --     WHEN _platform LIKE '%LinkedIn%' THEN impressions
      -- END AS reduced_impressions,
      -- CASE 
      --     WHEN _platform LIKE '%Google%' THEN clicks / adnum
      --     WHEN _platform LIKE '%LinkedIn%' THEN clicks
      -- END AS reduced_clicks
    FROM count_ads
  ),
  web_engagement AS (
    SELECT DISTINCT
      CAST(_timestamp AS DATE) AS _date,
      _visitorid,
      _fullurl AS _fullpage,
      _totalsessionviews,
      _utmsource
    FROM `x-marketing.logicsource.db_web_engagements_log`
  ),
  reduced_numbers_google_ads_agg AS (
    SELECT DISTINCT
      day,
      _source,
      _landingpageurl,
      -- Count the number of ads sharing the same URL
      COUNT(DISTINCT ad_id) AS ad_count
    FROM reduced_numbers_google_ads
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
    FROM web_engagement AS web
    JOIN reduced_numbers_google_ads_agg AS ad
      ON ad._landingpageurl LIKE CONCAT('%', web._fullpage, '%')
      AND EXTRACT(DATETIME FROM ad.day) = web._date
    WHERE UPPER(ad._source) = UPPER(web._utmsource)
      AND web._utmsource IN ('linkedin', 'LinkedIn', 'Google')
    GROUP BY 1, 2, 3, 4
  ),
  /*  
  Ads data and web visits data are tied using the activity day and the URL.
  This means that there would be a duplication in numbers if several ads share the same URL on that day.
  This duplication of numbers can be handled by dividing the web metric number by the number of ads with the same URL.
   */
  add_reduced_web_page_views AS (
    SELECT
      main.* EXCEPT (_status),
      side.pageviews,
      -- side.pageviews / side.ad_count AS reduced_pageviews,
      side.visitors,
      -- side.visitors / side.ad_count AS reduced_visitors
    FROM reduced_numbers_google_ads AS main
    LEFT JOIN get_web_page_views AS side
      ON main.day = side.day
      AND main._landingpageurl = side._landingpageurl
      AND main._source = side._source
  )
SELECT
  *
FROM add_reduced_web_page_views;


--CREATE OR REPLACE TABLE `x-marketing.logicsource.ad_metrics` AS
TRUNCATE TABLE `x-marketing.logicsource.ad_metrics`;

INSERT INTO `x-marketing.logicsource.ad_metrics` (
    ad_id,
    ad_group_id,
    day,
    spent,
    impressions,
    clicks,
    _status,
    _advariation,
    _content,
    _screenshot,
    _reportinggroup,
    _campaign,
    _source,
    _medium,
    _platform,
    _asset,
    _landingpageurl,
    _campaignname,
    _stage,
    adnum,
    reduced_spent,
    reduced_impressions,
    reduced_clicks,
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
      'Awareness' AS _stage
    FROM `x-marketing.logicsource_mysql.db_airtable_ads`
    WHERE _platform != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid ORDER BY _sdc_received_at DESC) = 1
  ),
  ads_title AS (
    SELECT
      SPLIT(
        SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:') + 18)
      ) [ORDINAL(1)] AS cID,
      campaign_id,
      c.account_id,
      intended_status,
      name AS _advariation,
      content_reference AS _content
    FROM `x-marketing.logicsource_linkedin_ads.creatives` c
    LEFT JOIN x-marketing.logicsource_linkedin_ads.video_ads v
      ON c.content.reference = v.content_reference
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
    FROM `x-marketing.logicsource_linkedin_ads.campaigns`
  ),
  campaign_group AS (
    SELECT
      id AS groupID,
      name AS _groupName,
      status
    FROM `x-marketing.logicsource_linkedin_ads.campaign_groups`
  ),
  linkedin AS (
    SELECT
      cID AS _adid,
      campaigns.status AS _status,
      ads_title._advariation AS _advariation,
      ads_title._content AS _content,
      _screenshot AS _screenshot,
      _groupName AS _reportinggroup,
      CAST(campaignID AS STRING) AS _campaign,
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
  airtable_ads AS (
    SELECT
      *
    FROM linkedin
  ),
  /* 
  Linkedin ads are tied with their statistics using the ad id itself, so no duplicates
   */
  linkedin_ads AS (
    SELECT
      main.creative_id AS ad_id,
      campaign_group.id AS ad_group_id,
      main.start_at AS day,
      CAST(main.cost_in_usd AS FLOAT64) AS spent,
      main.impressions AS impressions,
      main.clicks
    FROM `x-marketing.logicsource_linkedin_ads.ad_analytics_by_creative` main
    JOIN `x-marketing.logicsource_linkedin_ads.creatives` creative 
      ON CAST(main.creative_id AS STRING) = SPLIT(
        SUBSTR(
          creative.id,
          STRPOS(creative.id, 'sponsoredCreative:') + 18
        )
      ) [ORDINAL(1)]
    JOIN `x-marketing.logicsource_linkedin_ads.campaigns` campaign
      ON creative.campaign_id = campaign.id
    JOIN `x-marketing.logicsource_linkedin_ads.campaign_groups` campaign_group 
      ON campaign.campaign_group_id = campaign_group.id
    ORDER BY main.start_at DESC
  ),
  /*  
  Google ads are tied with their statistics using the ad group id instead of the ad id.
  This means that the statistics for each ad is the statistic of the ad group and not the individual ad itself.
  This duplication of numbers can be handled by dividing the statistics by the number of ads in the ad group.
   */
  combined_data AS (
    SELECT
      ads.*,
      airtable.* EXCEPT (_id, _adid, _adtype)
    FROM linkedin_ads AS ads
    LEFT JOIN airtable_ads AS airtable
      ON CAST(ads.ad_id AS STRING) = airtable._adid
  ),
  count_ads AS (
    SELECT
      *,
      COUNT(ad_id) OVER (PARTITION BY day, ad_group_id ) adnum
    FROM combined_data
  ),
  reduced_numbers_google_ads AS (
    SELECT
      *,
      -- For Google ads, divide by number of ads in ad group to reduce duplicated numbers
      CASE
        WHEN _platform LIKE '%Google%' THEN spent / adnum
        WHEN _platform LIKE '%LinkedIn%' THEN spent
      END AS reduced_spent,
      CASE
        WHEN _platform LIKE '%Google%' THEN impressions / adnum
        WHEN _platform LIKE '%LinkedIn%' THEN impressions
      END AS reduced_impressions,
      CASE
        WHEN _platform LIKE '%Google%' THEN clicks / adnum
        WHEN _platform LIKE '%LinkedIn%' THEN clicks
      END AS reduced_clicks
    FROM count_ads
  ),
  web_engagements AS (
    SELECT DISTINCT
      CAST(_timestamp AS DATE) AS _date,
      _visitorid,
      _fullurl AS _fullpage,
      _totalsessionviews,
      _utmsource
    FROM `x-marketing.logicsource.db_web_engagements_log`
  ),
  reduced_numbers_google_ads_agg AS (
    SELECT DISTINCT
      day,
      _source,
      _landingpageurl,
      -- Count the number of ads sharing the same URL
      COUNT(DISTINCT ad_id) AS ad_count
    FROM reduced_numbers_google_ads
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
    JOIN reduced_numbers_google_ads_agg AS ad
      ON ad._landingpageurl LIKE CONCAT('%', web._fullpage, '%')
      AND EXTRACT(
        DATETIME
        FROM     ad.day
      ) = web._date
    WHERE UPPER(ad._source) = UPPER(web._utmsource)
      AND web._utmsource IN ('linkedin', 'LinkedIn', 'Google')
    GROUP BY 1, 2, 3, 4
  ),
  /*  
  Ads data and web visits data are tied using the activity day and the URL.
  This means that there would be a duplication in numbers if several ads share the same URL on that day.
  This duplication of numbers can be handled by dividing the web metric number by the number of ads with the same URL.
   */
  add_reduced_web_page_views AS (
    SELECT
      main.*,
      side.pageviews,
      side.pageviews / side.ad_count AS reduced_pageviews,
      side.visitors,
      side.visitors / side.ad_count AS reduced_visitors
    FROM reduced_numbers_google_ads AS main
    LEFT JOIN get_web_page_views AS side 
      ON main.day = side.day
      AND main._landingpageurl = side._landingpageurl
      AND main._source = side._source
  )
SELECT
  *
FROM add_reduced_web_page_views;

------------------------------------------------
-------------- Content Analytics ---------------
------------------------------------------------
-- CREATE OR REPLACE TABLE `x-marketing.logicsource.db_ads_content_analytics` AS
TRUNCATE TABLE `x-marketing.logicsource.db_ads_content_analytics`;

INSERT INTO `x-marketing.logicsource.db_ads_content_analytics` (
    ad_id,
    ad_group_id,
    day,
    spent,
    impressions,
    clicks,
    _advariation,
    _content,
    _screenshot,
    _reportinggroup,
    _campaign,
    _source,
    _medium,
    _platform,
    _asset,
    _landingpageurl,
    _campaignname,
    _stage,
    adnum,
    reduced_spent,
    reduced_impressions,
    reduced_clicks,
    pageviews,
    reduced_pageviews,
    visitors,
    reduced_visitors,
    _contentitem,
    _contenttype,
    _gatingstrategy,
    _homeurl,
    _summary,
    _status,
    _buyerstage,
    _vertical,
    _persona,
    _jobtitles,
    _industry
  )
  WITH ads_log AS (
    SELECT
      *
    FROM `x-marketing.logicsource.ad_metrics`
  ),
  airtable AS (
    SELECT
      * EXCEPT (
        _sdc_batched_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version,
        _status
      ),
      -- Stage is set over here
      'Awareness' AS _stage
    FROM `x-marketing.logicsource_mysql.db_airtable_ads`
    WHERE _platform != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid ORDER BY _sdc_received_at DESC) = 1
  ),
  content AS (
    SELECT
      *
    FROM airtable
    JOIN `x-marketing.logicsource_mysql.db_airtable_content_inventory` CI
      ON airtable._websiteurl = CI._homeURL
  )
SELECT
  ads_log.* EXCEPT (_status),
  content._contentitem,
  content._contenttype,
  content._gatingstrategy,
  content._homeurl,
  content._summary,
  content._status,
  content._buyerstage,
  content._vertical,
  content._persona,
  content._jobtitles,
  content._industry,
FROM ads_log
LEFT JOIN content
  ON ads_log.ad_id = content._adid;