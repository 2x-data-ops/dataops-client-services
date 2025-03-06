CREATE OR REPLACE TABLE `x-marketing.plextrac.ad_metrics` AS
WITH airtable_ads AS (
  WITH 
    airtable AS (
        SELECT * EXCEPT(rownum)
        FROM ( 
            SELECT 
                * EXCEPT(
                    _sdc_batched_at, 
                    _sdc_received_at,
                    _sdc_sequence, 
                    _sdc_table_version
                ),
                -- Stage is set over here
                'Awareness' AS _stage,
                ROW_NUMBER() OVER(
                    PARTITION BY _adid
                    ORDER BY _sdc_received_at DESC
                ) AS rownum
            FROM 
              `x-marketing.toolsgroup_mysql.db_airtable_ads`
            WHERE _platform != ''
        )
        WHERE rownum = 1
    ), 
    ads_title AS (
        SELECT
            SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS cID,
            campaign_id,
            c.account_id,
            intended_status,
            name  as _advariation,
            content_reference AS _content
        FROM
            `plextrac_linkedin_ads.creatives`c  
            LEFT JOIN  x-marketing.plextrac_linkedin_ads.video_ads v ON content.reference  = v.content_reference 
        ),
        campaigns AS (
        SELECT
            id AS campaignID,
            name AS _campaignName,
            status,
            cost_type,
            total_budget.amount AS total_budget,
            campaign_group_id,
            run_schedule.end,run_schedule.start,
            TIMESTAMP_DIFF( run_schedule.end,run_schedule.start,DAY) AS date_diffs,
            type
        FROM
            `plextrac_linkedin_ads.campaigns`
    ),
    campaign_group AS (
           SELECT 
        id AS groupID, 
        name AS _groupName, 
        status 
    FROM 
        `x-marketing.plextrac_linkedin_ads.campaign_groups` 

    ),linkedin AS (
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
        type AS _adtype	,
       'Linkedin' AS _platform,
        "" AS _asset,
        _landingpageurl AS _landingpageurl,
        campaigns._campaignName AS _campaignname,
        "Awareness" AS _stage
    FROM 
        ads_title
    LEFT JOIN 
        campaigns ON ads_title.campaign_id = campaigns.campaignID
    LEFT JOIN  
    campaign_group ON campaigns.campaign_group_id = campaign_group.groupID
    LEFT JOIN 
        airtable  ON ads_title.cID = CAST(airtable._adid AS STRING)
    ), google AS  (
      SELECT 
      DISTINCT CAST(campaign_id AS STRING) AS _campaign,
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
      'Google' AS  _platform, 
      '' AS _asset,
      '' AS _landingpageurl,
      campaign.name AS _campaignname,
      "Awareness" AS _stage
      FROM `x-marketing.plextrac_google_ads.ad_groups` adgroup
      LEFT JOIN `x-marketing.plextrac_google_ads.campaigns` campaign ON campaign.id = adgroup.campaign_id
    ),
    _6sense AS (
    SELECT 
        CAST(_campaignid AS STRING) AS _campaign,
        CAST(_advariationid AS STRING) AS _adid,
        _adgroupid AS ad_group_id,
        "" AS _status,
        "" AS _advariation,
        "" AS _content,
        "" AS _screenshot,
        _adgroup AS _reportinggroup,
        "" AS _source,
        "" AS _medium,
        "" AS _id,
        "" AS _adtype,
        "6sense" AS _platform,
        "" AS _asset,
        "" AS _landingpageurl,
        _campaignname AS _campaignname,
        "Awareness" AS _stage,

      FROM `x-marketing.plextrac_mysql.db_airtable_6sense_campaign` 
    )
    SELECT * FROM linkedin 
    UNION ALL
   SELECT * FROM google
   UNION ALL
    SELECT * FROM _6sense
),

ads_metrics AS (
WITH linkedin_ads AS (
SELECT 
        main.creative_id AS ad_id, 
        creative.campaign_id,
        campaign_group.id AS ad_group_id,
        main.start_at AS day, 
        CAST(main.cost_in_usd AS FLOAT64) AS spent, 
        main.impressions AS impressions, 
        main.clicks,
        'LinkedIn' AS _platform_type,   
    FROM 
      `x-marketing.plextrac_linkedin_ads.ad_analytics_by_creative` main
    JOIN 
      `x-marketing.plextrac_linkedin_ads.creatives` creative
      ON CAST(main.creative_id AS STRING) = SPLIT(SUBSTR(creative.id, STRPOS(creative.id, 'sponsoredCreative:')+18))[ORDINAL(1)]
    JOIN 
      `x-marketing.plextrac_linkedin_ads.campaigns` campaign 
      ON creative.campaign_id = campaign.id
    JOIN 
      `x-marketing.plextrac_linkedin_ads.campaign_groups` campaign_group 
      ON campaign.campaign_group_id = campaign_group.id
    ORDER BY 
      main.start_at DESC
),

google_ads AS (
    SELECT
        NULL AS ad_id,
        campaign.campaign_id, 
        ad_group_id AS ad_group_id,
        campaign.date AS day,
        campaign.cost_micros/1000000 AS spent, 
        campaign.impressions AS impressions, 
        campaign.clicks,
        'Google' AS _platform_type,
    FROM  `x-marketing.plextrac_google_ads.ad_group_performance_report` campaign
),

_6sense_ads AS (
    SELECT
        CAST(_adid AS INT64) AS ad_id,
        CAST(_campaignid AS INT64) AS campaign_id, 
        NULL AS ad_group_id,
        CAST(_date AS TIMESTAMP) AS day,
        _spend AS spent, 
        _impressions AS impressions, 
        _clicks,
        '6sense' AS _platform_type,
    FROM  `x-marketing.plextrac.db_6sense_ads_performance` campaign

)
SELECT *
FROM (
  SELECT * FROM linkedin_ads
  UNION ALL
  SELECT * FROM google_ads
  UNION ALL
  SELECT * FROM _6sense_ads
  )

)
,all_ads AS (
  WITH linkedin_ads AS (
    SELECT * EXCEPT(_adid,_campaign,ad_group_id)
    FROM airtable_ads
    JOIN ads_metrics ON CAST(ads_metrics.ad_id AS STRING)  = airtable_ads._adid
    WHERE _platform = 'Linkedin'
  ),
  google_ads AS (
    SELECT * EXCEPT(_adid,_campaign,ad_group_id)
    FROM airtable_ads
    JOIN ads_metrics ON CAST(ads_metrics.campaign_id AS STRING) = airtable_ads._campaign AND airtable_ads.ad_group_id = ads_metrics.ad_group_id
    WHERE _platform = 'Google'
  ),
  _6sense_ads AS (
    SELECT * 
    EXCEPT(_adid,_campaign,ad_group_id)
    FROM airtable_ads
    LEFT JOIN ads_metrics   ON CAST(ads_metrics.ad_id AS STRING)  = airtable_ads._adid
   WHERE _platform = '6sense'
  )
  SELECT *
  FROM (
    SELECT * FROM linkedin_ads
    UNION ALL
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM _6sense_ads
  )
) ,
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
        FROM `x-marketing.plextrac.db_web_engagements_log`
    ) web
    JOIN (
        SELECT DISTINCT
            day,
            _source,
            _landingpageurl,
            -- Count the number of ads sharing the same URL
            COUNT(DISTINCT ad_id) AS ad_count
        FROM all_ads
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC
    ) ad 
    ON ad._landingpageurl LIKE CONCAT('%', web._fullpage, '%')
    AND EXTRACT(DATETIME FROM ad.day) = web._date
    WHERE UPPER(ad._source) = UPPER(web._utmsource) 
    AND LOWER(web._utmsource) IN('linkedin','google')
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
    AND main._source = side._source
