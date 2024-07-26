--ads overview 6sense
CREATE OR REPLACE TABLE `x-marketing.sandler.db_6sense_ads_overview` AS
WITH _ads AS (
SELECT DISTINCT
    main._campaignid AS campaignID,
    main._6senseid AS _adid,
    PARSE_DATE('%m/%d/%Y', _date) AS _date,
    main._sdc_sequence AS _sdc_sequence,
    '' AS adGroup,
    main._name AS adName,
    '' AS adVariation,
    '' AS adSize,
    '' AS dataType,
    _status AS status,
    CASE
        WHEN _startdate = '-' THEN NULL
        WHEN _startdate LIKE '%-%' THEN PARSE_DATE('%e-%h-%y', _startdate) 
        WHEN _startdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _startdate) 
    END AS startDate,
    CASE 
        WHEN _enddate = '-' THEN NULL
        WHEN _enddate LIKE '%-%' THEN PARSE_DATE('%e-%h-%y', _enddate) 
        WHEN _enddate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _enddate) 
    END AS endDate,
    #CAST(REPLACE(_accountreached, ',', '') AS INT) AS accountsReached,
    CASE WHEN _accountreached LIKE '%.%' THEN CAST(_accountreached AS DECIMAL) ELSE CAST(REPLACE(_accountreached, ',', '') AS INT) END AS accountsReached,
    CAST(REPLACE(_impressions, ',', '') AS INT) AS impressions,
    CASE WHEN _clicks LIKE '%.%' THEN CAST(_clicks AS DECIMAL) ELSE CAST(REPLACE(_clicks, ',', '') AS INT) END AS clicks,
    CAST(REPLACE(_ctr, '%', '') AS DECIMAL) / 100 AS ctr,
    CAST(REPLACE(_accountctr, '%', '') AS DECIMAL) / 100 AS actr,
    #CAST(REPLACE(REPLACE(_ecpm, ',', ''), '$', '') AS DECIMAL) AS cpm,
    CAST(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(_ecpm, '(', '-'),
            ')', ''
          ),
          ',', ''
        ),
        '$', ''
      ) AS DECIMAL
    ) AS cpm, -- change values with () to negative '-'
    CAST(REPLACE(_ecpc, '$', '') AS DECIMAL) AS cpc,
    CAST(REPLACE(_vtr, '%', '') AS DECIMAL) / 100 AS vtr,
    CAST(REPLACE(_accountvtr, '%', '') AS DECIMAL) / 100 AS avtr,
    CASE WHEN _budget = '-' THEN 0 
    ELSE CAST(REPLACE(REPLACE(_budget, ',', ''), '$', '') AS DECIMAL) END AS budget,
    CAST(REPLACE(REPLACE(_spend, ',', ''), '$', '') AS DECIMAL) AS spend
FROM `x-marketing.sandler_mysql.db_daily_campaign_performance` main
WHERE _name <> "" AND _datatype ='Ad'
),

_campaign AS (
  SELECT main._campaignid AS campaignID,
         main._name AS campaignname
  FROM `x-marketing.sandler_mysql.db_daily_campaign_performance` main
  WHERE _name <> "" AND _datatype ='Campaign'
)
SELECT _ads.*,
       _campaign.campaignname
FROM _ads
LEFT JOIN _campaign ON _campaign.campaignID = _ads.campaignID
QUALIFY ROW_NUMBER() OVER (PARTITION BY _adid, campaignID, _date ORDER BY _adid DESC) = 1;


--ads optimization
TRUNCATE TABLE `sandler.dashboard_opimization_ads` ;
INSERT INTO `x-marketing.sandler.dashboard_opimization_ads` (
  _adid, 
  _adname,
  _campaignname,
  _adgroup,
  _adcopy, 
  _ctacopy, 
  _designtemplate, 
  _size, 
  _platform, 
  _segment, 
  _designcolor,
  _designimages,
  _designblurp,
  _logos,
  _copymessaging,
  _copyassettype,
  _copytone,
  _copyproductcompanyname,
  _copystatisticproofpoint,
  _ctacopysofthard,
  _screenshot, 
  _painPoint,
  _pillars, 
  _instance, 
  _date, 
  _spend, 
  _clicks, 
  _impressions,
  _conversions,
  _campaignid, 
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition
  )
WITH
linkedin_ads AS (
    SELECT
      CAST(creative_id AS STRING) AS _adid,
      CAST(start_at AS TIMESTAMP) AS _date,
      SUM(cost_in_usd) AS _spend, 
      SUM(clicks) AS _clicks, 
      SUM(impressions) AS _impressions,
      SUM(external_website_conversions) AS _conversions
    FROM
      `x-marketing.sandler_linkedin_ads_v2.ad_analytics_by_creative`
    WHERE 
      start_at IS NOT NULL
    GROUP BY 
      creative_id, start_at
),
ads_title AS (
    SELECT
      SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
      campaign_id
    FROM
      `x-marketing.sandler_linkedin_ads_v2.creatives`
),
campaigns AS (
    SELECT 
      id AS _campaignID,
      name AS _campaignname
    FROM 
      `x-marketing.sandler_linkedin_ads_v2.campaigns`
),
s6ense_ads AS (
    SELECT
      _adid,
      adName AS _adname,
      campaignID AS _campaignid,
      CAST(_date AS TIMESTAMP) AS _date,
      SUM(spend) AS _spend, 
      SUM(CAST(clicks AS INT64)) AS _clicks, 
      SUM(impressions) AS _impressions,
      NULL AS _conversions
    FROM
      `x-marketing.sandler.db_6sense_ads_overview`
    WHERE 
      _date IS NOT NULL
    GROUP BY 
      _adid, adName, campaignID, _date   
),
linkedin_airtable_network AS (
  SELECT
    _adid, 
    _adname, 
    CASE WHEN _campaignid = "" THEN NULL ELSE _campaignid END AS _campaignid,  
    _campaignname, 
    _adgroup,
    _adcopy, 
    _ctacopy, 
    _designtemplate, 
    _size, 
    _platform, 
    _segment,
    _designcolor,
    _designimages,
    _designblurp,
    _logos,
    _copymessaging,
    _copyassettype,
    _copytone,
    _copyproductcompanyname,
    _copystatisticproofpoint,
    _ctacopysofthard, 
    _screenshot,
    _painPoint,
    _pillars,
    'Sandler Network' AS _instance
  FROM
    `x-marketing.sandlernetwork_mysql.sandlernetwork_optimization_airtable_ads_linkedin`
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
  GROUP BY ALL
),
linkedin_airtable_franchise AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
    '' AS _adgroup,
    _adcopy, 
    _ctacopy, 
    _designtemplate, 
    _size, 
    _platform, 
    _segment,
    _designcolor,
    _designimages,
    _designblurp,
    _logos,
    _copymessaging,
    _copyassettype,
    _copytone,
    _copyproductcompanyname,
    _copystatisticproofpoint,
    _ctacopysofthard, 
    _screenshot,
    _painPoint,
    _pillars,
    'Sandler' AS _instance
  FROM
    `x-marketing.sandler_mysql.optimization_airtable_ads_linkedin`
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
  GROUP BY ALL
),
s6sense_airtable AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
    '' AS _adgroup,
    _adcopy, 
    _ctacopy, 
    _designtemplate, 
    _size, 
    _platform, 
    _segment,
    _designcolor,
    _designimages,
    _designblurp,
    _logos,
    _copymessaging,
    _copyassettype,
    _copytone,
    _copyproductcompanyname,
    _copystatisticproofpoint,
    _ctacopysofthard, 
    _screenshot,
    _painPoint,
    _pillars,
    'Sandler' AS _instance
  FROM
    `x-marketing.sandler_mysql.optimization_airtable_ads_6sense` 
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
  GROUP BY ALL
),
linkedin_combined_network AS (
  SELECT
    linkedin_airtable_network.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions
  FROM 
    linkedin_ads
  JOIN
    linkedin_airtable_network ON linkedin_ads._adid = CAST(linkedin_airtable_network._adid AS STRING)
  RIGHT JOIN 
    ads_title ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN 
    campaigns ON campaigns._campaignID = ads_title.campaign_id
),
linkedin_combined_sandler AS (
  SELECT
    linkedin_airtable_franchise.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions
  FROM 
    linkedin_ads
  JOIN
    linkedin_airtable_franchise ON linkedin_ads._adid = CAST(linkedin_airtable_franchise._adid AS STRING)
  LEFT JOIN 
    ads_title ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN 
    campaigns ON campaigns._campaignID = ads_title.campaign_id
),
s6sense_combined_sandler AS (
  SELECT
    s6ense_ads._adid,
    s6sense_airtable._adname,
    s6sense_airtable._campaignid,
    s6sense_airtable._campaignname,
    s6sense_airtable._adgroup,
    s6sense_airtable._adcopy,
    s6sense_airtable._ctacopy,
    s6sense_airtable._designtemplate,
    s6sense_airtable._size,
    s6sense_airtable._platform,
    s6sense_airtable._segment,
    s6sense_airtable._designcolor,
    s6sense_airtable._designimages,
    s6sense_airtable._designblurp,
    s6sense_airtable._logos,
    s6sense_airtable._copymessaging,
    s6sense_airtable._copyassettype,
    s6sense_airtable._copytone,
    s6sense_airtable._copyproductcompanyname,
    s6sense_airtable._copystatisticproofpoint,
    s6sense_airtable._ctacopysofthard,
    s6sense_airtable._screenshot,
    s6sense_airtable._painPoint,
    s6sense_airtable._pillars,
    s6sense_airtable._instance,
    s6ense_ads._date,
    s6ense_ads._spend,
    s6ense_ads._clicks,
    s6ense_ads._impressions,
    s6ense_ads._conversions
  FROM 
    s6ense_ads
  JOIN
    s6sense_airtable ON s6ense_ads._adid = s6sense_airtable._adid AND s6ense_ads._campaignid = s6sense_airtable._campaignid
),
google_display_combined AS (

WITH ad_counts AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM
    `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN
    `x-marketing.sandler_google_ads.ads` ad ON ad.ad_group_id = report.ad_group_id
  JOIN
    `x-marketing.sandler_mysql.db_airtable_google_display_ads` airtable ON airtable._adid = CAST(ad.id AS STRING)
  WHERE
    ad.name IS NOT NULL
  GROUP BY
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date
),

adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    airtable._adname,
    '' AS _adcopy,
    _screenshot,
    '' AS _ctacopy,
    report.ad_group_id, 
    report.ad_group_name, 
    report.campaign_id,
    report.campaign_name, 
    ad.name AS ad_name, 
    report.date AS _date,
    airtable._adsize,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent, 
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks, 
    impressions / c.ad_count AS adjusted_impressions,
    airtable._painpoint,
    airtable._pillars,
    ad_count
  FROM
    `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN
    `x-marketing.sandler_google_ads.ads` ad ON ad.ad_group_id = report.ad_group_id
  JOIN
    `x-marketing.sandler_mysql.db_airtable_google_display_ads` airtable ON airtable._adid = CAST(ad.id AS STRING)
  JOIN
    ad_counts c ON ad.ad_group_id = c.ad_group_id AND report.date = c.date
  WHERE
    ad.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ad.id, campaign_id, report.date ORDER BY report.date DESC) = 1
)

SELECT
  _adid,
  ad_name,
    CAST(campaign_id AS STRING) AS _campaignID,
  campaign_name,
  ad_group_name,
  '' AS _adcopy,
  '' AS _ctacopy,
  '' AS _designtemplate, 
  _adsize AS _size, 
  'Google Display' AS _platform, 
  '' AS _segment, 
  '' AS _designcolor,
  '' AS _designimages,
  '' AS _designblurp,
  '' AS _logos,
  '' AS _copymessaging,
  '' AS _copyassettype,
  '' AS _copytone,
  '' AS _copyproductcompanyname,
  '' AS _copystatisticproofpoint,
  '' AS _ctacopysofthard,
  _screenshot, 
  _painPoint,
  _pillars,
  'Sandler' AS _instance,
  _date,
  SUM(CAST(adjusted_spent AS NUMERIC)) AS total_spent,
  SUM(CAST(adjusted_clicks AS NUMERIC)) AS total_clicks,
  SUM(CAST(adjusted_impressions AS NUMERIC)) AS total_impressions,
  SUM(CAST(adjusted_conversions AS NUMERIC)) AS total_conversions,
FROM
  adjusted_metrics
GROUP BY ALL
ORDER BY
  campaign_name, _date DESC  

),

google_sem AS (
  WITH google_overview AS (
  SELECT
  CAST(id AS STRING) AS _adid,
  '' AS ad_name,
    CAST(campaign_id AS STRING) AS _campaignID,
  campaign_name,
  ad_group_name,
  '' AS _adcopy,
  '' AS _ctacopy,
  '' AS _designtemplate, 
  '' AS _size, 
  'Google SEM' AS _platform, 
  '' AS _segment, 
  '' AS _designcolor,
  '' AS _designimages,
  '' AS _designblurp,
  '' AS _logos,
  '' AS _copymessaging,
  '' AS _copyassettype,
  '' AS _copytone,
  '' AS _copyproductcompanyname,
  '' AS _copystatisticproofpoint,
  '' AS _ctacopysofthard,
  '' _screenshot, 
  '' _painPoint,
  '' _pillars,
  'Sandler' AS _instance,
  date AS _date,
  CAST(cost_micros / 1000000 AS NUMERIC) AS _spent,
  clicks AS _clicks,
  impressions AS _impressions,
  conversions AS _conversions,
  FROM
    `x-marketing.sandler_google_ads.ad_performance_report` report
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id, ad_group_id, id ORDER BY _sdc_received_at DESC) = 1
),
 
 aggregated AS (
  SELECT * EXCEPT (_spent, _clicks, _impressions, _conversions),
  SUM(_spent) AS _spent,
  SUM(_clicks) AS _clicks,
  SUM(_impressions) AS _impressions,
  SUM(_conversions) AS _conversions
  FROM google_overview
  GROUP BY ALL
 )
 SELECT *
 FROM aggregated


),
_all AS (
  SELECT * EXCEPT (_campaignID),
    CAST(_campaignID AS INT64) AS _campaignid,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM _date), '-', EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM (
    SELECT * FROM linkedin_combined_network
    UNION ALL
    SELECT * FROM linkedin_combined_sandler
    UNION ALL
    SELECT * FROM s6sense_combined_sandler
    UNION ALL
    SELECT * FROM google_display_combined
    UNION ALL
    SELECT * FROM google_sem
  )
)

SELECT 
  _all.*,
  CASE 
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (4, 5, 6) THEN 
      CASE 
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (1, 2, 3) THEN 
      CASE 
        WHEN year = (SELECT MAX(year) - 1 FROM _all) AND quarter = 4 THEN 1
        WHEN year = (SELECT MAX(year) - 1 FROM _all) AND quarter = 3 THEN 2
      END
    ELSE 
      CASE 
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 2 THEN 2
      END
  END AS _quarterpartition
FROM 
  _all
GROUP BY 
  ALL;