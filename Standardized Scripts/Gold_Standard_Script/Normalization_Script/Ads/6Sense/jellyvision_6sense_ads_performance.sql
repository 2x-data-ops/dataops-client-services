CREATE OR REPLACE TABLE `x-marketing.jellyvision.6sense_ads_performance` AS

WITH sixsense_airtable AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
    _adgroup,
    _adcopy, 
    _ctacopy, 
    IF(LENGTH(_designtemplate) > 0, _designtemplate, _layout) AS _layout,
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
    _creativedirections
  FROM `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_6sense`
  WHERE LENGTH(_adid) > 2
  GROUP BY ALL
), 
sixsense_campaigns AS (
  SELECT DISTINCT
    _6senseid AS _campaignid,
    _name AS _campaignname
  FROM `x-marketing.jellyvision_mysql.jellyvision_db_6sense_daily_campaign_performance`
  WHERE _datatype = 'Campaign'
), 
sixsense_base AS (  
  SELECT
    base._6senseid AS _adid,
    base._name AS _adname,
    SAFE_CAST(base._campaignid AS INT64) AS _campaignid,
    sixsense_campaigns._campaignname,
    SAFE_CAST(base._adgroupid AS INT64) AS _adgroup,
    CASE
      WHEN base._date LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', base._date)
      WHEN base._date LIKE '%-%'
        THEN PARSE_DATE('%F', base._date)
    END AS _date,
    SUM(CAST(base._spend AS FLOAT64)) AS _spend,
    SUM(CAST(base._clicks AS INT64)) AS _clicks,
    SUM(CAST(base._impressions AS INT64)) AS _impressions,
    0 AS _reach,
    0 AS _conversions,
    0 AS _video_views,
  FROM `x-marketing.jellyvision_mysql.jellyvision_db_6sense_daily_campaign_performance` AS base
  LEFT JOIN sixsense_campaigns
    ON base._campaignid = sixsense_campaigns._campaignid
  WHERE base._datatype = 'Ad'
  GROUP BY ALL
)
SELECT
  sixsense_base._adid, 
  sixsense_base._adname, 
  sixsense_base._campaignid,  
  sixsense_base._campaignname, 
  sixsense_base._adgroup,
  sixsense_airtable._adcopy, 
  sixsense_airtable._ctacopy, 
  sixsense_airtable._layout,
  sixsense_airtable._size, 
  "6Sense" AS _platform, 
  sixsense_airtable._segment,
  sixsense_airtable._designcolor,
  sixsense_airtable._designimages,
  sixsense_airtable._designblurp,
  sixsense_airtable._logos,
  sixsense_airtable._copymessaging,
  sixsense_airtable._copyassettype,
  sixsense_airtable._copytone,
  sixsense_airtable._copyproductcompanyname,
  sixsense_airtable._copystatisticproofpoint,
  sixsense_airtable._ctacopysofthard, 
  sixsense_airtable._screenshot,
  sixsense_airtable._creativedirections,
  sixsense_base._date,
  sixsense_base._spend,
  sixsense_base._clicks,
  sixsense_base._impressions,
  sixsense_base._reach,
  sixsense_base._conversions,
  sixsense_base._video_views
FROM sixsense_base
LEFT JOIN sixsense_airtable 
  ON sixsense_base._adid = CAST(sixsense_airtable._adid AS STRING)
;