-- CREATE OR REPLACE TABLE `x-marketing.jellyvision_v2.6sense_ads_performance` AS

TRUNCATE TABLE `x-marketing.jellyvision_v2.6sense_ads_performance`;
INSERT INTO `x-marketing.jellyvision_v2.6sense_ads_performance` (
    _ad_id,
    _ad_name,
    _campaign_id,
    _campaign_name,
    _ad_group,
    _ad_copy,
    _cta_copy,
    _layout,
    _size,
    _platform,
    _segment,
    _design_color,
    _design_images,
    _design_blurb,
    _logos,
    _copy_messaging,
    _copy_asset_type,
    _copy_tone,
    _copy_product_company_name,
    _copy_statistic_proof_point,
    _cta_copy_soft_hard,
    _screenshot,
    _creative_directions,
    _date,
    _spend,
    _clicks,
    _impressions,
    _reach,
    _conversions,
    _video_views
)

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
    SUM(SAFE_CAST(base._spend AS FLOAT64)) AS _spend,
    SUM(SAFE_CAST(base._clicks AS INT64)) AS _clicks,
    SUM(SAFE_CAST(base._impressions AS INT64)) AS _impressions,
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
  sixsense_base._adid AS _ad_id, 
  sixsense_base._adname AS _ad_name, 
  sixsense_base._campaignid AS _campaign_id,  
  sixsense_base._campaignname AS _campaign_name, 
  sixsense_base._adgroup AS _ad_group,
  sixsense_airtable._adcopy AS _ad_copy, 
  sixsense_airtable._ctacopy AS _cta_copy, 
  sixsense_airtable._layout,
  sixsense_airtable._size, 
  "6Sense" AS _platform, 
  sixsense_airtable._segment,
  sixsense_airtable._designcolor AS _design_color,
  sixsense_airtable._designimages AS _design_images,
  sixsense_airtable._designblurp AS _design_blurb,
  sixsense_airtable._logos,
  sixsense_airtable._copymessaging AS _copy_messaging,
  sixsense_airtable._copyassettype AS _copy_asset_type,
  sixsense_airtable._copytone AS _copy_tone,
  sixsense_airtable._copyproductcompanyname AS _copy_product_company_name,
  sixsense_airtable._copystatisticproofpoint AS _copy_statistic_proof_point,
  sixsense_airtable._ctacopysofthard AS _cta_copy_soft_hard, 
  sixsense_airtable._screenshot,
  sixsense_airtable._creativedirections AS _creative_directions,
  sixsense_base._date,
  sixsense_base._spend,
  sixsense_base._clicks,
  sixsense_base._impressions,
  sixsense_base._reach,
  sixsense_base._conversions,
  sixsense_base._video_views
FROM sixsense_base
LEFT JOIN sixsense_airtable 
  ON sixsense_base._adid = CAST(sixsense_airtable._adid AS STRING);

