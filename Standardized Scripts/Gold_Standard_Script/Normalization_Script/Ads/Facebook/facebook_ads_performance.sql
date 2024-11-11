CREATE OR REPLACE TABLE `x-marketing.jellyvision_v2.facebook_ads_performance` AS

WITH  
FB_airtable AS (
  SELECT
    _adid, 
    _advariation AS _adname, 
    '' _campaignid,  
    _maincampaignname AS _campaignname, 
    '' _adgroup,
    '' _adcopy, 
    '' _ctacopy, 
    /*IF(LENGTH(_designtemplate) > 0, _designtemplate, _layout)*/ '' AS _layout,
    _adsize AS _size, 
    _platform, 
    '' _segment,
    '' _designcolor,
    '' _designimages,
    '' _designblurp,
    '' _logos,
    '' _copymessaging,
    '' _copyassettype,
    '' _copytone,
    '' _copyproductcompanyname,
    '' _copystatisticproofpoint,
    '' _ctacopysofthard, 
    _advisual AS _screenshot,
    _creativedirections
  
  FROM `x-marketing.jellyvision_mysql_2.optimization_airtable_ads_facebook`
  
  WHERE LENGTH(_adid) > 2
  
  GROUP BY ALL
),

FB_base AS (
SELECT
    ad_id AS _adid,
    ad_name as _adname,
    SAFE_CAST(adset_id AS INT64) AS _adgroup,
    adset_name,
    SAFE_CAST(campaign_id AS INT64) AS _campaignid,
    campaign_name AS _campaignname,
    DATE(date_start) AS _date,
    spend AS _spend,
    impressions AS _impressions,
    clicks AS _clicks,

FROM `x-marketing.jellyvision_facebook_ads.ads_insights` AS ads_insights
LEFT JOIN UNNEST (ads_insights.actions) AS actions
  ON actions.value.action_type = 'lead'
)

SELECT
    FB_base._adid AS _ad_id, 
    FB_base._adname AS _ad_name, 
    FB_base._campaignid AS _campaign_id,  
    FB_base._campaignname AS _campaign_name, 
    FB_base._adgroup AS _ad_group,
    FB_airtable._adcopy AS _ad_copy, 
    FB_airtable._ctacopy AS _cta_copy, 
    FB_airtable._layout,
    FB_airtable._size, 
    "Facebook" AS _platform, 
    FB_airtable._segment,
    FB_airtable._designcolor AS _design_color,
    FB_airtable._designimages AS _design_images,
    FB_airtable._designblurp AS _design_blurb,
    FB_airtable._logos,
    FB_airtable._copymessaging AS _copy_messaging,
    FB_airtable._copyassettype AS _copy_asset_type,
    FB_airtable._copytone AS _copy_tone,
    FB_airtable._copyproductcompanyname AS _copy_product_company_name,
    FB_airtable._copystatisticproofpoint AS _copy_statistic_proof_point,
    FB_airtable._ctacopysofthard AS _cta_copy_soft_hard, 
    FB_airtable._screenshot,
    FB_airtable._creativedirections AS _creative_directions,
    FB_base._date,
    FB_base._spend,
    FB_base._clicks,
    FB_base._impressions,
    0 AS _reach,      
    0 AS _conversions,      
    0 AS _video_views
FROM FB_base
LEFT JOIN FB_airtable 
    ON FB_base._adid = CAST(FB_airtable._adid AS STRING);

