CREATE OR REPLACE TABLE `x-marketing.jellyvision.facebook_ads_performance` AS

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
    FB_base._adid, 
    FB_base._adname, 
    FB_base._campaignid,  
    FB_base._campaignname, 
    FB_base._adgroup,
    FB_airtable._adcopy, 
    FB_airtable._ctacopy, 
    FB_airtable._layout,
    FB_airtable._size, 
    "Facebook" AS _platform, 
    FB_airtable._segment,
    FB_airtable._designcolor,
    FB_airtable._designimages,
    FB_airtable._designblurp,
    FB_airtable._logos,
    FB_airtable._copymessaging,
    FB_airtable._copyassettype,
    FB_airtable._copytone,
    FB_airtable._copyproductcompanyname,
    FB_airtable._copystatisticproofpoint,
    FB_airtable._ctacopysofthard, 
    FB_airtable._screenshot,
    FB_airtable._creativedirections,
    FB_base._date,
    FB_base._spend,
    FB_base._clicks,
    FB_base._impressions,
    0 AS _reach,      
    0 AS _conversions,      
    0 AS _video_views,

FROM FB_base
LEFT JOIN FB_airtable 
    ON FB_base._adid = CAST(FB_airtable._adid AS STRING)    
