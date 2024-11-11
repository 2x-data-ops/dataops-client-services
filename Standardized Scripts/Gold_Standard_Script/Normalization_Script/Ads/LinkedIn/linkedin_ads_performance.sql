CREATE OR REPLACE TABLE `x-marketing.jellyvision_v2.linkedin_ads_performance` AS
WITH

LI_airtable AS (
  SELECT
    _adid, 
    _adtitle AS _adname, 
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
  
  FROM `x-marketing.jellyvision_mysql_2.optimization_airtable_ads_linkedin`
  
  WHERE LENGTH(_adid) > 2
  
  GROUP BY ALL
)

, LI_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS DATE) AS _date,
    CASE WHEN CAST(creative_id AS STRING) = '448469914'  AND CAST(start_at AS DATE) = '2024-09-01' THEN 0 
    WHEN CAST(creative_id AS STRING) = '448452214'  AND CAST(start_at AS DATE) = '2024-09-01' THEN 0 ELSE IFNULL(SUM(cost_in_usd), 0) END AS _spend, 
    IFNULL(SUM(clicks), 0) AS _clicks, 
    IFNULL(SUM(impressions), 0) AS _impressions, 
    IFNULL(SUM(impressions), 0) AS _reach,
    IFNULL(SUM(external_website_conversions), 0) AS _conversions, 
    IFNULL(SUM(video_views), 0) AS _video_views, 

  FROM `x-marketing.jellyvision_linkedin_ads.ad_analytics_by_creative`

  WHERE start_at IS NOT NULL

  GROUP BY creative_id, start_at
)

, LI_ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS _adid,
    campaign_id AS _campaignid,
  
  FROM `x-marketing.jellyvision_linkedin_ads.creatives`
)

, LI_campaigns AS (
  SELECT 
    id AS _campaignid,
    name AS _campaignname,
    campaign_group_id AS li_campaign_group_id
  
  FROM `x-marketing.jellyvision_linkedin_ads.campaigns`
)

, LI_campaign_groups AS ( 
    SELECT 
      id AS li_campaign_group_id, 
      name AS li_campaign_group_name, 
      status AS li_campaign_group_status
    FROM `x-marketing.jellyvision_linkedin_ads.campaign_groups` 
)

, LI_base AS (
  SELECT
    LI_ads._adid,
    LI_ads_title._campaignid,
    LI_campaigns._campaignname,
    LI_campaigns.li_campaign_group_id,
    LI_campaign_groups.li_campaign_group_name,
    LI_campaign_groups.li_campaign_group_status,
    LI_ads._date,
    LI_ads._spend,
    LI_ads._clicks,
    LI_ads._impressions,
    LI_ads._reach,
    LI_ads._conversions,
    LI_ads._video_views,
    "LinkedIn" AS _platform,

  FROM LI_ads
  LEFT JOIN LI_ads_title
    ON LI_ads._adid = LI_ads_title._adid
  LEFT JOIN LI_campaigns
    ON LI_ads_title._campaignid = LI_campaigns._campaignid
  LEFT JOIN LI_campaign_groups
    ON LI_campaigns.li_campaign_group_id = LI_campaign_groups.li_campaign_group_id
)

SELECT
    LI_base._adid AS _ad_id, 
    LI_airtable._adname AS _ad_name, 
    LI_base._campaignid AS _campaign_id,  
    LI_base._campaignname AS _campaign_name, 
    LI_base.li_campaign_group_id AS _ad_group,
    LI_airtable._adcopy AS _ad_copy, 
    LI_airtable._ctacopy AS _cta_copy, 
    LI_airtable._layout,
    LI_airtable._size, 
    "LinkedIn" AS _platform, 
    LI_airtable._segment,
    LI_airtable._designcolor AS _design_color,
    LI_airtable._designimages AS _design_images,
    LI_airtable._designblurp AS _design_blurb,
    LI_airtable._logos,
    LI_airtable._copymessaging AS _copy_messaging,
    LI_airtable._copyassettype AS _copy_asset_type,
    LI_airtable._copytone AS _copy_tone,
    LI_airtable._copyproductcompanyname AS _copy_product_company_name,
    LI_airtable._copystatisticproofpoint AS _copy_statistic_proof_point,
    LI_airtable._ctacopysofthard AS _cta_copy_soft_hard, 
    LI_airtable._screenshot,
    LI_airtable._creativedirections AS _creative_directions,
    LI_base._date,
    LI_base._spend,
    LI_base._clicks,
    LI_base._impressions,
    LI_base._reach,
    LI_base._conversions,
    LI_base._video_views
FROM LI_base
LEFT JOIN LI_airtable 
    ON LI_base._adid = CAST(LI_airtable._adid AS STRING);
