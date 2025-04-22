TRUNCATE TABLE `x-marketing.brp.facebook_ads_performance`;
INSERT INTO `x-marketing.brp.facebook_ads_performance` (
  ad_id,
  adset_id,
  campaign_id,
  ads_name,
  adset_name,
  campaign_name,
  date_start,
  reach,
  spend,
  impressions,
  _clicks,
  inline_link_clicks,
  unique_inline_link_clicks,
  ctr,
  cost_per_unique_click,
  cpp,
  cpc,
  conversions,
  bid_type,
  _status_ads,
  body,
  _title,
  url_tags,
  _ad_status,
  title,
  thumbnail_url,
  instagram_actor_id,
  image_url,
  created_time,
  lifetime_budget,
  budget_remaining,
  end_time,
  objective,
  buying_type,
  effective_status,
  updated_time,
  creative_id,
  _adid,
  _status,
  _advariation,
  _screenshot,
  _airtableid,
  _reportinggroup,
  _campaignname,
  _id,
  _adtype,
  _livedate,
  _platform,
  _landingpage,
  _adname,
  ads_per_campaign,
  dailyBudget_per_ad,
  dailybudget_remaining_per_ad
)
WITH FB_ads AS (
  SELECT
    ad_id,
    adset_id,
    campaign_id,
    ad_name as ads_name,
    adset_name,
    campaign_name,
    date_start,
    reach,
    spend,
    impressions,
    clicks AS _clicks,
    inline_link_clicks,
    unique_inline_link_clicks,
    ctr,
    cost_per_unique_click,
    cpp, 
    cpc, 
    (
      SELECT 
        action.value.value 
      FROM UNNEST(actions) AS action 
      WHERE action.value.action_type = 'lead'
    ) AS conversions
  FROM `x-marketing.brp_rogersgray_facebook_ads.ads_insights` 
),
ad_names AS (
  SELECT 
    adset_id, 
    k.id as _ads_id, 
    k.name AS ads_name, 
    bid_type, 
    k.status AS _status_ads, 
    body, 
    l.name AS _title, 
    url_tags, 
    l.status AS _ad_status, 
    title, 
    thumbnail_url, 
    instagram_actor_id, 
    image_url,
    creative.id AS creative_id
  FROM `x-marketing.brp_rogersgray_facebook_ads.ads` k
  LEFT JOIN `x-marketing.brp_rogersgray_facebook_ads.adcreative` l 
    ON creative.id = l.id  
), 
ad_adsets AS (
  SELECT 
    id, 
    name, 
    created_time, 
    lifetime_budget,
    budget_remaining, 
    end_time 
  FROM `x-marketing.brp_rogersgray_facebook_ads.adsets`
),
ad_campaign AS (
  SELECT 
    id, 
    name, 
    objective, 
    buying_type, 
    effective_status, 
    updated_time 
  FROM `x-marketing.brp_rogersgray_facebook_ads.campaigns`
), 
airtable_ads AS (
  SELECT 
    * EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) 
  FROM `x-marketing.brp_mysql.optimization_airtable_ads_facebook` -- no update on the new airtable migration
),
combine_all AS (
  SELECT 
    FB_ads.*,
    ad_names.bid_type, 
    ad_names._status_ads, 
    ad_names.body, 
    ad_names._title, 
    ad_names.url_tags, 
    ad_names._ad_status, 
    ad_names.title, 
    ad_names.thumbnail_url, 
    ad_names.instagram_actor_id, 
    ad_names.image_url,
    ad_adsets.created_time, 
    ad_adsets.lifetime_budget,
    ad_adsets.budget_remaining, 
    ad_adsets.end_time,
    ad_campaign.objective, 
    ad_campaign.buying_type, 
    ad_campaign.effective_status, 
    ad_campaign.updated_time,
    CAST(ad_names.creative_id AS INT64) AS creative_id,
    airtable_ads.*
  FROM FB_ads
  LEFT JOIN ad_names 
    ON ad_names._ads_id = FB_ads.ad_id
  LEFT JOIN ad_adsets 
    ON ad_adsets.id = FB_ads.adset_id
  LEFT JOIN ad_campaign 
    ON ad_campaign.id = FB_ads.campaign_id
  LEFT JOIN airtable_ads 
    ON FB_ads.ads_name = airtable_ads._advariation 
    OR FB_ads.ad_id = CAST(airtable_ads._adid AS STRING)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY             
      ad_id,
      FB_ads.adset_id,
      campaign_id,
      FB_ads.ads_name,
      adset_name,
      campaign_name,
      date_start,
      reach,
      impressions,
      _clicks,
      inline_link_clicks,
      unique_inline_link_clicks
  ORDER BY date_start, CAST(conversions AS INT64) DESC) = 1
),
total_ads_per_campaign AS (
  SELECT
    *,
    COUNT(ad_id) OVER (PARTITION BY date_start, campaign_name) AS ads_per_campaign
  FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
  SELECT
    *,
    SAFE_DIVIDE(lifetime_budget, ads_per_campaign) AS dailyBudget_per_ad,
    SAFE_DIVIDE(budget_remaining, ads_per_campaign) AS dailybudget_remaining_per_ad
  FROM total_ads_per_campaign
)
SELECT 
  * 
FROM daily_budget_per_ad_per_campaign;