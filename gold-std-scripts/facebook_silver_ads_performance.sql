TRUNCATE TABLE `x-marketing.brp.facebook_ads_performance`;
INSERT INTO `x-marketing.brp.facebook_ads_performance` (
  _ad_id,
  _ad_set_id,
  _campaign_id,
  _ads_name,
  _ad_set_name,
  _campaign_name,
  _date_start,
  _reach,
  _spend,
  _impressions,
  _clicks,
  _inline_link_clicks,
  _unique_inline_link_clicks,
  _ctr,
  _cost_per_unique_click,
  _cpp,
  _cpc,
  _conversions,
  _bid_type,
  _status_ads,
  _body,
  _title_name,
  _url_tags,
  _ad_status,
  _title,
  _thumbnail_url,
  _instagram_actor_id,
  _image_url,
  _created_time,
  _lifetime_budget,
  _budget_remaining,
  _end_time,
  _objective,
  _buying_type,
  _effective_status,
  _updated_time,
  _creative_id,
  _ad_id_airtable,
  _status,
  _ad_variation,
  _screenshot,
  _airtable_id,
  _reporting_group,
  _campaign_name_airtable,
  _id,
  _adtype,
  _live_date,
  _platform,
  _landing_page,
  _ad_name,
  _ads_per_campaign,
  _daily_budget_per_ad,
  _daily_budget_remaining_per_ad
)
WITH FB_ads AS (
  SELECT
    ad_id AS _ad_id,
    adset_id AS _ad_set_id,
    campaign_id AS _campaign_id,
    ad_name as _ads_name,
    adset_name AS _ad_set_name,
    campaign_name AS _campaign_name,
    date_start AS _date_start,
    reach AS _reach,
    spend AS _spend,
    impressions AS _impressions,
    clicks AS _clicks,
    inline_link_clicks AS _inline_link_clicks,
    unique_inline_link_clicks AS _unique_inline_link_clicks,
    ctr AS _ctr,
    cost_per_unique_click AS _cost_per_unique_click,
    cpp AS _cpp, 
    cpc AS _cpc, 
    (
      SELECT 
        action.value.value 
      FROM UNNEST(actions) AS action 
      WHERE action.value.action_type = 'lead'
    ) AS _conversions
  FROM `x-marketing.brp_rogersgray_facebook_ads.ads_insights` 
),
ad_names AS (
  SELECT 
    adset_id AS _adset_id, 
    k.id AS _ads_id, 
    k.name AS _ads_name, 
    bid_type AS _bid_type, 
    k.status AS _status_ads, 
    body AS _body, 
    l.name AS _title_name, 
    url_tags AS _url_tags, 
    l.status AS _ad_status, 
    title AS _title, 
    thumbnail_url AS _thumbnail_url, 
    instagram_actor_id AS _instagram_actor_id, 
    image_url AS _image_url,
    creative.id AS _creative_id
  FROM `x-marketing.brp_rogersgray_facebook_ads.ads` k
  LEFT JOIN `x-marketing.brp_rogersgray_facebook_ads.adcreative` l 
    ON creative.id = l.id  
), 
ad_adsets AS (
  SELECT 
    id AS _id, 
    name AS _name, 
    created_time AS _created_time, 
    lifetime_budget AS _lifetime_budget,
    budget_remaining AS _budget_remaining, 
    end_time AS _end_time 
  FROM `x-marketing.brp_rogersgray_facebook_ads.adsets`
),
ad_campaign AS (
  SELECT 
    id AS _id, 
    name AS _name, 
    objective AS _objective, 
    buying_type AS _buying_type, 
    effective_status AS _effective_status, 
    updated_time AS _updated_time 
  FROM `x-marketing.brp_rogersgray_facebook_ads.campaigns`
), 
airtable_ads AS (
  SELECT 
    -- * EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version)
    _adid AS _ad_id_airtable,
    _status,
    _advariation AS _ad_variation,
    _screenshot,
    _airtableid AS _airtable_id,
    _reportinggroup AS _reporting_group,
    _campaignname AS _campaign_name_airtable,
    _id,
    _adtype AS _adtype,
    _livedate AS _live_date,
    _platform,
    _landingpage _landing_page,
    _adname AS  _ad_name
  FROM `x-marketing.brp_mysql.optimization_airtable_ads_facebook` -- no update on the new airtable migration
),
combine_all AS (
  SELECT 
    FB_ads.*,
    ad_names._bid_type, 
    ad_names._status_ads, 
    ad_names._body, 
    ad_names._title_name, 
    ad_names._url_tags, 
    ad_names._ad_status, 
    ad_names._title, 
    ad_names._thumbnail_url, 
    ad_names._instagram_actor_id, 
    ad_names._image_url,
    ad_adsets._created_time, 
    ad_adsets._lifetime_budget,
    ad_adsets._budget_remaining, 
    ad_adsets._end_time,
    ad_campaign._objective, 
    ad_campaign._buying_type, 
    ad_campaign._effective_status, 
    ad_campaign._updated_time,
    CAST(ad_names._creative_id AS INT64) AS _creative_id,
    airtable_ads.*
  FROM FB_ads
  LEFT JOIN ad_names 
    ON ad_names._ads_id = FB_ads._ad_id
  LEFT JOIN ad_adsets 
    ON ad_adsets._id = FB_ads._ad_set_id
  LEFT JOIN ad_campaign 
    ON ad_campaign._id = FB_ads._campaign_id
  LEFT JOIN airtable_ads 
    ON FB_ads._ads_name = airtable_ads._ad_variation 
    OR FB_ads._ad_id = CAST(airtable_ads._ad_id_airtable AS STRING)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY             
      FB_ads._ad_id,
      FB_ads._ad_set_id,
      _campaign_id,
      FB_ads._ads_name,
      _ad_set_name,
      FB_ads._campaign_name,
      _date_start,
      _reach,
      _impressions,
      _clicks,
      _inline_link_clicks,
      _unique_inline_link_clicks
  ORDER BY _date_start, CAST(_conversions AS INT64) DESC) = 1
),
total_ads_per_campaign AS (
  SELECT
    *,
    COUNT(_ad_id) OVER (PARTITION BY _date_start, _campaign_name) AS _ads_per_campaign
  FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
  SELECT
    *,
    SAFE_DIVIDE(_lifetime_budget, _ads_per_campaign) AS _daily_budget_per_ad,
    SAFE_DIVIDE(_budget_remaining, _ads_per_campaign) AS _daily_budget_remaining_per_ad
  FROM total_ads_per_campaign
)
SELECT 
  * 
FROM daily_budget_per_ad_per_campaign;