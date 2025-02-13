--Linkedin ads performance
TRUNCATE TABLE `brp.linkedin_ads_performance`;
INSERT INTO `brp.linkedin_ads_performance` (
  _status,
  _advariation,
  _content,
  _screenshot,
  _campaignid,
  _campaignobjective,
  _reportinggroup,
  _campaign,
  _source,
  _medium,
  _id,
  _adtype,
  _livedate,
  _platform,
  _landingpageurl,
  start_year,
  start_month,
  start_day,
  end_month,
  end_year,
  end_day,
  last_start_day,
  start_week,
  start_quater,
  start_month_num,
  weekday,
  start_month_name,
  start_week_num,
  _date,
  _estdate,
  _quater_startdate,
  creative_id,
  _startDate,
  _endDate,
  _leads,
  _reach,
  _spent,
  _impressions,
  _clicks,
  _conversions,
  _landing_pages_clicks,
  _video_views,
  _lead_form_opens,
  _video_play,
  _video_views_25percent,
  _video_views_50percent,
  _video_views_75percent,
  _video_completions,
  account_id,
  campaignID,
  _campaignNames,
  groupID,
  _groupName,
  dailyBudget,
  cost_type,
  status,
  ads_per_campaign,
  dailyBudget_per_ad
)
WITH LI_ads AS (
 SELECT
    date_range.start.year AS start_year, 
    date_range.start.month AS start_month, 
    date_range.start.day AS start_day,
    date_range.end.month AS end_month,
    date_range.end.year AS end_year, 
    date_range.end.day AS end_day,
    LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS last_start_day,
    TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS start_week,
    TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS start_quater,
    TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS start_month_num,
    FORMAT_DATETIME('%A', start_at) AS weekday,
    FORMAT_DATE('%B', start_at) AS start_month_name,
    EXTRACT(WEEK FROM start_at) AS start_week_num,
    EXTRACT(DATE FROM start_at) AS _date,
    DATETIME (start_at, "America/New_York") AS _estdate,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    card_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `brp_linkedin_ads.ad_analytics_by_creative` 
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `brp_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id,
  FROM `brp_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM `brp_linkedin_ads.campaign_groups`
),
airtable_ads AS (
  SELECT 
    * EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
  FROM `x-marketing.brp_mysql.db_airtable_ads` --no new airtable update
), 
_all AS (
  SELECT
    airtable_ads.* EXCEPT(_adid), 
    LI_ads.*,
    campaigns.account_id,
    campaigns.campaignID,
    campaigns._campaignNames,
    campaign_group.groupID,
    campaign_group._groupName,
    campaigns.dailyBudget,
    campaigns.cost_type,
    campaign_group.status
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads.creative_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
  LEFT JOIN airtable_ads 
    ON CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING)
), 
total_ads AS (
  SELECT 
    *, 
  COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
  FROM _all
)
SELECT 
  *,
  SAFE_DIVIDE(dailyBudget, ads_per_campaign) AS dailyBudget_per_ad
FROM total_ads;

-- Google Search Campaign Performance
TRUNCATE TABLE `brp.google_search_campaign_performance`;
INSERT INTO `brp.google_search_campaign_performance` (
  campaign_id,
  campaign_name,
  day,
  company_name,
  currency,
  budget,
  campaign_advertising_channel_type,
  status,
  search_rank_lost_absolute_top_impression_share,
  search_impression_share,
  search_rank_lost_top_impression_share,
  cost,
  impressions,
  clicks,
  conversions,
  view_through_conv,
  all_conversions,
  ctr,
  avg_cpc,
  abs_top_impr,
  conv_rate
)
WITH unique_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    date AS day, 
    customer_descriptive_name AS company_name,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget, 
    cost_micros/1000000 AS cost, 
    search_rank_lost_absolute_top_impression_share,
    search_impression_share,
    search_rank_lost_top_impression_share,
    impressions, 
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END search_impressions,
    clicks, 
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions, 
    view_through_conversions AS view_through_conv,
    all_conversions AS all_conversions,
    campaign_advertising_channel_type AS campaign_advertising_channel_type,
    campaign_status AS status
  FROM `x-marketing.brp_google_ads.campaign_performance_report` report
  WHERE campaign_advertising_channel_type = 'SEARCH'
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id
    ORDER BY report._sdc_received_at DESC) = 1
),
aggregate_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    day,
    company_name,
    currency,
    budget,
    campaign_advertising_channel_type,
    status,
    SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
    SUM(search_impression_share) AS search_impression_share,
    SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share ,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv,
    SUM(all_conversions) AS all_conversions
  FROM unique_rows
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    SAFE_DIVIDE(clicks, impressions) AS ctr,
    SAFE_DIVIDE(cost, clicks) AS avg_cpc,
    SAFE_DIVIDE(abs_top_impr_value, search_impressions) AS abs_top_impr,
    SAFE_DIVIDE(conversions, clicks) AS conv_rate
  FROM aggregate_rows
)
SELECT 
  add_calculated_columns.*,
FROM add_calculated_columns
ORDER BY day, campaign_id;

-- Google Search Ads Variation Performance

TRUNCATE TABLE `brp.google_search_adsvariation_performance`;
INSERT INTO `brp.google_search_adsvariation_performance` (
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  company_name,
  day,
  ad_id,
  headlines,
  final_urls,
  currency,
  cost,
  impressions,
  clicks,
  conversions,
  view_through_conv,
  ctr,
  avg_cpc,
  abs_top_impr,
  conv_rate
)
WITH unique_rows AS (
  SELECT 
    campaign_id,
    campaign_name, 
    ad_group_id, 
    ad_group_name, 
    customer_descriptive_name AS company_name,
    date AS day, 
    id AS ad_id, 
    CASE 
      WHEN type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
    END AS headlines,
    final_urls,
    customer_currency_code AS currency, 
    cost_micros/1000000 AS cost, 
    impressions, 
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END search_impressions,
    clicks, 
    absolute_top_impression_percentage * impressions AS abs_top_impr, 
    conversions, 
    view_through_conversions AS view_through_conv
  FROM `x-marketing.brp_google_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id, ad_group_id, id
    ORDER BY ads._sdc_received_at DESC) = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name, 
    ad_group_id, 
    ad_group_name, 
    company_name,
    day,
    ad_id,
    headlines,
    final_urls,
    currency,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM unique_rows
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    SAFE_DIVIDE(clicks, impressions) AS ctr,
    SAFE_DIVIDE(cost, clicks) AS avg_cpc,
    SAFE_DIVIDE(abs_top_impr_value, search_impressions) AS abs_top_impr,
    SAFE_DIVIDE(conversions, clicks) AS conv_rate
  FROM aggregate_rows
)
SELECT 
  add_calculated_columns.*,
FROM add_calculated_columns
ORDER BY day, campaign_id, ad_group_id, ad_id;


-- Google Search Keyword Performance
TRUNCATE TABLE `x-marketing.brp.google_search_keyword_performance`;
INSERT INTO `x-marketing.brp.google_search_keyword_performance` (
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  match_type,
  keyword,
  quality_score,
  company_name,
  day,
  currency,
  search_rank_lost_absolute_top_impression_share,
  search_impression_share,
  search_rank_lost_top_impression_share,
  cost,
  impressions,
  clicks,
  conversions,
  view_through_conv,
  ctr,
  avg_cpc,
  abs_top_impr,
  conv_rate
)
WITH unique_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    ad_group_id, 
    ad_group_name, 
    ad_group_criterion_keyword.match_type AS match_type, 
    ad_group_criterion_keyword.text AS keyword, 
    NULL AS quality_score,
    customer_descriptive_name AS company_name,
    date AS day, 
    customer_currency_code AS currency, 
    cost_micros/1000000 AS cost, 
    impressions, 
    CASE
      WHEN ad_network_type = 'SEARCH'
      THEN impressions
      ELSE NULL
    END search_impressions,
    clicks, 
    absolute_top_impression_percentage * impressions AS abs_top_impr, 
    conversions, 
    view_through_conversions AS view_through_conv,
    search_rank_lost_absolute_top_impression_share,
    search_impression_share,
    search_rank_lost_top_impression_share    
  FROM `x-marketing.brp_google_ads.keywords_performance_report` keywords
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
    ORDER BY keywords._sdc_received_at DESC) = 1
),
aggregate_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    ad_group_id, 
    ad_group_name, 
    match_type, 
    keyword, 
    quality_score,
    company_name,
    day,
    currency,
    SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
    SUM(search_impression_share) AS search_impression_share,
    SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share ,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM unique_rows
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    SAFE_DIVIDE(clicks, impressions) AS ctr,
    SAFE_DIVIDE(cost, clicks) AS avg_cpc,
    SAFE_DIVIDE(abs_top_impr_value, search_impressions) AS abs_top_impr,
    SAFE_DIVIDE(conversions, clicks) AS conv_rate
  FROM aggregate_rows
)
SELECT 
  add_calculated_columns.*,
FROM add_calculated_columns
ORDER BY day, campaign_id, ad_group_id, keyword;



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


-- ABX BRP LinkedIn
CREATE OR REPLACE TABLE `brp.abx_linkedin_ads_performance` AS
WITH LI_ads AS (
 SELECT
    date_range.start.year AS start_year, 
    date_range.start.month AS start_month, 
    date_range.start.day AS start_day,
    date_range.end.month AS end_month,
    date_range.end.year AS end_year, 
    date_range.end.day AS end_day,
    LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS last_start_day,
    TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS start_week,
    TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS start_quater,
    TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS start_month_num,
    FORMAT_DATETIME('%A', start_at) AS weekday,
    FORMAT_DATE('%B', start_at) AS start_month_name,
    EXTRACT(WEEK FROM start_at) AS start_week_num,
    EXTRACT(DATE FROM start_at) AS _date,
    DATETIME (start_at, "America/New_York") AS _estdate,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    card_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `brp_abx_linkedin_ads_v2.ad_analytics_by_creative` 
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id,
    intended_status
  FROM `brp_abx_linkedin_ads_v2.creatives`
),
campaigns AS (
  SELECT
    campaign.id AS campaignID,
    campaign.name AS _campaignNames,
    campaign.status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    acc.name AS account_name,
    account_id,
  FROM `brp_abx_linkedin_ads_v2.campaigns` campaign
  LEFT JOIN `x-marketing.brp_abx_linkedin_ads_v2.accounts` acc 
    ON acc.id = account_id
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM `brp_abx_linkedin_ads_v2.campaign_groups`   
),
airtable_ads AS (
  SELECT 
    * EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
  FROM `x-marketing.brp_mysql.optimization_airtable_ads_linkedin`
), 
_all AS (
  SELECT
    airtable_ads.* EXCEPT(_adid, _campaignid), 
    LI_ads.*,
    ads_title.intended_status AS _ads_status,
    campaigns.account_id,
    campaigns.account_name,
    campaigns.campaignID,
    campaigns._campaignNames,
    campaign_group.groupID,
    campaign_group._groupName,
    campaigns.dailyBudget,
    campaigns.cost_type,
    campaigns.status AS _campaign_status
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads.creative_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
  JOIN airtable_ads 
    ON CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING)
), 
total_ads AS (
  SELECT 
    *, 
    COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
  FROM _all
), 
daily_budget_per_ad_per_campaign AS (
  SELECT 
    *,
    SAFE_DIVIDE(dailyBudget, ads_per_campaign) AS dailyBudget_per_ad
  FROM total_ads
) 
SELECT 
  * 
FROM daily_budget_per_ad_per_campaign;


--Paid Media LinkedIn
CREATE OR REPLACE TABLE `brp.paid_media_linkedin_ads_performance` AS
WITH LI_ads AS (
 SELECT
    date_range.start.year AS start_year, 
    date_range.start.month AS start_month, 
    date_range.start.day AS start_day,
    date_range.end.month AS end_month,
    date_range.end.year AS end_year, 
    date_range.end.day AS end_day,
    LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS last_start_day,
    TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS start_week,
    TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS start_quater,
    TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS start_month_num,
    FORMAT_DATETIME('%A', start_at) AS weekday,
    FORMAT_DATE('%B', start_at) AS start_month_name,
    EXTRACT(WEEK FROM start_at) AS start_week_num,
    EXTRACT(DATE FROM start_at) AS _date,
    DATETIME (start_at, "America/New_York") AS _estdate,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    card_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `brp_abx_linkedin_ads_v2.ad_analytics_by_creative` 
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id,
    intended_status
  FROM `brp_abx_linkedin_ads_v2.creatives`
),
campaigns AS (
  SELECT
    campaign.id AS campaignID,
    campaign.name AS _campaignNames,
    campaign.status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    acc.name AS account_name,
    account_id,
  FROM `brp_abx_linkedin_ads_v2.campaigns` campaign
  LEFT JOIN `x-marketing.brp_abx_linkedin_ads_v2.accounts` acc 
    ON acc.id = account_id
  WHERE account_id = 507653713
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM `brp_abx_linkedin_ads_v2.campaign_groups`
), 
_all AS (
  SELECT 
    LI_ads.*,
    ads_title.intended_status AS _ads_status,
    campaigns.account_id,
    campaigns.account_name,
    campaigns.campaignID,
    campaigns._campaignNames,
    campaign_group.groupID,
    campaign_group._groupName,
    campaigns.dailyBudget,
    campaigns.cost_type,
    campaigns.status AS _campaign_status
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads.creative_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
), 
total_ads AS (
  SELECT 
    *, 
    COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
  FROM _all
), 
daily_budget_per_ad_per_campaign AS (
  SELECT 
    *,
    SAFE_DIVIDE(dailyBudget, ads_per_campaign) AS dailyBudget_per_ad
  FROM total_ads
) 
SELECT 
  * 
FROM daily_budget_per_ad_per_campaign
WHERE account_id = 507653713;