/* LinkedIn Ad Performance */
-- CREATE OR REPLACE TABLE `x-marketing.equiteq.linkedin_ads_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.linkedin_ads_performance`;

INSERT INTO `x-marketing.equiteq.linkedin_ads_performance` (
  li_creative_id,
  li_campaign_id,
  li_campaign_name,
  li_campaign_type,
  li_campaign_objective,
  li_campaign_status,
  li_daily_budget,
  li_campaign_start_date,
  li_campaign_end_date,
  li_campaign_group_id,
  li_campaign_group_name,
  li_campaign_group_status,
  li_run_date,
  li_end_date,
  li_leads,
  li_spent,
  li_impressions,
  li_clicks,
  li_conversions,
  li_website_visits,
  fm_ads_per_campaign,
  fm_daily_budget_per_ad
)
WITH LI_ads AS (
  SELECT
    creative_id AS li_creative_id,
    start_at AS li_run_date,
    end_at AS li_end_date,
    one_click_leads AS li_leads,
    cost_in_usd AS li_spent,
    impressions AS li_impressions,
    clicks AS li_clicks,
    external_website_conversions AS li_conversions,
    landing_page_clicks AS li_website_visits,
  FROM `x-marketing.equiteq_linkedin_ads.ad_analytics_by_creative`
),
ads_title AS (
  SELECT
    REGEXP_EXTRACT(id, r'[\d]+') AS li_creative_id,
    campaign_id AS li_campaign_id
  FROM `x-marketing.equiteq_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS li_campaign_id,
    name AS li_campaign_name,
    REPLACE(INITCAP(type), '_', ' ') AS li_campaign_type,
    REPLACE(INITCAP(objective_type), '_', ' ') AS li_campaign_objective,
    INITCAP(status) AS li_campaign_status,
    CASE
      WHEN daily_budget.amount IS NOT NULL THEN daily_budget.amount
      ELSE total_budget.amount / TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY)
    END AS li_daily_budget,
    run_schedule.start AS li_campaign_start_date,
    run_schedule.end AS li_campaign_end_date,
    campaign_group_id AS li_campaign_group_id
  FROM `x-marketing.equiteq_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS li_campaign_group_id,
    REPLACE(name, '_', ' ') AS li_campaign_group_name,
    INITCAP(status) AS li_campaign_group_status
  FROM `x-marketing.equiteq_linkedin_ads.campaign_groups`
),
combine_all AS (
  SELECT
    LI_ads.li_creative_id,
    campaigns.* EXCEPT (li_campaign_group_id),
    campaign_group.*,
    LI_ads.* EXCEPT (li_creative_id),
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads.li_creative_id AS STRING) = ads_title.li_creative_id
  JOIN campaigns
    ON ads_title.li_campaign_id = campaigns.li_campaign_id
  JOIN campaign_group
    ON campaigns.li_campaign_group_id = campaign_group.li_campaign_group_id
),
total_ads_per_campaign AS (
  SELECT
    *,
    COUNT(li_creative_id) OVER (
      PARTITION BY
        li_run_date,
        li_campaign_name
    ) AS fm_ads_per_campaign
  FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
  SELECT
    *,
    li_daily_budget / fm_ads_per_campaign AS fm_daily_budget_per_ad,
  FROM total_ads_per_campaign
)
SELECT
  *
FROM daily_budget_per_ad_per_campaign;

/* Google Ads Performance */
-- Google Search Campaign Performance --
--CREATE OR REPLACE TABLE `x-marketing.equiteq.google_search_campaign_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.google_search_campaign_performance`;

INSERT INTO `x-marketing.equiteq.google_search_campaign_performance` (
  campaign_id,
  campaign_name,
  day,
  currency,
  campaign_status,
  customer_time_zone,
  campaign_advertising_channel_type,
  budget,
  cost,
  impressions,
  search_impressions,
  clicks,
  conversions,
  view_through_conv
)
WITH
unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    DATE(date) AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros / 1000000 AS budget,
    cost_micros / 1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    conversions,
    view_through_conversions AS view_through_conv,
    INITCAP(campaign_status) AS campaign_status,
    customer_time_zone,
    INITCAP(campaign_advertising_channel_type) AS campaign_advertising_channel_type
  FROM `x-marketing.equiteq_google_ads.campaign_performance_report`
  QUALIFY RANK() OVER (
    PARTITION BY
      date,
      campaign_id
    ORDER BY
      _sdc_received_at DESC
  ) = 1
)
SELECT
  campaign_id,
  campaign_name,
  day,
  currency,
  campaign_status,
  customer_time_zone,
  campaign_advertising_channel_type,
  budget,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
ORDER BY
  day,
  campaign_id;

-- Google Search Ads Variation Performance --
-- CREATE OR REPLACE TABLE `x-marketing.equiteq.google_search_adsvariation_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.google_search_adsvariation_performance`;

INSERT INTO `x-marketing.equiteq.google_search_adsvariation_performance` (
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  day,
  ad_id,
  headlines,
  final_urls,
  currency,
  ad_group_status,
  customer_time_zone,
  cost,
  impressions,
  search_impressions,
  abs_top_impr_value,
  clicks,
  conversions,
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    DATE(date) AS day,
    id AS ad_id,
    CASE
      WHEN type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(
        ARRAY_TO_STRING(
          REGEXP_EXTRACT_ALL(responsive_search_ad.headlines, "'text': '[^']*"),
          "\n"
        ),
        "'text': '",
        ""
      )
    END AS headlines,
    TRIM(final_urls, "[']") AS final_urls,
    customer_currency_code AS currency,
    cost_micros / 1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    INITCAP(ad_group_status) AS ad_group_status,
    customer_time_zone
  FROM `x-marketing.equiteq_google_ads.ad_performance_report`
  QUALIFY RANK() OVER (
    PARTITION BY
      date,
      campaign_id,
      ad_group_id,
      id
    ORDER BY
      _sdc_received_at DESC
  ) = 1
)
SELECT
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  day,
  ad_id,
  headlines,
  final_urls,
  currency,
  ad_group_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11
ORDER BY
  day,
  campaign_id,
  ad_group_id,
  ad_id;

-- Google Search Keyword Performance --
-- CREATE OR REPLACE TABLE `x-marketing.equiteq.google_search_keyword_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.google_search_keyword_performance`;

INSERT INTO `x-marketing.equiteq.google_search_keyword_performance` (
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  match_type,
  keyword,
  ad_group_criterion_status,
  quality_score,
  day,
  currency,
  customer_time_zone,
  cost,
  impressions,
  search_impressions,
  abs_top_impr_value,
  clicks,
  conversions,
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    INITCAP(ad_group_criterion_keyword.match_type) AS match_type,
    ad_group_criterion_keyword.text AS keyword,
    INITCAP(ad_group_criterion_status) AS ad_group_criterion_status,
    ad_group_criterion_quality_info.quality_score AS quality_score,
    DATE(date) AS day,
    customer_currency_code AS currency,
    cost_micros / 1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    customer_time_zone,
  FROM `x-marketing.equiteq_google_ads.keywords_performance_report`
  QUALIFY RANK() OVER (
    PARTITION BY
      date,
      campaign_id,
      ad_group_id,
      ad_group_criterion_keyword.text
    ORDER BY
      _sdc_received_at DESC
  ) = 1
)
SELECT
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  match_type,
  keyword,
  ad_group_criterion_status,
  quality_score,
  day,
  currency,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11
ORDER BY
  day,
  campaign_id,
  ad_group_id,
  keyword;

-- Google Search Query Performance --
-- CREATE OR REPLACE TABLE `x-marketing.equiteq.google_search_query_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.google_search_query_performance`;

INSERT INTO `x-marketing.equiteq.google_search_query_performance` (
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  keyword,
  search_term,
  day,
  currency,
  campaign_status,
  ad_group_status,
  customer_time_zone,
  cost,
  impressions,
  search_impressions,
  abs_top_impr_value,
  clicks,
  conversions,
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    keyword.info.text AS keyword_text,
    search_term_view_search_term AS search_term,
    DATE(date) AS day,
    customer_currency_code AS currency,
    cost_micros / 1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    INITCAP(ad_group_status) AS ad_group_status,
    customer_time_zone,
  FROM `x-marketing.equiteq_google_ads.search_query_performance_report`
  QUALIFY RANK() OVER (
    PARTITION BY
      date,
      campaign_id,
      ad_group_id,
      keyword.info.text,
      search_term_view_search_term
    ORDER BY
      _sdc_received_at DESC
  ) = 1
)
SELECT
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  keyword_text AS keyword,
  search_term,
  day,
  currency,
  campaign_status,
  ad_group_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11
ORDER BY
  day,
  campaign_id,
  ad_group_id,
  keyword,
  search_term;

-- Google Display Campaign Performance --
-- CREATE OR REPLACE TABLE `x-marketing.equiteq.google_display_campaign_performance` AS
TRUNCATE TABLE `x-marketing.equiteq.google_display_campaign_performance`;

INSERT INTO `x-marketing.equiteq.google_display_campaign_performance` (
    campaign_id,
    campaign_name,
    day,
    currency,
    budget,
    campaign_status,
    customer_time_zone,
    cost,
    impressions,
    active_view_impressions,
    search_impressions,
    abs_top_impr_value,
    clicks,
    conversions,
    view_through_conv
  )
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    DATE(date) AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros / 1000000 AS budget,
    cost_micros / 1000000 AS cost,
    impressions,
    active_view_impressions,
    CASE
      WHEN ad_network_type = 'DISPLAY' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    INITCAP(campaign_status) AS campaign_status,
    customer_time_zone
  FROM `x-marketing.equiteq_google_ads.campaign_performance_report`
  WHERE campaign_advertising_channel_type = 'DISPLAY'
  QUALIFY RANK() OVER (
    PARTITION BY
      date,
      campaign_id
    ORDER BY
      _sdc_received_at DESC
  ) = 1
)
SELECT
  campaign_id,
  campaign_name,
  day,
  currency,
  budget,
  campaign_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(active_view_impressions) AS active_view_impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7
ORDER BY
  day,
  campaign_id;