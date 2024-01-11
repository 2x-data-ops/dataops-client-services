CREATE OR REPLACE TABLE fastmarkets.linkedin_ads_performance AS

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
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    approximate_unique_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions
  FROM
    `fastmarkets_linkedin_ads.ad_analytics_by_creative`
  --WHERE CAST(start_at AS DATE) >= '2023-11-14' AND CAST(start_at AS DATE) <= '2023-11-20'
  --AND creative_id = 316827574
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS cID,
    campaign_id,
    account_id,
    acc.name AS account_name
  FROM
    `fastmarkets_linkedin_ads.creatives` c
  LEFT JOIN `x-marketing.fastmarkets_linkedin_ads.accounts` acc ON acc.id = account_id
   --WHERE campaign_id = 'NewGen_find out more'
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignName,
    status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
  FROM
    `fastmarkets_linkedin_ads.campaigns`
   --where ID = 208141506
    
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM
    `fastmarkets_linkedin_ads.campaign_groups`
    --where ID = 623055756
), _all AS (
SELECT
  LI_ads.*,
  campaigns.campaignID,
  campaigns._campaignName,
  campaigns.status AS campaign_status,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaign_group.status,
  ads_title.account_id,
  ads_title.account_name
FROM
  LI_ads
RIGHT JOIN
 ads_title
ON
CAST( LI_ads.creative_id AS STRING) = ads_title.cID
LEFT JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
LEFT JOIN
  campaign_group
ON
  campaigns.campaign_group_id = campaign_group.groupID
),
 airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at,_campaignid),
    CASE WHEN _campaignid = '272438124 ' THEN '272438124' ELSE _campaignid END AS _campaignid
    FROM `x-marketing.fastmarkets_mysql.fastmarkets_db_airtable_ads` 
), total_ads AS (
  SELECT *, 
  count(creative_id) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
  FROM _all
)
, daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
      AS dailyBudget_per_ad
  FROM total_ads
) SELECT daily_budget_per_ad_per_campaign.* EXCEPT (_campaignname) ,
airtable_ads._adname,
airtable_ads._commodity,
airtable_ads._campaignname
FROM daily_budget_per_ad_per_campaign
JOIN airtable_ads ON CAST(creative_id AS STRING) = _adid AND CAST(campaignID AS STRING) = _campaignid;
--where account_id = 500918077;


-- Google Search Campaign Performance

CREATE OR REPLACE TABLE fastmarkets.google_search_campaign_performance AS
-- Google Search Campaign (New)
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      date AS day,
      week,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
      impressions,
      campaign_advertising_channel_type,
      CASE
        WHEN ad_network_type = 'SEARCH' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      campaign_status,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank,
      CASE WHEN campaign_base_campaign LIKE '%7848293272%' THEN 7848293272
      WHEN campaign_base_campaign LIKE '%1650715709%' THEN 1650715709
      WHEN campaign_base_campaign LIKE '%5046870203%' THEN 5046870203
      WHEN campaign_base_campaign LIKE '%5110172709%' THEN 5110172709 END AS account_id,
      descriptive_name
    FROM
      `fastmarkets_google_ads.campaign_performance_report` report
    LEFT JOIN `fastmarkets_google_ads.accounts` acc ON CASE WHEN campaign_base_campaign LIKE '%7848293272%' THEN 7848293272
      WHEN campaign_base_campaign LIKE '%1650715709%' THEN 1650715709
      WHEN campaign_base_campaign LIKE '%5046870203%' THEN 5046870203
      WHEN campaign_base_campaign LIKE '%5110172709%' THEN 5110172709 END = acc.id
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    campaign_advertising_channel_type,
    day,
    week,
    currency,
    budget,
    campaign_status,
    account_id,
    descriptive_name,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6,7,8,9,10
),
add_calculated_columns AS (
  SELECT
    * EXCEPT (search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM 
    aggregate_rows
)
SELECT
  add_calculated_columns.*,
FROM
  add_calculated_columns
WHERE 
  campaign_name LIKE '2X%'
--campaign_advertising_channel_type = 'SEARCH'
ORDER BY
  day, campaign_id;


-- Google Search Ads Variation Performance

CREATE OR REPLACE TABLE fastmarkets.google_search_adsvariation_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      ad_group_id,
      ad_group_name,
      customer_descriptive_name,
      date AS day,
      id AS ad_id,
      CASE
        WHEN type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
        END AS headlines,
      TRIM(final_urls, "[']") AS final_urls,
      customer_currency_code AS currency,
      cost_micros/1000000 AS cost,
      impressions,
      CASE
        WHEN ad_network_type = 'SEARCH' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      ad_group_status,
      RANK() OVER(
        PARTITION BY date, campaign_id, ad_group_id, id
        ORDER BY ads._sdc_received_at DESC
      ) AS _rank
    FROM 
      `fastmarkets_google_ads.ad_performance_report` ads  
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    customer_descriptive_name,
    day,
    ad_id,
    headlines,
    final_urls,
    currency,
    ad_group_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9,10,11
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM aggregate_rows
)
SELECT 
  add_calculated_columns.*
FROM
  add_calculated_columns
ORDER BY
  day, campaign_id, ad_group_id, ad_id;


-- Google Seach Keyword Performance

CREATE OR REPLACE TABLE fastmarkets.google_search_keyword_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      ad_group_id,
      ad_group_name,
      ad_network_type,
      customer_descriptive_name,
      ad_group_criterion_keyword.match_type AS match_type,
      ad_group_criterion_keyword.text AS keyword,
      ad_group_criterion_quality_info.quality_score AS quality_score,
      date AS day,
      customer_currency_code AS currency,
      cost_micros/1000000 AS cost,
      impressions,
      CASE
        WHEN ad_network_type = 'SEARCH' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      ad_group_criterion_status,
      RANK() OVER (
        PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
        ORDER BY keywords._sdc_received_at DESC
      ) AS _rank
    FROM 
      `fastmarkets_google_ads.keywords_performance_report` keywords
  )
  WHERE _rank = 1
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
    ad_network_type,
    customer_descriptive_name,
    day,
    currency,
    ad_group_criterion_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM
    aggregate_rows
)
SELECT
  add_calculated_columns.*
FROM
  add_calculated_columns
ORDER BY
  day, campaign_id, ad_group_id, keyword;


-- Google Search Query Performance

CREATE OR REPLACE TABLE fastmarkets.google_search_query_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
      SELECT
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        keyword.info.text AS keyword,
        -- search_term_match_type AS match_type,
        search_term_view_search_term AS search_term,
        date AS day,
        customer_currency_code AS currency,
        cost_micros/1000000 AS cost,
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
        ad_group_status,
        RANK() OVER(
          PARTITION BY date, campaign_id, ad_group_id, keyword.info.text, search_term_view_search_term
          ORDER BY _sdc_received_at DESC
        ) AS _rank
      FROM `fastmarkets_google_ads.search_query_performance_report`
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    keyword,
    -- match_type,
    search_term,
    day,
    currency,
            campaign_status,
        ad_group_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8,9,10
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    CASE 
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM
    aggregate_rows
)
SELECT 
  add_calculated_columns.*
FROM
  add_calculated_columns
ORDER BY
  day, campaign_id, ad_group_id, keyword, search_term;


-- Google Display Campaign Performance

CREATE OR REPLACE TABLE fastmarkets.google_display_campaign_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      date AS day,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
      impressions,
      CASE
        WHEN ad_network_type = 'DISPLAY' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      campaign_status,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM 
      `fastmarkets_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'DISPLAY'
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    day,
    currency,
    budget,
    campaign_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM
    aggregate_rows
)
SELECT
  add_calculated_columns.*
FROM
  add_calculated_columns
ORDER BY
  day, campaign_id;




-- Ad group Performance

CREATE OR REPLACE TABLE fastmarkets.google_ad_group_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      ad_group_id,
      ad_group_name,
      ad_group_type,
      customer_descriptive_name,
      date AS day,
      customer_currency_code AS currency,
      --campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
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
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank,
    FROM
      `fastmarkets_google_ads.ad_group_performance_report` report
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    ad_group_id,
    ad_group_name,
    ad_group_type,
    customer_descriptive_name,
    day,
    currency,
    --budget,
    campaign_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6,7
),
add_calculated_columns AS (
  SELECT
    * EXCEPT (search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM 
    aggregate_rows
)
SELECT
  add_calculated_columns.*,
FROM
  add_calculated_columns
ORDER BY
  day, ad_group_id;