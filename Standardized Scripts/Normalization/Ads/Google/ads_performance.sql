-- Google Search Campaign Performance
CREATE OR REPLACE TABLE corcentric.google_search_campaign_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
    FROM (
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
                WHEN ad_network_type = 'SEARCH'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr,
            conversions, 
            view_through_conversions AS view_through_conv,
            all_conversions AS all_conversions,
            campaign_advertising_channel_type AS campaign_advertising_channel_type,
            campaign_status AS status,
            RANK() OVER(
                PARTITION BY date, campaign_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.corcentric_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'SEARCH'
    )
    WHERE _rank = 1
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
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
FROM add_calculated_columns
ORDER BY day, campaign_id;


-- Google Search Ads Variation Performance
CREATE OR REPLACE TABLE corcentric.google_search_adsvariation_performance AS

WITH unique_rows AS (
    SELECT * EXCEPT(_rank)
    FROM (
        SELECT 
            campaign_id,
            campaign_name, 
            ad_group_id, 
            ad_group_name, 
            customer_descriptive_name AS company_name,
            date AS day, 
            id AS ad_id, 
            CASE 
                WHEN type = 'RESPONSIVE_SEARCH_AD'
                THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
            END AS headlines,
            --responsive_search_ad.headlines AS headlines,
            final_urls,
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
            RANK() OVER(
                PARTITION BY date, campaign_id, ad_group_id, id
                ORDER BY ads._sdc_received_at DESC
            ) _rank
        FROM `x-marketing.corcentric_google_ads.ad_performance_report` ads
        
        -- WHERE campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
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
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
FROM add_calculated_columns
ORDER BY day, campaign_id, ad_group_id, ad_id;


-- Google Search Keyword Performance
CREATE OR REPLACE TABLE corcentric.google_search_keyword_performance AS

WITH unique_rows AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
            campaign_id, 
            campaign_name, 
            ad_group_id, 
            ad_group_name, 
            ad_group_criterion_keyword.match_type AS match_type, 
            ad_group_criterion_keyword.text AS keyword, 
            /*ad_group_criterion_quality_info.quality_score*/ NULL AS quality_score,
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
            search_rank_lost_top_impression_share,
            RANK() OVER (
                PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
                ORDER BY keywords._sdc_received_at DESC
            ) AS _rank           
        FROM `x-marketing.corcentric_google_ads.keywords_performance_report` keywords
        -- WHERE campaign_name LIKE 'US | %'
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
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
FROM add_calculated_columns
ORDER BY day, campaign_id, ad_group_id, keyword;


-- Google Search Query Performance
CREATE OR REPLACE TABLE corcentric.google_search_query_performance AS

WITH unique_rows AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
        campaign_id, 
        campaign_name, 
        ad_group_id,
        ad_group_name, 
        keyword.info.text AS keyword, 
        --search_term_match_type AS match_type, 
        search_term_view_search_term AS search_term,
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
        RANK() OVER( 
            PARTITION BY date, campaign_id, ad_group_id, keyword.info.text, search_term_view_search_term
            ORDER BY _sdc_received_at DESC
        ) AS _rank
        FROM `x-marketing.corcentric_google_ads.search_query_performance_report` query
        -- WHERE campaign_name LIKE 'US | %'
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
        --match_type, 
        search_term,
        company_name,
        day,
        currency,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
add_calculated_columns AS (
    SELECT
        * EXCEPT(search_impressions, abs_top_impr_value),
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
FROM add_calculated_columns
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;


--Google Display
CREATE OR REPLACE TABLE corcentric.google_display_campaign_performance AS

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
      `corcentric_google_ads.campaign_performance_report` report
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
) /*,
airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.sandler_mysql.db_airtable_google_display_ads` 
) */
SELECT add_calculated_columns.*
--airtable_ads.* EXCEPT(_campaignid,_campaignname)
FROM add_calculated_columns
--JOIN airtable_ads ON CAST(add_calculated_columns.campaign_id AS STRING) = airtable_ads._campaignid
ORDER BY day, campaign_id;