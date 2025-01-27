--Linkedin ads performance
CREATE OR REPLACE TABLE `x-marketing.corcentric.linkedin_ads_performance` AS

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
    FROM `corcentric_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `corcentric_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    status, --not used in main query// use status from campaign group
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM
    `corcentric_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM
    `corcentric_linkedin_ads.campaign_groups`
),
/* airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.sandler_mysql.optimization_airtable_ads_linkedin` 
), */
_all AS (
  SELECT
--airtable_ads.*EXCEPT(_adid), 
  LI_ads.*,
  campaigns.account_id,
  campaigns.campaignID,
  campaigns._campaignNames,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaign_group.status
FROM
  LI_ads
RIGHT JOIN
  ads_title
ON
  CAST(LI_ads.creative_id AS STRING) = ads_title.cID
LEFT JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
LEFT JOIN
  campaign_group
ON
  campaigns.campaign_group_id = campaign_group.groupID

/* LEFT JOIN airtable_ads 
ON 
CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING) */
),
total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignNames ) AS ads_per_campaign
  FROM _all
),
daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
      AS dailyBudget_per_ad
  FROM total_ads
)
SELECT * FROM daily_budget_per_ad_per_campaign;


-- Google Search Campaign Performance
CREATE OR REPLACE TABLE `x-marketing.corcentric.google_search_campaign_performance` AS

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
            campaign_status AS status
        FROM `x-marketing.corcentric_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'SEARCH'
        QUALIFY RANK() OVER(
                PARTITION BY date, campaign_id
                ORDER BY report._sdc_received_at DESC
            ) = 1
        -- AND campaign_name LIKE 'US | %'
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
CREATE OR REPLACE TABLE `x-marketing.corcentric.google_search_adsvariation_performance` AS

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
            
        FROM `x-marketing.corcentric_google_ads.ad_performance_report` ads
        -- WHERE campaign_name LIKE 'US | %'
        QUALIFY RANK() OVER(
                PARTITION BY date, campaign_id, ad_group_id, id
                ORDER BY ads._sdc_received_at DESC
            ) = 1
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
CREATE OR REPLACE TABLE `x-marketing.corcentric.google_search_keyword_performance` AS

WITH unique_rows AS (
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
                     
        FROM `x-marketing.corcentric_google_ads.keywords_performance_report` keywords
        -- WHERE campaign_name LIKE 'US | %'
        QUALIFY RANK() OVER (
                PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
                ORDER BY keywords._sdc_received_at DESC
            ) = 1 
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
CREATE OR REPLACE TABLE `x-marketing.corcentric.google_search_query_performance` AS

WITH unique_rows AS (
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
        
        FROM `x-marketing.corcentric_google_ads.search_query_performance_report` query
        -- WHERE campaign_name LIKE 'US | %'
        QUALIFY RANK() OVER( 
            PARTITION BY date, campaign_id, ad_group_id, keyword/*.info.text*/, search_term_view_search_term
            ORDER BY _sdc_received_at DESC
        ) = 1
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
CREATE OR REPLACE TABLE `x-marketing.corcentric.google_display_campaign_performance` AS

WITH unique_rows AS (
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
      campaign_status
    FROM 
      `corcentric_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'DISPLAY'
      QUALIFY RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) = 1
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