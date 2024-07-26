--Linkedin ads performance
CREATE OR REPLACE TABLE `brp.linkedin_ads_performance` AS

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
  FROM
    `brp_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `brp_linkedin_ads.creatives`
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
  FROM
    `brp_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM
    `brp_linkedin_ads.campaign_groups`
),
 airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.brp_mysql.db_airtable_ads` 
), _all AS (
SELECT
airtable_ads.*EXCEPT(_adid), 
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

LEFT JOIN airtable_ads 
ON 
CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING)
), total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignNames ) AS ads_per_campaign
  FROM _all
)
, daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
      AS dailyBudget_per_ad
  FROM total_ads
) SELECT * FROM daily_budget_per_ad_per_campaign;

--linkedin social media
CREATE OR REPLACE TABLE brp.linkedin_social_media AS
WITH brp_new_followers AS (
    SELECT PARSE_TIMESTAMP('%m/%d/%Y',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'Baldwin Risk Partners' AS company_name, '507653713' AS _id
    FROM `x-marketing.brp_linkedin_social_media.BRP_New_followers`
),
burnham_new_followers AS (
    SELECT PARSE_TIMESTAMP('%F',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'Burnham' AS company_name, '510471709' AS _id
    FROM `x-marketing.brp_linkedin_social_media.Burnham_New_Followers`
),
guided_solutions_new_followers AS (
    SELECT PARSE_TIMESTAMP('%F',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'Guided Solutions' AS company_name, '' AS _id
    FROM `x-marketing.brp_linkedin_social_media.Guided_Solutions_New_Followers`
),
insgroup_new_followers AS (
    SELECT PARSE_TIMESTAMP('%F',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'Insgroup' AS company_name, '507101587' AS _id
    FROM `x-marketing.brp_linkedin_social_media.Insgroup_New_Followers`
),
jgs_new_followers AS (
    SELECT PARSE_TIMESTAMP('%F',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'JGS' AS company_name, '509520978' AS _id
    FROM `x-marketing.brp_linkedin_social_media.JGS_New_Followers`
),
rogersgray_new_followers AS (
    SELECT PARSE_TIMESTAMP('%F',date) AS timestamp, organic_followers, sponsored_followers, total_followers,
    'RogersGray' AS company_name, '' AS _id
    FROM `x-marketing.brp_linkedin_social_media.RogersGray_New_Followers`
)
SELECT *
FROM (
    SELECT * FROM brp_new_followers
    UNION ALL
    SELECT * FROM burnham_new_followers
    UNION ALL
    SELECT * FROM guided_solutions_new_followers
    UNION ALL
    SELECT * FROM insgroup_new_followers
    UNION ALL
    SELECT * FROM jgs_new_followers
    UNION ALL
    SELECT * FROM rogersgray_new_followers
);

-- Google Search Campaign Performance
CREATE OR REPLACE TABLE brp.google_search_campaign_performance AS
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
        FROM `x-marketing.brp_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'SEARCH'
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),aggregate_rows AS (
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
),add_calculated_columns AS (
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
SELECT add_calculated_columns.*,
--airtable_ads.* EXCEPT(campaign_id) 
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id;


-- Google Search Ads Variation Performance

CREATE OR REPLACE TABLE brp.google_search_adsvariation_performance AS
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
        FROM `x-marketing.brp_google_ads.ad_performance_report` ads
        
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
SELECT add_calculated_columns.* ,
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, ad_id;


-- Google Search Keyword Performance
CREATE OR REPLACE TABLE brp.google_search_keyword_performance AS
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
        FROM `x-marketing.brp_google_ads.keywords_performance_report` keywords
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
SELECT add_calculated_columns.* ,
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword;


-- Google Search Query Performance
CREATE OR REPLACE TABLE brp.google_search_query_performance AS
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
        FROM `x-marketing.brp_google_ads.search_query_performance_report` query
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
SELECT add_calculated_columns.* ,
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;

--Google Display
CREATE OR REPLACE TABLE brp.google_display_campaign_performance AS
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
                WHEN ad_network_type = 'DISPLAY'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr, 
            conversions, 
            view_through_conversions AS view_through_conv,
            RANK() OVER (
                PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
                ORDER BY keywords._sdc_received_at DESC
            ) AS _rank           
        FROM `x-marketing.brp_google_ads.keywords_performance_report` keywords
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
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10
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
SELECT add_calculated_columns.* ,
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword;


--ConversionversusSQLConversion
CREATE OR REPLACE TABLE brp.google_conversions_sqlconversions AS
WITH brp_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'Baldwin Risk Partners' AS company_name
FROM `x-marketing.brp_google_sheets_conversion.BRP`
),
burnham_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'Burnham' AS company_name
FROM `x-marketing.brp_google_sheet_conversion.Burnham_Benefits`
),
guidedsolutions_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'Guided Solutions' AS company_name
FROM `x-marketing.brp_google_sheet_conversion.Guided_Solutions` 
),
insgroup_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'Insgroup' AS company_name
FROM `x-marketing.brp_google_sheet_conversion.INSGroup` 
),
jgs_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'JGS' AS company_name
FROM `x-marketing.brp_google_sheet_conversion.JGS_`
),
rogersgray_conversion AS (
SELECT conversion_source,conversion_action,currency_code,campaign_id,campaign_type,conversion_category,all_conv_,conversions,campaign,status,budget,network__with_search_partners_,PARSE_TIMESTAMP('%F',day) AS day, 'RogersGray' AS company_name
FROM `x-marketing.brp_google_sheet_conversion.RogersGray` 
)
SELECT *
FROM (
SELECT * FROM brp_conversion
UNION ALL
SELECT * FROM burnham_conversion
UNION ALL
SELECT * FROM guidedsolutions_conversion
UNION ALL
SELECT * FROM insgroup_conversion
UNION ALL
SELECT * FROM jgs_conversion
UNION ALL
SELECT * FROM rogersgray_conversion
);

-- ABX BRP
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
  FROM
    `brp_abx_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `brp_abx_linkedin_ads.creatives`
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
  FROM
    `brp_abx_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM
    `brp_abx_linkedin_ads.campaign_groups`
),
 airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.brp_mysql.optimization_airtable_ads_linkedin`
), _all AS (
SELECT
airtable_ads.*EXCEPT(_adid), 
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

LEFT JOIN airtable_ads 
ON 
CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING)
), total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignNames ) AS ads_per_campaign
  FROM _all
)
, daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
      AS dailyBudget_per_ad
  FROM total_ads
) SELECT * FROM daily_budget_per_ad_per_campaign;


--Google Analytics
/*CREATE OR REPLACE TABLE `x-marketing.brp.google_analytics_sheet` AS
WITH burnham AS (
SELECT *,
'Burnham Benefits' AS company FROM `x-marketing.brp_google_sheets_v2.Burnham_Benefits_`
),
guided_solution AS(
  SELECT *, 'Guided Solution' AS company FROM `x-marketing.brp_google_sheets_v2.Guided_Solutions`
),
bks_partners AS (
  SELECT *, 'BKS Partners' AS company FROM `x-marketing.brp_google_sheets.BKS_Partners`
),
brp AS (
  SELECT *, 'BRP' AS company FROM `x-marketing.brp_google_sheets.BRP`
),
rogers_gray AS (
  SELECT *, 'Rogers Gray' AS company FROM `x-marketing.brp_google_sheets.RogersGray`
),
jgs_insurance AS (
  SELECT *, 'JGS Insurance' AS company FROM `x-marketing.brp_google_sheets.JGS_Insurance_`
)

SELECT *
FROM (
  SELECT * FROM burnham
  UNION ALL
  SELECT * FROM guided_solution
  UNION ALL
  SELECT * FROM bks_partners
  UNION ALL
  SELECT * FROM brp
  UNION ALL
  SELECT * FROM rogers_gray
  UNION ALL
  SELECT * FROM jgs_insurance
)*/