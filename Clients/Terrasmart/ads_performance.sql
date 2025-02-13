CREATE OR REPLACE TABLE `terrasmart.linkedin_ads_performance` AS
WITH LI_ads AS (
    SELECT 
        *
    FROM 
        (
            SELECT 
            creative_id, 
            start_at AS _startDate, 
            one_click_leads AS _leads, 
            impressions AS _reach, 
            cost_in_usd AS _spent, 
            impressions AS _impressions, 
            clicks AS _clicks, 
            external_website_conversions AS _conversions,
            total_engagements AS _total_engagements,
            landing_page_clicks,
            total_engagements, 
            shares, 
            follows, 
            comments
            FROM `x-marketing.terrasmart_linkedin_ads_v2.ad_analytics_by_creative` 
            UNION ALL 
            SELECT 
            creative_id, 
            start_at AS _startDate, 
            one_click_leads AS _leads, 
            impressions AS _reach, 
            cost_in_usd AS _spent, 
            impressions AS _impressions, 
            clicks AS _clicks, 
            external_website_conversions AS _conversions,
            total_engagements AS _total_engagements,
            landing_page_clicks,
            total_engagements, 
            shares, 
            follows, 
            comments
            FROM `x-marketing.terrasmart_ts_linkedin_ads.ad_analytics_by_creative`
        
        )
        ORDER BY _startDate DESC
        
), 
ads_title AS (
    SELECT 
        *
    FROM 
        (
            SELECT 
            SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID, 
            campaign_id,
            account_id,
            acc.name AS account_name
            FROM `x-marketing.terrasmart_linkedin_ads_v2.creatives` c
            LEFT JOIN `x-marketing.terrasmart_linkedin_ads_v2.accounts` acc ON acc.id = account_id
            UNION ALL
            SELECT 
            SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID, 
            campaign_id,
            account_id,
            acc.name AS account_name
            FROM `x-marketing.terrasmart_ts_linkedin_ads.creatives` c
            LEFT JOIN `x-marketing.terrasmart_ts_linkedin_ads.accounts` acc ON acc.id = account_id 

        )
        ---WHERE SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] = '360474706'
), 
campaigns AS (
   SELECT * 
   FROM
   (
     SELECT 
        id AS campaignID,   
        name AS _campaignName, 
        status, 
        cost_type, 
        COALESCE(
            daily_budget.amount,
            total_budget.amount / TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) 
        ) AS dailyBudget, 
        campaign_group_id 
    FROM 
        `x-marketing.terrasmart_linkedin_ads_v2.campaigns` 
    UNION ALL 
    SELECT 
        id AS campaignID,   
        name AS _campaignName, 
        status, 
        cost_type, 
        COALESCE(
            --daily_budget.amount
            0,
            total_budget.amount / TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) 
        ) AS dailyBudget, 
        campaign_group_id 
    FROM 
        `x-marketing.terrasmart_ts_linkedin_ads.campaigns` 
   )
), 
campaign_group AS ( 
    SELECT * 
    FROM (
    SELECT 
        id AS groupID, 
        name AS _groupName, 
        status 
    FROM 
        `x-marketing.terrasmart_linkedin_ads_v2.campaign_groups` 
        UNION ALL
    SELECT 
        id AS groupID, 
        name AS _groupName, 
        status 
    FROM 
        `x-marketing.terrasmart_ts_linkedin_ads.campaign_groups`  
    )

), 
airtable_ads AS (
    WITH ad_ops_airtable AS (
        SELECT SAFE_CAST(_adid AS INT) AS _adid, _status, _adname AS _advariation, '' AS _content, _screenshot, _reportinggroup, _campaignname AS _campaign, '' AS _source, '' AS _medium, _id, _adtype, CAST(NULL AS TIMESTAMP) AS _livedate, _platform, _websiteurl AS _landingpageurl,_segment
        FROM `x-marketing.terrasmart_mysql_2.optimization_airtable_ads_linkedin`
        WHERE _adid IS NOT NULL
        --_adid = '355056754'
    ),
    old_airtable AS (
    SELECT 
    * 
    EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) , ''
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads`
    WHERE _adid NOT IN (SELECT DISTINCT _adid FROM ad_ops_airtable)
    )
    SELECT *
    FROM (
        SELECT * FROM ad_ops_airtable
        UNION DISTINCT
        SELECT * FROM old_airtable
    )
),
combine_all AS (
    SELECT 
        airtable_ads.* EXCEPT(_adid), 
        campaign_group._groupName, 
        campaign_group.status, 
        campaigns._campaignName, 
        campaigns.dailyBudget, 
        LI_ads.*,
        ads_title.account_id,
        ads_title.account_name
    FROM 
        LI_ads
    RIGHT JOIN 
        ads_title 
    ON 
        CAST(LI_ads.creative_id AS STRING) = ads_title.cID
    JOIN 
        campaigns 
    ON 
        ads_title.campaign_id = campaigns.campaignID
    JOIN 
        campaign_group 
    ON 
        campaigns.campaign_group_id = campaign_group.groupID
    JOIN 
        airtable_ads 
    ON 
        LI_ads.creative_id = airtable_ads._adid
),
total_ads_per_campaign AS (
    SELECT
        *,
        COUNT(creative_id) OVER (
            PARTITION BY _startDate, _campaignName
        ) AS ads_per_campaign
    FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
    SELECT
        *,
        dailyBudget / ads_per_campaign AS dailyBudget_per_ad
    FROM total_ads_per_campaign
)
SELECT * FROM daily_budget_per_ad_per_campaign;

-- Google Search Campaign Performance
CREATE OR REPLACE TABLE terrasmart.google_search_campaign_performance AS
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
                WHEN ad_network_type = 'SEARCH'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr,
            conversions, 
            view_through_conversions AS view_through_conv,
            RANK() OVER(
                PARTITION BY date, campaign_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.terrasmart_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'SEARCH'
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),aggregate_rows AS (
    SELECT
        campaign_id, 
        campaign_name, 
        day,
        currency,
        budget,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5
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
), airtable_ads AS (
    SELECT 
    airtable.* EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) ,
    ad_group_id,
    campaign_id
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads` airtable 
    JOIN`x-marketing.terrasmart_google_ads.ads` ads ON ads.id = airtable._adid
)
SELECT add_calculated_columns.*,
airtable_ads.* EXCEPT(campaign_id) FROM add_calculated_columns
JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id;


-- Google Search Ads Variation Performance

CREATE OR REPLACE TABLE terrasmart.google_search_adsvariation_performance AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank)
    FROM (
        SELECT 
            campaign_id,
            campaign_name, 
            ad_group_id, 
            ad_group_name, 
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
        FROM `x-marketing.terrasmart_google_ads.ad_performance_report` ads
        
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
), airtable_ads AS (
    SELECT 
    airtable.* EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) ,
    ad_group_id,
    campaign_id
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads` airtable 
    JOIN`x-marketing.terrasmart_google_ads.ads` ads ON ads.id = airtable._adid
)
SELECT add_calculated_columns.* ,
airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, ad_id;


-- Google Search Keyword Performance
CREATE OR REPLACE TABLE terrasmart.google_search_keyword_performance AS
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
            ad_group_criterion_quality_info.quality_score AS quality_score,
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
            RANK() OVER (
                PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
                ORDER BY keywords._sdc_received_at DESC
            ) AS _rank           
        FROM `x-marketing.terrasmart_google_ads.keywords_performance_report` keywords
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
), airtable_ads AS (
    SELECT 
    airtable.* EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) ,
    ad_group_id,
    campaign_id
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads` airtable 
    JOIN`x-marketing.terrasmart_google_ads.ads` ads ON ads.id = airtable._adid
)
SELECT add_calculated_columns.* ,
airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword;


-- Google Search Query Performance
CREATE OR REPLACE TABLE terrasmart.google_search_query_performance AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
        campaign_id, 
        campaign_name, 
        ad_group_id,
        ad_group_name, 
        keyword.info.text AS keyword, 
        search_term_match_type AS match_type, 
        search_term_view_search_term AS search_term,
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
        FROM `x-marketing.terrasmart_google_ads.search_query_performance_report` query
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
        match_type, 
        search_term,
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
), airtable_ads AS (
    SELECT 
    airtable.* EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) ,
    ad_group_id,
    campaign_id
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads` airtable 
    JOIN`x-marketing.terrasmart_google_ads.ads` ads ON ads.id = airtable._adid
)
SELECT add_calculated_columns.* ,
airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;


CREATE OR REPLACE TABLE terrasmart.google_display_campaign_performance AS
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
                WHEN ad_network_type = 'DISPLAY'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr,
            conversions, 
            view_through_conversions AS view_through_conv,
            RANK() OVER(
                PARTITION BY date, campaign_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.terrasmart_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'DISPLAY'
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),aggregate_rows AS (
    SELECT
        campaign_id, 
        campaign_name, 
        day,
        currency,
        budget,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5
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
), airtable_ads AS (
    SELECT 
    airtable.* EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) ,
    ad_group_id,
    campaign_id
    FROM `x-marketing.gibraltar_mysql.db_airtable_ads` airtable 
    JOIN`x-marketing.terrasmart_google_ads.ads` ads ON ads.id = airtable._adid


)
SELECT add_calculated_columns.*,
airtable_ads.* EXCEPT(campaign_id) FROM add_calculated_columns
JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id;