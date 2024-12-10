
CREATE OR REPLACE TABLE `thunder.linkedin_ads_performance` AS

WITH LI_ads AS (

    SELECT 
        creative_id AS li_creative_id, 
        start_at AS li_run_date, 
        one_click_leads AS li_leads, 
        approximate_unique_impressions AS li_reach, 
        cost_in_usd AS li_spent, 
        impressions AS li_impressions, 
        clicks AS li_clicks, 
        external_website_conversions AS li_conversions,
        landing_page_clicks AS li_website_visits
    FROM 
        `x-marketing.thunder_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY 
        start_at DESC

),

ads_title AS (

    SELECT 
        SPLIT(id, 'Creative:')[ORDINAL(2)] AS creative_id, 
        campaign_id 
    FROM 
        `x-marketing.thunder_linkedin_ads.creatives`

),

campaigns AS (

    SELECT 
        id AS li_campaign_id, 
        name AS li_campaign_name, 
        objective_type AS li_campaign_objective,
        status AS li_campaign_status,
        COALESCE(
            daily_budget.amount,
            total_budget.amount / TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) 
        ) AS li_daily_budget, 
        run_schedule.start AS li_campaign_start_date,
        run_schedule.end AS li_campaign_end_date,
        campaign_group_id 
    FROM 
        `x-marketing.thunder_linkedin_ads.campaigns` 

), 

campaign_group AS ( 

    SELECT 
        id AS li_campaign_group_id, 
        name AS li_campaign_group_name, 
        status AS li_campaign_group_status
    FROM 
        `x-marketing.thunder_linkedin_ads.campaign_groups` 

), 
-- airtable_ads AS (
--     SELECT 
--         _adid,
--         _adformat AS at_ad_format,
--         _adid AS at_ad_id,
--         _adtype AS at_ad_type,
--         _advariation AS at_ad_variation,
--         _campaign AS at_utm_campaign,
--         _campaignid AS at_campaign_id,
--         _campaignobjective AS at_campaign_objective,
--         _content AS at_utm_content,
--         _landingpageurl AS at_landing_page_url,
--         _linkedincampaignmanager AS at_linkedin_campaign_manager_url,
--         _livedate AS at_live_date,
--         _medium AS at_utm_medium,
--         _platform AS at_platform,
--         _reportinggroup AS at_reporting_group,
--         _screenshot AS at_screenshot,
--         _source AS at_utm_source,
--         _status AS at_status
--     FROM 
--         `x-marketing.hyland_mysql.db_airtable_ads`
-- ),

combine_all AS (

    SELECT 
        -- airtable_ads.* EXCEPT(_adid), 
        campaign_group.li_campaign_group_name, 
        campaign_group.li_campaign_group_status, 
        campaigns.li_campaign_name, 
        campaigns.li_campaign_objective,
        campaigns.li_campaign_status,
        campaigns.li_daily_budget, 
        campaigns.li_campaign_start_date,
        campaigns.li_campaign_end_date,
        LI_ads.*
    FROM 
        LI_ads
    JOIN 
        ads_title 
    ON 
        CAST(LI_ads.li_creative_id AS STRING) = ads_title.creative_id
    JOIN 
        campaigns 
    ON 
        ads_title.campaign_id = campaigns.li_campaign_id
    JOIN 
        campaign_group 
    ON 
        campaigns.campaign_group_id = campaign_group.li_campaign_group_id
    -- JOIN 
    --     airtable_ads 
    -- ON 
    --     LI_ads.li_creative_id = airtable_ads._adid
),

total_ads_per_campaign AS (

    SELECT
        *,
        COUNT(li_creative_id) OVER (
            PARTITION BY li_run_date, li_campaign_name
        ) AS fm_ads_per_campaign
    FROM 
        combine_all

),
daily_budget_per_ad_per_campaign AS (

    SELECT
        *,
        li_daily_budget / fm_ads_per_campaign AS fm_daily_budget_per_ad
    FROM 
        total_ads_per_campaign

)
SELECT * FROM daily_budget_per_ad_per_campaign;


