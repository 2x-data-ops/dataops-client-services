CREATE OR REPLACE TABLE blend360.linkedin_ads_performance AS
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
        landing_page_clicks AS li_website_visits,
        video_views AS _video_views,
        video_starts AS _video_play,
        video_first_quartile_completions AS _video_views_25percent,
        video_midpoint_completions AS _video_views_50percent,
        video_third_quartile_completions AS _video_views_75percent,
        video_completions AS _video_completions
    FROM 
        `x-marketing.blend360_linkedin_ads.ad_analytics_by_creative`
    ORDER BY 
        start_at DESC
), 
ads_title AS (
    SELECT 
        SPLIT(id, 'Creative:')[ORDINAL(2)] AS creative_id, 
        campaign_id 
    FROM 
        `x-marketing.blend360_linkedin_ads.creatives`
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
        `x-marketing.blend360_linkedin_ads.campaigns` 
), 
campaign_group AS ( 
    SELECT 
        id AS li_campaign_group_id, 
        name AS li_campaign_group_name, 
        status AS li_campaign_group_status
    FROM 
        `x-marketing.blend360_linkedin_ads.campaign_groups` 
), 
airtable_ads AS (
    SELECT * 
     FROM 
         `x-marketing.blend360_mysql.db_airtable_digital_campaign`
 ),
 manual_tracker AS (
    SELECT
      campaign_id,
      ad_variant_id,
      variant_monthly_reach,
      variant_all_time_reach,
      campaign_monthly_reach,
      campaign_all_time_reach,
      PARSE_TIMESTAMP('%F',extract_date) AS extract_date,
      PARSE_TIMESTAMP('%F',live_date) AS _trackerLiveDate
    FROM `x-marketing.blend360_linkedin_reached.LinkedIn_Campaign_Data`
    --WHERE campaign_id = 263790924 AND ad_variant_id = 320339804
 ),
combine_all AS (
    SELECT 
        airtable_ads.*, --EXCEPT(_adid), 
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
    RIGHT JOIN 
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
    JOIN 
        airtable_ads 
    ON 
        LI_ads.li_creative_id = CAST(airtable_ads._advariantid AS INT64)
),
total_ads_per_campaign AS (
    SELECT
        *,
        COUNT(li_creative_id) OVER (
            PARTITION BY li_run_date, li_campaign_name
        ) AS fm_ads_per_campaign
    FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
    SELECT
        *,
        li_daily_budget / IF(fm_ads_per_campaign = 0, 1, fm_ads_per_campaign)  AS fm_daily_budget_per_ad
    FROM total_ads_per_campaign
)
SELECT 
    a.* ,
    b.variant_monthly_reach,
    b.variant_all_time_reach,
    b.campaign_monthly_reach,
    b.campaign_all_time_reach,
    b.extract_date,
    b._trackerLiveDate
FROM daily_budget_per_ad_per_campaign a 
LEFT JOIN manual_tracker b 
ON a.li_creative_id = CAST(b.ad_variant_id AS INT64)
AND b.campaign_id = a._campaignid
AND extract_date = li_run_date 
--AND FORMAT_DATE('%Y-%m-%d', TIMESTAMP(a.li_campaign_start_date)) = b._trackerLiveDate
--WHERE b._trackerLiveDate IS NOT NULL;