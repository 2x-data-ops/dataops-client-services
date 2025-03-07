CREATE OR REPLACE TABLE assent.linkedin_ads_performance AS
WITH LI_ads AS (
    SELECT 
        campaign_id, 
        _sdc_sequence,
        date_range.start.day AS _startDate, 
        start_at AS _date,
        --one_click_leads AS _leads, 
        impressions AS _reach, 
        cost_in_usd AS _spent, 
        impressions AS _impressions, 
        clicks AS _clicks, 
        external_website_conversions AS _conversions,
        total_engagements AS _total_engagements,
        landing_page_clicks,
    FROM 
        `x-marketing.assent_linkedin_ads.ad_analytics_by_campaign` 
        ORDER BY start_at DESC
),
--, 
-- ads_title AS (
--     SELECT 
--         SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID, 
--         campaign_id ,
--          name AS _ads_name,
--     intended_status AS ads_status,
--   FROM
--     `assent_linkedin_ads.creatives` c
--     LEFT JOIN `assent_linkedin_ads.video_ads` v ON content.reference  = v.content_reference 
        
-- ), 
campaigns AS (
    SELECT 
        id AS campaignID, 
        name AS _campaignName, 
        status, 
        cost_type, 
        total_budget.amount AS total_budget, 
        campaign_group_id,
        CASE WHEN account LIKE '%503313685%' THEN 'assent' 
        WHEN account LIKE '%509580263%' THEN 'OneMarket'
        ELSE NULL END AS account_name,
        TIMESTAMP_DIFF( run_schedule.end,run_schedule.start,DAY) AS date_diffs,
        run_schedule.start AS _campaign_start_date,
        run_schedule.end AS _campaign_end_date
    FROM 
        `x-marketing.assent_linkedin_ads.campaigns` 
), 
campaign_group AS ( 
    SELECT 
        id AS groupID, 
        name AS _groupName, 
        status 
    FROM 
        `x-marketing.assent_linkedin_ads.campaign_groups` 
), 
-- airtable_ads AS (
--     SELECT 
--         * EXCEPT(
--             _sdc_batched_at, 
--             _sdc_received_at, 
--             _sdc_sequence, 
--             _sdc_table_version
--         ) 
--     FROM 
--         `x-marketing.assent_mysql.db_airtable_ads`
--)
--,
combine_all AS (
    SELECT 
        --airtable_ads.* EXCEPT(_adid), 
        campaign_group._groupName, 
        campaign_group.status, 
        campaigns._campaignName, 
        campaigns.total_budget, 
        campaigns.account_name,
        campaigns._campaign_start_date,
        --ads_status,
        date_diffs,
        LI_ads.*
    FROM 
        LI_ads
    -- RIGHT JOIN 
    --     ads_title 
    -- ON 
    --     CAST(LI_ads.creative_id AS STRING) = ads_title.cID
    JOIN 
        campaigns 
    ON 
        LI_ads.campaign_id = campaigns.campaignID
    JOIN 
        campaign_group 
    ON 
        campaigns.campaign_group_id = campaign_group.groupID
    -- JOIN 
    --     airtable_ads 
    -- ON 
    --     LI_ads.creative_id = airtable_ads._adid
),
 total_ads AS (
    SELECT
        *,
        COUNT(campaign_id) OVER (
            PARTITION BY _campaign_start_date, _campaignName
        ) AS ads_per_campaign
    FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
    SELECT
        *,
        CASE WHEN ads_per_campaign > 0 THEN total_budget / ads_per_campaign ELSE 0 END AS dailyBudget_per_ad
    FROM total_ads
)
SELECT * FROM daily_budget_per_ad_per_campaign;




--if daily budget is not null, used this
/*daily_budget_per_ad_per_campaign AS (
    SELECT
        *,
        CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign ELSE 0 END AS dailyBudget_per_ad
    FROM total_ads_per_campaign
)
SELECT * FROM daily_budget_per_ad_per_campaign;*/