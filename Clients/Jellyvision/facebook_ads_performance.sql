CREATE OR REPLACE TABLE `x-marketing.jellyvision.facebook_ads_performance` AS
WITH FB_ads AS (
    SELECT
        ad_id AS _adid,
        adset_id AS _adset_id,
        campaign_id AS _campaign_id,
        ad_name as _ads_name,
        adset_name AS _adset_name,
        campaign_name AS _campaign_name,
        date_start AS _date_start,
        reach AS _reach,
        spend AS _spend,
        impressions AS _impressions,
        clicks AS _clicks,
        inline_link_clicks AS _inline_link_clicks,
        unique_inline_link_clicks AS _unique_inline_link_clicks,
        ctr AS _ctr,
        cost_per_unique_click AS _cost_per_unique_click,
        cpp AS _cpp, 
        cpc AS _cpc, 
        (SELECT action.value.value FROM UNNEST(actions) AS action WHERE action.value.action_type = 'lead') AS _conversions
    FROM `x-marketing.jellyvision_facebook_ads.ads_insights` 
)
, ad_names AS (
    SELECT 
        adset_id AS _adset_id, 
        k.id AS _adid, 
        k.name AS _ads_name, 
        bid_type AS _bid_type, 
        k.status AS _status_ads, 
        body AS _body, 
        l.name AS _creative_name, 
        --url_tags, 
        l.status AS _status_creative, 
        title AS _title  , 
        thumbnail_url AS _thumbnail_url, 
        instagram_actor_id AS _instagram_actor_id, 
        image_url AS _image_url,
        creative.id AS _creative_id,
    FROM `x-marketing.jellyvision_facebook_ads.ads` k
    LEFT JOIN `x-marketing.jellyvision_facebook_ads.adcreative` l 
        ON creative.id = l.id 
)
, ad_adsets AS (
    SELECT 
        id AS _adset_id, 
        name AS _name, 
        created_time AS _created_time, 
        lifetime_budget AS _lifetime_budget,
        budget_remaining AS _budget_remaining, 
        end_time AS _end_time 
    FROM `x-marketing.jellyvision_facebook_ads.adsets`
)
, ad_campaign AS (
    SELECT 
        id AS _campaign_id, 
        name AS _campaign_name, 
        objective AS _campaign_objective, 
        buying_type AS _buying_type, 
        effective_status AS _effective_status, 
        updated_time AS _updated_time
    FROM `x-marketing.jellyvision_facebook_ads.campaigns`
)
, airtable_ads AS (
    SELECT 
        _adid, 
        _status, 
        _maincampaignname, 
        _advariation, 
        _adsize, 
        _screenshot, 
        _reportinggroup, 
        _adtype, 
        _livedate, 
        _platform, 
        _landingpageurl, 
        _creativedirection, 
        _creativedirections, 
        _advisual 
    FROM `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_facebook` 
)
, combine_all AS (
    SELECT 
        * EXCEPT (rownum)
    FROM (
            SELECT 
                FB_ads.*,
                ad_names._bid_type, 
                ad_names._status_ads, 
                ad_names._body, 
                ad_names._title, 
                --ad_names.url_tags, 
                ad_names._status_creative, 
                ad_names._creative_name, 
                ad_names._thumbnail_url, 
                ad_names._instagram_actor_id, 
                ad_names._image_url,
                ad_adsets._created_time, 
                ad_adsets._lifetime_budget,
                ad_adsets._budget_remaining, 
                ad_adsets._end_time,
                ad_campaign._campaign_objective, 
                ad_campaign._buying_type, 
                ad_campaign._effective_status, 
                ad_campaign._updated_time,
                CAST(ad_names._adid AS INT64) AS _creative_id,
                airtable_ads.* EXCEPT (_adid),
                ROW_NUMBER() OVER(
                    PARTITION BY 
                        FB_ads._adid,
                        FB_ads._adset_id,
                        FB_ads._campaign_id,
                        FB_ads._ads_name,
                        FB_ads._adset_name,
                        FB_ads._campaign_name,
                        FB_ads._date_start,
                        FB_ads._reach,
                        FB_ads._impressions,
                        FB_ads._clicks,
                        FB_ads._inline_link_clicks,
                        FB_ads._unique_inline_link_clicks
                        
                        --CAST(conversions AS INT64)
                    ORDER BY FB_ads._date_start, CAST(FB_ads._conversions AS INT64)  DESC) AS rownum
            FROM FB_ads
            LEFT JOIN ad_names 
                ON ad_names._adid = FB_ads._adid
            LEFT JOIN ad_adsets 
                ON ad_adsets._adset_id = FB_ads._adset_id
            LEFT JOIN ad_campaign 
                ON ad_campaign._campaign_id = FB_ads._campaign_id
            LEFT JOIN airtable_ads 
                ON FB_ads._ads_name = airtable_ads._advariation 
                OR FB_ads._adid = CAST(airtable_ads._adid AS STRING)
        ) 
        WHERE rownum = 1 
)
, total_ads_per_campaign AS (
    SELECT
        *,
        COUNT(_adid) OVER (
            PARTITION BY _date_start, _campaign_name
        ) AS _ads_per_campaign
    FROM combine_all
)
, daily_budget_per_ad_per_campaign AS (
    SELECT
        *,
        CASE WHEN _ads_per_campaign > 0 THEN _lifetime_budget / _ads_per_campaign 
        ELSE 0
        END
           AS _dailyBudget_per_ad,
          CASE WHEN _ads_per_campaign > 0 THEN _budget_remaining / _ads_per_campaign 
        ELSE 0
        END
        AS _dailybudget_remaining_per_ad

    FROM total_ads_per_campaign
)
SELECT 
    final.* 
FROM daily_budget_per_ad_per_campaign final;