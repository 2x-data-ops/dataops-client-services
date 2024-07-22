-----------Consolidate Ads------------------
CREATE OR REPLACE TABLE `emburse.consolidate_ad_performance` AS 
WITH google_ads AS (
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      ads.campaign_id,
      campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
      ads.ad_group_id,
      ad_group_name,
      date AS day,
      ads.id AS ad_id,
      cost_micros/1000000 AS spent,
      impressions,
      clicks,
      conversions,
      INITCAP(campaign_status) AS campaign_status,
      RANK() OVER(
        PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
        ORDER BY ads._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.ad_performance_report` ads
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    day,
    ad_id,
    campaign_status,
    'Google' AS _platform,
    SUM(spent) AS spent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8
  ORDER BY
   day, campaign_id, ad_group_id, ad_id
),

bing_ads AS (
WITH ads AS (
    SELECT * FROM (
        SELECT
            ads.campaignid, 
            ads.campaignname,
            CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
            WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
            WHEN campaignname LIKE '%Germany%' THEN 'Germany'
            WHEN campaignname LIKE '%UK%' THEN 'UK'
            WHEN campaignname LIKE '%US%' THEN 'US'
            WHEN campaignname LIKE '%APAC%' THEN 'APAC'
            WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
            WHEN campaignname LIKE '%NORAM%' THEN 'NORAM' 
            ELSE NULL END AS campaign_country_region,
            adgroupid,
            adgroupname,
            ads.timeperiod, 
            ads.adid, 
            ads.campaignstatus,
            'Bing' AS _platform,
            ads.spend AS cost, 
            ads.impressions, 
            ads.clicks, 
            ads.conversions,
            RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
       )
    WHERE _rank =1
)
SELECT 
ads.* EXCEPT(_rank), 
FROM ads
),

linkedin_ads AS (
WITH LI_ads AS (
 SELECT
    start_at AS _date,
    creative_id,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
  FROM
    `emburse_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `emburse_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    CASE WHEN name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
    WHEN name LIKE '%US/CA%' THEN 'US/CA'
    WHEN name LIKE '%Germany%' THEN 'Germany'
    WHEN name LIKE '%UK%' THEN 'UK'
    WHEN name LIKE '%US%' THEN 'US'
    WHEN name LIKE '%APAC%' THEN 'APAC'
    WHEN name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
    WHEN name LIKE '%NORAM%' THEN 'NORAM'
    ELSE NULL END AS campaign_country_region, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM
    `emburse_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status)
  FROM
    `emburse_linkedin_ads.campaign_groups`
),
_all AS (
SELECT
  campaigns.campaignID,
  campaigns._campaignNames,
  campaigns.campaign_country_region,
  campaign_group.groupID,
  campaign_group._groupName,
  LI_ads._date,
  LI_ads.creative_id,
  campaigns.status,
  'LinkedIn' AS _platform,
  LI_ads._spent,
  LI_ads._impressions,
  LI_ads._clicks,
  LI_ads._conversions
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
) 
SELECT * FROM _all
WHERE _date IS NOT NULL

)
SELECT *
FROM (
  SELECT * FROM google_ads
  UNION ALL
  SELECT * FROM bing_ads
  UNION ALL
  SELECT * FROM linkedin_ads
);






---------------Google Ads----------------------
---Google Search Campaign Performance
CREATE OR REPLACE TABLE emburse.google_search_campaign_performance AS
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
      date AS day,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
      impressions,
      CASE
        WHEN ad_network_type = 'SEARCH' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      conversions,
      view_through_conversions AS view_through_conv,
      campaign_status,
      customer_time_zone,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM
      `emburse_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'SEARCH'
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    day,
    currency,
    campaign_status,
    customer_time_zone,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6, 7
  ORDER BY
  day, campaign_id;


-- Google Search Ads Variation Performance

CREATE OR REPLACE TABLE emburse.google_search_adsvariation_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      ads.campaign_id,
      campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
      ads.ad_group_id,
      ad_group_name,
      date AS day,
      ads.id AS ad_id,
      CASE
        WHEN ads.type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(ads.responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
        END AS headlines,
      TRIM(ads.final_urls, "[']") AS final_urls,
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
      customer_time_zone,
      RANK() OVER(
        PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
        ORDER BY ads._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.ad_performance_report` ads
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
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
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12
  ORDER BY
   day, campaign_id, ad_group_id, ad_id;



-- Google Seach Keyword Performance

CREATE OR REPLACE TABLE emburse.google_search_keyword_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
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
        WHEN ad_network_type = 'SEARCH' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      ad_group_criterion_status,
      customer_time_zone,
      RANK() OVER (
        PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
        ORDER BY keywords._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.keywords_performance_report` keywords
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    match_type,
    keyword,
    quality_score,
    day,
    currency,
    ad_group_criterion_status,
    customer_time_zone,
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
  ORDER BY
  day, campaign_id, ad_group_id, keyword;



-- Google Search Query Performance

CREATE OR REPLACE TABLE emburse.google_search_query_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
      SELECT
        campaign_id,
        campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
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
        customer_time_zone,
        RANK() OVER(
          PARTITION BY date, campaign_id, ad_group_id, keyword.info.text, search_term_view_search_term
          ORDER BY _sdc_received_at DESC
        ) AS _rank
      FROM `emburse_google_ads.search_query_performance_report`
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    keyword,
    -- match_type,
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
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8,9,10, 11,12
ORDER BY
  day, campaign_id, ad_group_id, keyword, search_term;



-- Google Display Campaign Performance

CREATE OR REPLACE TABLE emburse.google_display_campaign_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
      date AS day,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
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
      campaign_status,
      customer_time_zone,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'DISPLAY'
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
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
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6,7,8
ORDER BY
  day, campaign_id;


--- google video performance

CREATE OR REPLACE TABLE `x-marketing.emburse.video_performance` AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank)
    FROM (
SELECT
            report.campaign_id, 
            report.campaign_name,
            CASE WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
            WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
            WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
            WHEN campaign_name LIKE '%UK%' THEN 'UK'
            WHEN campaign_name LIKE '%US%' THEN 'US'
            WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
            WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
            WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
            ELSE NULL END AS campaign_country_region,
            report.date AS day, 
            report.customer_currency_code AS currency,
            report.cost_micros/1000000 AS cost, 
            report.impressions, 
            CASE
                WHEN report.ad_network_type = 'VIDEO'
                THEN report.impressions
                ELSE NULL
            END search_impressions,
            report.clicks, 
            report.conversions, 
            report.view_through_conversions AS view_through_conv,
            report.ad_network_type AS network_type, 
            video_title AS video_title,
            video_channel_id,
            video_id,
            ad_group_status AS group_status,
            report.campaign_status AS campaign_status,
            report.video_views AS _view_views,
            customer_time_zone,
            RANK() OVER(
                PARTITION BY date, campaign_id,report.video_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.emburse_google_ads.video_performance_report` report 
    )
    WHERE _rank = 1
)
    SELECT
        campaign_id, 
        campaign_name, 
        campaign_country_region,
        day,
        currency,
        network_type,
        video_title,
        video_channel_id,
        group_status,
        campaign_status,
        customer_time_zone,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv,
        SUM(_view_views) AS view_views,
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5,6,7,8,9,10, 11
    ORDER BY day, campaign_id;



---bings ads performance
---bing keyword performance
CREATE OR REPLACE TABLE emburse.bing_keyword_performance AS
WITH keywords AS(
    SELECT * EXCEPT(_rank) FROM (
        SELECT
            adgroupid, 
            keywords.timeperiod, 
            keywords.campaignname,
            CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
            WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
            WHEN campaignname LIKE '%Germany%' THEN 'Germany'
            WHEN campaignname LIKE '%UK%' THEN 'UK'
            WHEN campaignname LIKE '%US%' THEN 'US'
            WHEN campaignname LIKE '%APAC%' THEN 'APAC'
            WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
            WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
            ELSE NULL END AS campaign_country_region,
            keywords.campaignid, 
            addistribution,
            keywords.currencycode, 
            keywords.spend AS cost, 
            keywords.impressions , 
            keywords.clicks, 
            CAST(REPLACE(keywords.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
            keywords.ctr, 
            keywords.averagecpc AS avgcpc,
            keywords.conversions , 
            keywords.conversionrate , 
            keywords.deliveredmatchtype, 
            keywords.keyword, 
            keywords.qualityscore, 
            bidmatchtype,
            RANK() OVER (PARTITION BY keywords.timeperiod, keywords.keywordid ORDER BY keywords._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.emburse_bing_ads.keyword_performance_report` keywords
        )
    WHERE _rank =1
), budget AS (
    SELECT id AS campaignid, dailybudget 
    FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
keywords.*, 
budget.dailybudget 
FROM keywords 
JOIN budget ON keywords.campaignid = budget.campaignid
;


---bing ads variation performance
CREATE OR REPLACE TABLE emburse.bing_adsvariation_performance AS
WITH ads AS (
    SELECT* FROM (
        SELECT
            adgroupid, 
            titlepart1, 
            titlepart2, 
            titlepart3, 
            ads.campaignname,
            CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
            WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
            WHEN campaignname LIKE '%Germany%' THEN 'Germany'
            WHEN campaignname LIKE '%UK%' THEN 'UK'
            WHEN campaignname LIKE '%US%' THEN 'US'
            WHEN campaignname LIKE '%APAC%' THEN 'APAC'
            WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
            WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
            ELSE NULL END AS campaign_country_region, 
            ads.campaignid, 
            ads.adid, 
            ads.timeperiod, 
            ads.currencycode, 
            ads.spend AS cost, 
            ads.impressions , 
            ads.clicks, 
            CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
            ads.conversions , 
            ads.conversionrate ,
             ads._sdc_report_datetime,
            RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
       )
    WHERE _rank =1
), budget AS (
    SELECT 
    id AS campaignid, 
    dailybudget 
    FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
ads.* EXCEPT(_rank), 
FROM ads;


---Bing Campaign Performance
CREATE OR REPLACE TABLE emburse.bing_campaign_performance AS
WITH campaign AS(
    SELECT * EXCEPT(_rank) FROM (
        SELECT
            campaign.timeperiod, 
            campaign.campaignname,
            CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
            WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
            WHEN campaignname LIKE '%Germany%' THEN 'Germany'
            WHEN campaignname LIKE '%UK%' THEN 'UK'
            WHEN campaignname LIKE '%US%' THEN 'US'
            WHEN campaignname LIKE '%APAC%' THEN 'APAC'
            WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
            WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
            ELSE NULL END AS campaign_country_region, 
            campaign.campaignid, 
            addistribution,
            campaign.currencycode, 
            campaign.spend AS cost, 
            campaign.impressions , 
            campaign.clicks, 
            CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
            campaign.conversions,
            RANK() OVER (PARTITION BY campaign.timeperiod, campaign.campaignid ORDER BY campaign._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.emburse_bing_ads.campaign_performance_report` campaign
        )
    WHERE _rank =1 
), budget AS (
    SELECT id AS campaignid, 
    dailybudget 
    FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT campaign.*, 
budget.dailybudget 
FROM campaign
JOIN budget ON campaign.campaignid = budget.campaignid;


---Bing Ad Group Performance
CREATE OR REPLACE TABLE emburse.bing_adgroup_performance AS
WITH adgroups AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
            adgroups.timeperiod, 
            adgroups.adgroupid, 
            addistribution, 
            campaignid,
            adgroups.currencycode, 
            adgroups.allreturnonadspend AS cost, 
            adgroups.impressions , 
            adgroups.clicks, 
            CAST(REPLACE(adgroups.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
            adgroups.ctr, adgroups.averagecpc AS avgcpc,
            adgroups.conversions , 
            adgroups.conversionrate,
            RANK() OVER (PARTITION BY adgroups.timeperiod, adgroups.adgroupid ORDER BY adgroups._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.emburse_bing_ads.ad_group_performance_report` adgroups
        )
    WHERE _rank = 1 
), budget AS (
    SELECT id AS campaignid, 
    dailybudget 
    FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
adgroups.* EXCEPT(campaignid), 
budget.dailybudget
FROM adgroups
JOIN budget ON adgroups.campaignid = budget.campaignid;




---Bing Search Query Performance
CREATE OR REPLACE TABLE emburse.bing_search_query_performance AS
  SELECT * EXCEPT(_rank)
  FROM (
      SELECT
        campaignid,
        campaignname,
      CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
        adgroupid,
        adgroupname,
        keyword AS keyword,
        -- search_term_match_type AS match_type,
        searchquery AS search_term,
        timeperiod AS day,
        spend AS cost,
        impressions,
        clicks,
        conversions,
        campaignstatus,
        adgroupstatus,
        RANK() OVER(
          PARTITION BY timeperiod, campaignid, adgroupid, keyword, searchquery
          ORDER BY _sdc_received_at DESC
        ) AS _rank
      FROM `x-marketing.emburse_bing_ads.search_query_performance_report`
  )
  WHERE _rank = 1;



---LinkedIn ads
CREATE OR REPLACE TABLE emburse.linkedin_ads_performance AS
WITH LI_ads AS (
 SELECT
    EXTRACT(DATE FROM start_at) AS _date,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens
  FROM
    `emburse_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `emburse_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    CASE WHEN name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
    WHEN name LIKE '%US/CA%' THEN 'US/CA'
    WHEN name LIKE '%Germany%' THEN 'Germany'
    WHEN name LIKE '%UK%' THEN 'UK'
    WHEN name LIKE '%US%' THEN 'US'
    WHEN name LIKE '%APAC%' THEN 'APAC'
    WHEN name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
    WHEN name LIKE '%NORAM%' THEN 'NORAM'
    ELSE NULL END AS campaign_country_region, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id,
    INITCAP(REPLACE(objective_type,"_"," ")) AS campaign_objective
  FROM
    `emburse_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status) AS status
  FROM
    `emburse_linkedin_ads.campaign_groups`
),
_all AS (
SELECT
  LI_ads.*,
  campaigns.account_id,
  campaigns.campaignID,
  campaigns._campaignNames,
  campaigns.campaign_country_region,
  campaigns.status AS _campaign_status,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaigns.campaign_objective,
  campaign_group.status AS _campaign_group_status
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
),
total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
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
SELECT * FROM daily_budget_per_ad_per_campaign
WHERE _date IS NOT NULL;