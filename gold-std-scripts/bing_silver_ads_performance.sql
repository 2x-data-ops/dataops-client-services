---bing keyword performance
TRUNCATE TABLE `x-marketing.emburse.bing_keyword_performance`;
INSERT INTO `x-marketing.emburse.bing_keyword_performance` (
  _ad_group_id,
  _time_period,
  _campaign_name,
  _campaign_country_region,
  _campaign_id,
  _ad_distribution,
  _currency_code,
  _cost,
  _impressions,
  _clicks,
  _absolute_top_impression_rate_percent,
  _ctr,
  _avg_cpc,
  _conversions,
  _conversion_rate,
  _delivered_match_type,
  _keyword,
  _quality_score,
  _bid_match_type,
  _daily_budget
)
WITH keywords AS (
  SELECT
    adgroupid AS _ad_group_id, 
    keywords.timeperiod AS _time_period, 
    keywords.campaignname AS _campaign_name,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS _campaign_country_region,
    keywords.campaignid AS _campaign_id, 
    addistribution AS _ad_distribution,
    keywords.currencycode AS _currency_code, 
    keywords.spend AS _cost, 
    keywords.impressions AS _impressions, 
    keywords.clicks AS _clicks, 
    CAST(REPLACE(keywords.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS _absolute_top_impression_rate_percent, 
    keywords.ctr AS _ctr, 
    keywords.averagecpc AS _avg_cpc,
    keywords.conversions AS _conversions, 
    keywords.conversionrate AS _conversion_rate, 
    keywords.deliveredmatchtype AS _delivered_match_type, 
    keywords.keyword AS _keyword, 
    keywords.qualityscore AS _quality_score, 
    bidmatchtype AS _bid_match_type
  FROM `x-marketing.emburse_bing_ads.keyword_performance_report` keywords
  QUALIFY RANK() OVER (
    PARTITION BY keywords.timeperiod, keywords.keywordid
    ORDER BY keywords._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS _campaign_id,
    dailybudget AS _daily_budget
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  keywords.*, 
  budget._daily_budget
FROM keywords 
JOIN budget 
  USING(_campaign_id);

---bing ads variation performance
TRUNCATE TABLE `x-marketing.emburse.bing_adsvariation_performance`;
INSERT INTO `x-marketing.emburse.bing_adsvariation_performance` (
  _ad_group_id,
  _title_part_1,
  _title_part_2,
  _title_part_3,
  _campaign_name,
  _campaign_country_region,
  _campaign_id,
  _ad_id,
  _time_period,
  _currency_code,
  _cost,
  _impressions,
  _clicks,
  _absolute_top_impression_rate_percent,
  _conversions,
  _conversion_rate,
  _sdc_report_datetime,
  _daily_budget
)
WITH ads AS (
  SELECT
    adgroupid AS _ad_group_id, 
    titlepart1 AS _title_part_1, 
    titlepart2 AS _title_part_2, 
    titlepart3 AS _title_part_3, 
    ads.campaignname AS _campaign_name,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS _campaign_country_region, 
    ads.campaignid AS _campaign_id, 
    ads.adid AS _ad_id, 
    ads.timeperiod AS _time_period, 
    ads.currencycode AS _currency_code, 
    ads.spend AS _cost, 
    ads.impressions AS _impressions, 
    ads.clicks AS _clicks, 
    CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS _absolute_top_impression_rate_percent, 
    ads.conversions AS _conversions, 
    ads.conversionrate AS _conversion_rate,
    ads._sdc_report_datetime
  FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS _campaign_id, 
    dailybudget AS _daily_budget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  ads.*, 
  budget._daily_budget 
FROM ads
JOIN budget
  USING(_campaign_id);

---Bing Campaign Performance
TRUNCATE TABLE `x-marketing.emburse.bing_campaign_performance`;
INSERT INTO `x-marketing.emburse.bing_campaign_performance` (
  _time_period,
  _campaign_name,
  _campaign_country_region,
  _campaign_id,
  _ad_distribution,
  _currency_code,
  _cost,
  _impressions,
  _clicks,
  _absolute_top_impression_rate_percent,
  _conversions,
  _campaign_status,
  _daily_budget
)
WITH campaign AS (
  SELECT
    campaign.timeperiod AS _time_period, 
    campaign.campaignname AS _campaign_name,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS _campaign_country_region, 
    campaign.campaignid AS _campaign_id, 
    addistribution AS _ad_distribution,
    campaign.currencycode AS _currency_code, 
    campaign.spend AS _cost,
    campaign.impressions AS _impressions,
    campaign.clicks AS _clicks,
    CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64) AS _absolute_top_impression_rate_percent, 
    campaign.conversions AS _conversions,
    campaign.campaignstatus AS _campaign_status
  FROM `x-marketing.emburse_bing_ads.campaign_performance_report` campaign
  QUALIFY RANK() OVER (PARTITION BY campaign.timeperiod, campaign.campaignid ORDER BY campaign._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS _campaign_id, 
    dailybudget AS _daily_budget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  campaign.*, 
  budget._daily_budget 
FROM campaign
JOIN budget 
  USING(_campaign_id);

---Bing Ad Group Performance
TRUNCATE TABLE `x-marketing.emburse.bing_adgroup_performance`;
INSERT INTO `x-marketing.emburse.bing_adgroup_performance` (
  _time_period,
  _ad_group_id,
  _ad_distribution,
  _currency_code,
  _cost,
  _impressions,
  _clicks,
  _absolute_top_impression_rate_percent,
  ctr,
  _avg_cpc,
  _conversions,
  _conversion_rate,
  _daily_budget
)
WITH adgroups AS (
  SELECT
    adgroups.timeperiod AS _time_period, 
    adgroups.adgroupid AS _ad_group_id, 
    addistribution AS _ad_distribution, 
    campaignid AS _campaign_id,
    adgroups.currencycode AS _currency_code, 
    adgroups.allreturnonadspend AS _cost, 
    adgroups.impressions AS _impressions, 
    adgroups.clicks AS _clicks, 
    CAST(REPLACE(adgroups.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS _absolute_top_impression_rate_percent, 
    adgroups.ctr, adgroups.averagecpc AS _avg_cpc,
    adgroups.conversions AS _conversions, 
    adgroups.conversionrate AS _conversion_rate
  FROM `x-marketing.emburse_bing_ads.ad_group_performance_report` adgroups
  QUALIFY RANK() OVER (PARTITION BY adgroups.timeperiod, adgroups.adgroupid ORDER BY adgroups._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS _campaign_id, 
    dailybudget AS _daily_budget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  adgroups.* EXCEPT (_campaign_id), 
  budget._daily_budget
FROM adgroups
JOIN budget 
  USING(_campaign_id);

---Bing Search Query Performance
TRUNCATE TABLE `x-marketing.emburse.bing_search_query_performance`;
INSERT INTO `x-marketing.emburse.bing_search_query_performance` (
  _campaign_id,
  _campaign_name,
  _campaign_country_region,
  _ad_group_id,
  _ad_group_name,
  _keyword,
  _search_term,
  _day,
  _cost,
  _impressions,
  _clicks,
  _conversions,
  _campaign_status,
  _ad_group_status
)
SELECT
  campaignid AS _campaign_id,
  campaignname AS _campaign_name,
  CASE 
    WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
    WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
    WHEN campaignname LIKE '%Germany%' THEN 'Germany'
    WHEN campaignname LIKE '%UK%' THEN 'UK'
    WHEN campaignname LIKE '%US%' THEN 'US'
    WHEN campaignname LIKE '%APAC%' THEN 'APAC'
    WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
    WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
    ELSE NULL 
  END AS _campaign_country_region,
  adgroupid AS _ad_group_id,
  adgroupname AS _ad_group_name,
  keyword AS _keyword,
  -- search_term_match_type AS match_type,
  searchquery AS _search_term,
  timeperiod AS _day,
  spend AS _cost,
  impressions AS _impressions,
  clicks AS _clicks,
  conversions AS _conversions,
  campaignstatus AS _campaign_status,
  adgroupstatus AS _ad_group_status
FROM `x-marketing.emburse_bing_ads.search_query_performance_report`
QUALIFY RANK() OVER (PARTITION BY timeperiod, campaignid, adgroupid, keyword, searchquery ORDER BY _sdc_received_at DESC) = 1;