---bing keyword performance
TRUNCATE TABLE `x-marketing.emburse.bing_keyword_performance`;
INSERT INTO `x-marketing.emburse.bing_keyword_performance` (
  adgroupid,	
  timeperiod,	
  campaignname,	
  campaign_country_region,	
  campaignid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  ctr,	
  avgcpc,	
  conversions,	
  conversionrate,	
  deliveredmatchtype,	
  keyword,	
  qualityscore,	
  bidmatchtype,	
  dailybudget	
)
WITH keywords AS (
  SELECT
    adgroupid, 
    keywords.timeperiod, 
    keywords.campaignname,
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
    END AS campaign_country_region,
    keywords.campaignid, 
    addistribution,
    keywords.currencycode, 
    keywords.spend AS cost, 
    keywords.impressions , 
    keywords.clicks, 
    CAST(REPLACE(keywords.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
    keywords.ctr, 
    keywords.averagecpc AS avgcpc,
    keywords.conversions , 
    keywords.conversionrate , 
    keywords.deliveredmatchtype, 
    keywords.keyword, 
    keywords.qualityscore, 
    bidmatchtype
  FROM `x-marketing.emburse_bing_ads.keyword_performance_report` keywords
  QUALIFY RANK() OVER (
    PARTITION BY keywords.timeperiod, keywords.keywordid
    ORDER BY keywords._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  keywords.*, 
  budget.dailybudget 
FROM keywords 
JOIN budget 
  ON keywords.campaignid = budget.campaignid;

---bing ads variation performance
TRUNCATE TABLE `x-marketing.emburse.bing_adsvariation_performance`;
INSERT INTO `x-marketing.emburse.bing_adsvariation_performance` (
  adgroupid,
  titlepart1,	
  titlepart2,	
  titlepart3,	
  campaignname,	
  campaign_country_region,	
  campaignid,
  adid,
  timeperiod,	
  currencycode,	
  cost,	
  impressions,
  clicks,
  AbsoluteTopImpressionRatePercent,	
  conversions,	
  conversionrate,	
  _sdc_report_datetime,	
  dailybudget
)
WITH ads AS (
  SELECT
    adgroupid, 
    titlepart1, 
    titlepart2, 
    titlepart3, 
    ads.campaignname,
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
    END AS campaign_country_region, 
    ads.campaignid, 
    ads.adid, 
    ads.timeperiod, 
    ads.currencycode, 
    ads.spend AS cost, 
    ads.impressions , 
    ads.clicks, 
    CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
    ads.conversions, 
    ads.conversionrate,
    ads._sdc_report_datetime
  FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  ads.*, 
  budget.dailybudget 
FROM ads
JOIN budget
  ON ads.campaignid = budget.campaignid;

---Bing Campaign Performance
TRUNCATE TABLE `x-marketing.emburse.bing_campaign_performance`;
INSERT INTO `x-marketing.emburse.bing_campaign_performance` (
  timeperiod,	
  campaignname,	
  campaign_country_region,	
  campaignid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  conversions,	
  campaign_status,
  dailybudget
)
WITH campaign AS (
  SELECT
    campaign.timeperiod, 
    campaign.campaignname,
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
    END AS campaign_country_region, 
    campaign.campaignid, 
    addistribution,
    campaign.currencycode, 
    campaign.spend AS cost,
    campaign.impressions,
    campaign.clicks,
    CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64) AS AbsoluteTopImpressionRatePercent, 
    campaign.conversions,
    campaign.campaignstatus AS campaign_status
  FROM `x-marketing.emburse_bing_ads.campaign_performance_report` campaign
  QUALIFY RANK() OVER (PARTITION BY campaign.timeperiod, campaign.campaignid ORDER BY campaign._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  campaign.*, 
  budget.dailybudget 
FROM campaign
JOIN budget 
  ON campaign.campaignid = budget.campaignid;

---Bing Ad Group Performance
TRUNCATE TABLE `x-marketing.emburse.bing_adgroup_performance`;
INSERT INTO `x-marketing.emburse.bing_adgroup_performance` (
  timeperiod,	
  adgroupid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  ctr,	
  avgcpc,	
  conversions,	
  conversionrate,	
  dailybudget
)
WITH adgroups AS (
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
    adgroups.conversionrate
  FROM `x-marketing.emburse_bing_ads.ad_group_performance_report` adgroups
  QUALIFY RANK() OVER (PARTITION BY adgroups.timeperiod, adgroups.adgroupid ORDER BY adgroups._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  adgroups.* EXCEPT (campaignid), 
  budget.dailybudget
FROM adgroups
JOIN budget 
  ON adgroups.campaignid = budget.campaignid;

---Bing Search Query Performance
TRUNCATE TABLE `x-marketing.emburse.bing_search_query_performance`;
INSERT INTO `x-marketing.emburse.bing_search_query_performance` (
  campaignid,	
  campaignname,	
  campaign_country_region,	
  adgroupid,	
  adgroupname,	
  keyword,	
  search_term,	
  day,	
  cost,	
  impressions,	
  clicks,	
  conversions,	
  campaignstatus,	
  adgroupstatus
)
SELECT
  campaignid,
  campaignname,
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
  END AS campaign_country_region,
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
  adgroupstatus
FROM `x-marketing.emburse_bing_ads.search_query_performance_report`
QUALIFY RANK() OVER (PARTITION BY timeperiod, campaignid, adgroupid, keyword, searchquery ORDER BY _sdc_received_at DESC) = 1;