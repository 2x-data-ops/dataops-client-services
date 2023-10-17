---bings_ads_performance

--Keywords Performance
CREATE OR REPLACE TABLE quantum.bing_keyword_performance AS
WITH keywords AS(
    SELECT*EXCEPT(_rank) FROM (
        SELECT
            adgroupid, 
            keywords.timeperiod, 
            keywords.campaignname, 
            keywords.campaignid, 
            addistribution, /* ads.*EXCEPT(adid), */
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
        FROM `x-marketing.quantum_bing_ads.keyword_performance_report` keywords
        )
    WHERE _rank =1
)/*, airtable AS (
    SELECT *EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) FROM `x-marketing.quantum_mysql.db_airtable_ads` WHERE _platform = 'Bing'
)*/, budget AS (
    SELECT id AS campaignid, dailybudget FROM `x-marketing.quantum_bing_ads.campaigns`
)
SELECT 
--airtable._screenshot, 
keywords.*, 
budget.dailybudget 
FROM keywords 
--JOIN airtable ON keywords.adgroupid = airtable._adid
JOIN budget ON keywords.campaignid = budget.campaignid
;

--Ads Variation Performance
CREATE OR REPLACE TABLE quantum.bing_adsvariation_performance AS
WITH ads AS (
    SELECT* FROM (
        SELECT
            adgroupid, 
            titlepart1, 
            titlepart2, 
            titlepart3, 
            ads.campaignname, 
            ads.campaignid, 
            ads.adid, 
            ads.timeperiod, 
            ads.currencycode, 
            ads.spend AS cost, 
            ads.impressions , 
            ads.clicks, 
            CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
            ads.ctr, 
            ads.averagecpc AS avgcpc,
            ads.conversions , 
            ads.conversionrate ,
             ads._sdc_report_datetime,
            RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.quantum_bing_ads.ad_performance_report` ads
       )
    WHERE _rank =1
)
/*, airtable AS (
    SELECT *EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) FROM `x-marketing.quantum_mysql.db_airtable_ads` WHERE _platform = 'Bing'
)*/, budget AS (
    SELECT 
    id AS campaignid, 
    dailybudget FROM `x-marketing.quantum_bing_ads.campaigns`
)
SELECT 
--airtable._screenshot, 
ads.*EXCEPT(_rank), 
--budget.dailybudget 
FROM ads
--LEFT JOIN airtable ON ads.adgroupid = airtable._adid
--LEFT JOIN budget ON ads.campaignid = budget.campaignid
;



-- Campaign Performance
CREATE OR REPLACE TABLE quantum.bing_campaign_performance AS
WITH campaign AS(
    SELECT * EXCEPT(_rank) FROM (
        SELECT
            campaign.timeperiod, 
            campaign.campaignname, 
            campaign.campaignid, 
            addistribution,
            campaign.currencycode, 
            campaign.spend AS cost, 
            campaign.impressions , 
            campaign.clicks, 
            CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
            campaign.ctr, 
            campaign.averagecpc AS avgcpc,
            campaign.conversions , 
            campaign.conversionrate,
            RANK() OVER (PARTITION BY campaign.timeperiod, campaign.campaignid ORDER BY campaign._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.quantum_bing_ads.campaign_performance_report` campaign
        )
    WHERE _rank =1 
), budget AS (
    SELECT id AS campaignid, dailybudget FROM `x-marketing.quantum_bing_ads.campaigns`
), influence_conversion AS (
  SELECT campaign_id, 
  campaign, 
  counta_of_contact_link AS conversion_influence, 
  PARSE_TIMESTAMP('%F',created_date) AS created_date
  FROM `x-marketing.quantum_google_sheets.Pivot_Table_2`
), combined AS (
    SELECT campaign.*, 
    budget.dailybudget 
    FROM campaign
    JOIN budget ON campaign.campaignid = budget.campaignid
), combined_2 AS (
    SELECT *, 
    COUNT(campaignid) OVER (PARTITION BY timeperiod, campaignname) AS campaign_count
    FROM combined
), _all AS(
    SELECT combined_2.*, conversion_influence, campaign
    FROM combined_2 
    LEFT JOIN influence_conversion c ON c.created_date = combined_2.timeperiod AND c.campaign_id = combined_2.campaignid
)
    SELECT * EXCEPT(conversion_influence), CASE WHEN campaign_count > 0 THEN conversion_influence / campaign_count ELSE 0 END AS conv_influence
    FROM _all
;


--Ad Group Performance
CREATE OR REPLACE TABLE quantum.bing_adgroup_performance AS
WITH adgroups AS (
    SELECT * EXCEPT(_rank) FROM (
        SELECT
            adgroups.timeperiod, 
            adgroups.adgroupid, 
            addistribution, /* ads.*EXCEPT(adid), */ 
            campaignid,
            adgroups.currencycode, 
            adgroups.allreturnonadspend AS cost, 
            adgroups.impressions , 
            adgroups.clicks, 
            CAST(REPLACE(adgroups.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
            adgroups.ctr, 
            adgroups.averagecpc AS avgcpc,
            adgroups.conversions , 
            adgroups.conversionrate,
            RANK() OVER (PARTITION BY adgroups.timeperiod, adgroups.adgroupid ORDER BY adgroups._sdc_report_datetime DESC) AS _rank
        FROM `x-marketing.quantum_bing_ads.ad_group_performance_report` adgroups
        )
    WHERE _rank =1 /* AND adgroupsname LIKE '%2X%' */
)/*, airtable AS (
    SELECT *EXCEPT(_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version) FROM `x-marketing.quantum_mysql.db_airtable_ads` WHERE _platform = 'Bing'
)*/, budget AS (
    SELECT id AS campaignid, dailybudget FROM `x-marketing.quantum_bing_ads.campaigns`
)
SELECT 
--airtable._screenshot, 
adgroups.*EXCEPT(campaignid), 
budget.dailybudget
FROM adgroups
--JOIN airtable ON airtable._adid = adgroups.adgroupid
JOIN budget ON adgroups.campaignid = budget.campaignid;