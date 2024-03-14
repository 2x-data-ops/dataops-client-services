/* 0. Update campaign name 
UPDATE `x-marketing.tecsys_mysql.tecsys_db_campaign_info` origin
SET origin._campaignName = scenario._campaignName
FROM (
    SELECT
        _campaignid,
        -- _campaignName,
        CASE 
            WHEN _campaignname LIKE "%Jan%"
            THEN SUBSTRING(_campaignname,1,length(_campaignname)-5)
            WHEN _campaignName LIKE "%Feb%"
            THEN SUBSTRING(_campaignName,1,length(_campaignName)-5)
            ELSE _campaignName
        END AS _campaignName
    FROM    
    `x-marketing.tecsys_mysql.tecsys_db_campaign_info`
) scenario
WHERE origin._campaignid = scenario._campaignid;
*/



/* 1. Target Accounts */

CREATE OR REPLACE TABLE `tecsys_6sense.tecsys_db_target_accounts2` AS 
SELECT DISTINCT
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    campaign._campaigntype AS campaignType,
    -- campaign._campaignregion AS campaignRegion,
    -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    other._sdc_sequence AS _sdc_sequence,
    other._6sensecompanyname company,
    other._6sensecountry AS country,
    other._6sensedomain AS domain,
    other._6senseemployeerange AS employeeRange,
    other._6senserevenuerange AS revenueRange,
    other._industry AS industry
FROM `x-marketing.tecsys_mysql.tecsys_db_target_accounts` AS other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid;

/* 2. Reached Accounts */

CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_reached_accounts2 AS 
SELECT DISTINCT
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    PARSE_DATE('%e/%m/%Y', campaign._campaignMonth) AS campaignDate,
    campaign._campaigntype AS campaignType,
    -- campaign._campaignregion AS campaignRegion,
    -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    other._sdc_sequence AS _sdc_sequence,
    other._6sensecompanyname AS company,
    other._6sensecountry AS country,
    other._6sensedomain AS domain,
    CAST(REPLACE(other._impressions, ',', '') AS INT) AS impressions,
    CAST(REPLACE(other._clicks, ',', '') AS INT) AS clicks,
    CAST(REPLACE(other._spend, '$', '') AS DECIMAL) AS spend,
    other._websiteengagement AS websiteEngagement,
    CASE 
        WHEN other._latestimpression LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._latestimpression) 
        WHEN other._latestimpression LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._latestimpression) 
    END AS latestImpression,
    CAST(REPLACE(other._influencedformfills, ',', '') AS INT) AS influencedFormFills
FROM `x-marketing.tecsys_mysql.tecsys_db_reached_accounts` AS other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid;

/* 3. Comparison Chart */

CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_comparison_chart2 AS 
SELECT DISTINCT
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    campaign._campaigntype AS campaignType,
    -- campaign._campaignregion AS campaignRegion,
    -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    other._sdc_sequence AS _sdc_sequence,
    CASE 
        WHEN other._date LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._date) 
        WHEN other._date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', other._date) 
    END AS date,
    CAST(other._accountreached AS INT) AS accountReached,
    CAST(other._impression AS INT) AS impression,
    CAST(other._clicks AS INT) AS clicks,
    CAST(REPLACE(other._spend, '$', '') AS DECIMAL) AS spend,
    CAST(REPLACE(other._cpm, '$', '') AS DECIMAL) AS cpm,
    CAST(REPLACE(other._cpc, '$', '') AS DECIMAL) AS cpc
FROM `x-marketing.tecsys_mysql.tecsys_db_comparison_chart` AS other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid;

/* 4. Ads Overview */

CREATE OR REPLACE TABLE `x-marketing.tecsys_6sense.tecsys_db_ads_overview2` AS 
SELECT DISTINCT
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    campaign._campaigntype AS campaignType,
    PARSE_DATE('%e/%m/%Y', campaign._campaignMonth) AS campaignDate,
    -- campaign._campaignregion AS campaignRegion,
    -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    other._sdc_sequence AS _sdc_sequence,
    other._adgroup AS adGroup,
    other._adname AS adName,
    '' AS adVariation,
    '' AS adSize,
    '' AS dataType,
    other._state AS state,
    CASE 
        WHEN other._startdate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._startdate) 
        WHEN other._startdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._startdate) 
    END AS startDate,
    CASE 
        WHEN other._enddate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._enddate) 
        WHEN other._enddate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._enddate) 
    END AS endDate,
    CAST(REPLACE(other._accountsreached, ',', '') AS INT) AS accountsReached,
    CAST(REPLACE(other._impressions, ',', '') AS INT) AS impressions,
    CAST(REPLACE(other._clicks, ',', '') AS INT) AS clicks,
    CAST(REPLACE(other._ctr, '%', '') AS DECIMAL) / 100 AS ctr,
    CAST(REPLACE(other._actr, '%', '') AS DECIMAL) / 100 AS actr,
    CAST(REPLACE(other._cpm, '$', '') AS DECIMAL) AS cpm,
    CAST(REPLACE(other._cpc, '$', '') AS DECIMAL) AS cpc,
    CAST(REPLACE(other._vtr, '%', '') AS DECIMAL) / 100 AS vtr,
    CAST(REPLACE(other._avtr, '%', '') AS DECIMAL) / 100 AS avtr,
    CAST(REPLACE(REPLACE(other._budget, ',', ''), '$', '') AS DECIMAL) AS budget,
    CAST(REPLACE(REPLACE(other._spend, ',', ''), '$', '') AS DECIMAL) AS spend,
    ROW_NUMBER() OVER(
        PARTITION BY campaign._campaignname, PARSE_DATE('%e/%m/%Y', campaign._campaignMonth),other._adname,other._adgroup
        ORDER BY _extractDate DESC
    ) AS latestWeek
FROM `x-marketing.tecsys_mysql.tecsys_db_ads_overview` AS other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid;

/*
-- For Deleting past week data just for ads overview in a particular campaigns
DELETE
FROM `x-marketing.tecsys_mysql.tecsys_db_ads_overview` 
WHERE _campaignid IN ('122731','122732','122733')
AND _extractDate = '2023-06-01'
*/

/* Update Calculated Fields in Ads Overview */
/*
UPDATE `x-marketing.tecsys_6sense.tecsys_db_ads_overview`
SET adVariation = (
    CASE
        WHEN adName LIKE '%Elite WMS Awareness%' THEN SPLIT(adName, 'LIVE_22')[ORDINAL(1)]
        WHEN adName LIKE '%Elite WMS Consideration%' THEN SPLIT(adName, '_')[ORDINAL(1)]
        WHEN adName LIKE '%HC_Enterprise%' THEN REPLACE(REPLACE(SPLIT(adGroup, ') -')[ORDINAL(1)], '(', ''), ' ', '_')
        ELSE ''
    END
)    
WHERE adVariation = '';

UPDATE `x-marketing.tecsys_6sense.tecsys_db_ads_overview`
SET adSize = (
    CASE
        WHEN adName LIKE '%Elite WMS Awareness %' THEN SPLIT(RIGHT(adName, 10), '_')[ORDINAL(2)]
        WHEN adName LIKE '%Elite WMS Consideration%' THEN SPLIT(REPLACE(REPLACE(RIGHT(adName, 10), '--', '-'), 'px', ''), '-')[ORDINAL(2)]
        WHEN adName LIKE '%HC_Enterprise%' THEN REPLACE(REPLACE(RIGHT(adName, 7), '-', ''), 'o', '')
        ELSE ''
    END
)    
WHERE adSize = '';
*/
UPDATE `x-marketing.tecsys_6sense.tecsys_db_ads_overview2`
SET adSize = (
    CASE
        WHEN adName LIKE '%Elite WMS Awareness%' 
            -- THEN SPLIT(RIGHT(adName, 10), '_')[ORDINAL(2)]
            THEN SPLIT(RIGHT (adName,7), '_')[ORDINAL (1)]
        WHEN adName LIKE '%Elite WMS Consideration%' 
            THEN SPLIT(RIGHT (adName,7), '_')[ORDINAL (1)]
        ELSE ''
    END
)    
WHERE adSize = '';

UPDATE `x-marketing.tecsys_6sense.tecsys_db_ads_overview2`
SET dataType = (
    CASE
        WHEN adSize = '' THEN ''
        WHEN adSize = '728x90' THEN 'Desktop'
        ELSE 'Mobile-Friendly'
    END
)    
WHERE dataType = '';

/* 5. Job Level Function */
CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_job_level_function2 AS 
SELECT DISTINCT
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    campaign._campaigntype AS campaignType,
    -- campaign._campaignregion AS campaignRegion,
    -- PARSE_DATE('%e/%m/%Y', other._extractDate) AS extractDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    other._sdc_sequence AS _sdc_sequence,
    other._job AS job,
    other._jobLevel AS jobLevel_6sense,
    other._jobFunction AS jobFunction_6sense,
    CAST('' AS STRING) AS jobLevel,
    CAST('' AS STRING) AS jobFunction,
    CAST(REPLACE(other._accountsReached, ',', '') AS INT) AS accountsReached,
    CAST(REPLACE(other._impressions, ',', '') AS INT) AS impressions,
    CAST(REPLACE(other._clicks, ',', '') AS INT) AS clicks
-- FROM `x-marketing.tecsys_mysql.tecsys_db_job_level_function_new` AS other
FROM `x-marketing.tecsys_mysql.tecsys_db_job_level_function` other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid;



/* Update Calculated Fields in Job Level Function */

UPDATE `x-marketing.tecsys_6sense.tecsys_db_job_level_function2`
SET jobLevel = (
    CASE
        WHEN job LIKE '%CXO%' THEN 'C-Suite'
        WHEN job LIKE '%Vice President%' THEN 'VP'
        WHEN job LIKE '%Director%' THEN 'Director'
        WHEN job LIKE '%Manager%' THEN 'Manager'
        WHEN job LIKE '%Senior%' THEN 'Senior'
    END
)
WHERE job IS NOT NULL;

/* 6. Device Type Distribution */

CREATE OR REPLACE TABLE `x-marketing.tecsys_6sense.tecsys_db_device_type_distribution2` AS 
SELECT *
FROM (
    SELECT DISTINCT
        campaign._campaignid AS campaignID,
        CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
        -- campaign._campaignindustry AS campaignIndustry,
        -- campaign._campaignobjective AS campaignObjective,
        campaign._campaigntype AS campaignType,
        -- campaign._campaignregion AS campaignRegion,
        -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
        CASE 
            WHEN other._extractdate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._extractdate) 
            WHEN other._extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._extractdate) 
        END AS extractDate,
        other._sdc_sequence AS _sdc_sequence,
        other._devicetype AS deviceType,
        CAST(REPLACE(other._accountsreached, ',', '') AS INT) AS accountsReached,
        CAST(REPLACE(other._impressions, ',', '') AS INT) AS impressions,
        CAST(REPLACE(other._clicks, ',', '') AS INT) AS clicks,
        ROW_NUMBER() OVER(
            PARTITION BY campaign._campaignid, _deviceType
            ORDER BY (CASE 
                WHEN other._extractdate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._extractdate) 
                WHEN other._extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._extractdate) 
            END) DESC
        ) AS rownum
    FROM `x-marketing.tecsys_mysql.tecsys_db_device_type_distribution` AS other
    JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
    ON other._campaignid = campaign._campaignid
)
WHERE rownum = 1;

/* 7. Summary Data */

CREATE OR REPLACE TABLE `tecsys_6sense.tecsys_db_summary_data2` AS 
SELECT 
    * EXCEPT(rownum)
    FROM (
    SELECT 
        campaign._campaignid AS campaignID,
        CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
        -- campaign._campaignindustry AS campaignIndustry,
        -- campaign._campaignobjective AS campaignObjective,
        campaign._campaigntype AS campaignType,
        -- campaign._campaignregion AS campaignRegion,
        -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
        CASE 
            WHEN other._extractdate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._extractdate) 
            WHEN other._extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._extractdate) 
        END AS extractDate,
        other._sdc_sequence AS _sdc_sequence,
        0 AS targetAccounts,
        0 AS reachedAccounts,
        0 AS clickedAccounts,
        0 AS retargetedAccounts,
        CASE
            WHEN other._totalspent LIKE "%/%"
            THEN SAFE_CAST(REPLACE(REPLACE(LEFT(other._totalspent, STRPOS(other._totalspent, '/') - 1), ',', ''), '$', '') AS DECIMAL)
            ELSE SAFE_CAST(other._totalspent AS DECIMAL)
        END AS totalSpent,
        SAFE_CAST(REPLACE(other._accountctr, '%', '') AS DECIMAL) / 100 AS accountCtr,
        SAFE_CAST(REPLACE(other._accountvtr, '%', '') AS DECIMAL) / 100 AS accountVtr,
        SAFE_CAST(SUBSTR(other._avgincreaseinaccountengagement, 1, 4) AS DECIMAL) AS avgIncreaseInAccountEngagement,
        SAFE_CAST(REPLACE(other._accountsnewlyengaged, ',', '') AS INT) AS accountsNewlyEngaged,
        SAFE_CAST(REPLACE(other._accountswithincreasedengagement, ',', '') AS INT) AS accountsWithIncreasedEngagement,
        SAFE_CAST(REPLACE(other._ctr, '%', '') AS DECIMAL) / 100 AS ctr,
        SAFE_CAST(REPLACE(other._vtr, '%', '') AS DECIMAL) / 100 AS vtr,
        SAFE_CAST(REPLACE(other._impressions, ',', '') AS INT) AS impressions,
        SAFE_CAST(REPLACE(other._clicks, ',', '') AS INT) AS clicks,
        CASE
            WHEN other._ecpc != ''
            THEN SAFE_CAST(REPLACE(other._ecpc, '$', '') AS DECIMAL)
            ELSE 0
        END AS ecpc,
        SAFE_CAST(REPLACE(other._ecpm, '$', '') AS DECIMAL) AS ecpm,
        SAFE_CAST(REPLACE(other._views, ',', '') AS INT) AS views,
        SAFE_CAST(REPLACE(other._influencedformfills, ',', '') AS INT) AS influencedFormFills,
        ROW_NUMBER() OVER(
            PARTITION BY campaign._campaignid 
            ORDER BY (
                CASE 
                    WHEN other._extractdate LIKE '%-%' THEN PARSE_DATE('%Y-%m-%d', other._extractdate) 
                    WHEN other._extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', other._extractdate) 
                END
            ) DESC
        ) rownum
    FROM `x-marketing.tecsys_mysql.tecsys_db_summary_data` AS other
    JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
    ON other._campaignid = campaign._campaignid
)
WHERE rownum = 1;

/* Update Calculated Fields in Summary Data */

UPDATE `x-marketing.tecsys_6sense.tecsys_db_summary_data2` sum
SET sum.targetAccounts = sub.count
FROM (
    SELECT extractDate, campaignID, COUNT(*) AS count
    FROM `x-marketing.tecsys_6sense.tecsys_db_target_accounts2`
    GROUP BY 1, 2
) sub
WHERE sum.extractDate = sub.extractDate
AND sum.campaignID = sub.campaignID;


UPDATE `x-marketing.tecsys_6sense.tecsys_db_summary_data2` sum
SET sum.reachedAccounts = sub.count
FROM (
    SELECT extractDate, campaignID, COUNT(*) AS count
    FROM `x-marketing.tecsys_6sense.tecsys_db_reached_accounts2`
    GROUP BY 1, 2
) sub
WHERE sum.extractDate = sub.extractDate
AND sum.campaignID = sub.campaignID;


UPDATE `x-marketing.tecsys_6sense.tecsys_db_summary_data2` sum
SET sum.clickedAccounts = sub.count
FROM (
    SELECT extractDate, campaignID, COUNT(*) AS count
    FROM `x-marketing.tecsys_6sense.tecsys_db_reached_accounts2`
    WHERE clicks > 0
    GROUP BY 1, 2
) sub
WHERE sum.extractDate = sub.extractDate
AND sum.campaignID = sub.campaignID;

/* 8. Buying Stage Accounts */

CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_buying_stage_accounts2 AS 
WITH main_info AS (
    SELECT DISTINCT
        campaign._campaignid AS campaignID,
        CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
        -- campaign._campaignindustry AS campaignIndustry,
        -- campaign._campaignobjective AS campaignObjective,
        campaign._campaigntype AS campaignType,
        -- campaign._campaignregion AS campaignRegion,
        CASE
            WHEN other._extractdate LIKE "%/%"
            THEN PARSE_DATE('%e/%m/%Y', other._extractdate)
            ELSE PARSE_DATE('%Y-%m-%e', other._extractdate)
        END AS extractDate,
        -- PARSE_DATE('%e/%m/%Y', other._extractDate) AS extractDate,
        other._sdc_sequence AS _sdc_sequence,
        other._6sensecompanyname AS company,
        other._6sensecountry AS country,
        other._6sensedomain AS domain,
        other._buyingstagestart AS buyingStageStart,
        other._buyingstageend AS buyingStageEnd,
        other._maxengagementstatestart AS maxEngagementStateStart,
        other._maxengagementstateend AS maxEngagementStateEnd,
        CASE
            WHEN other._newpipelineusd = '' THEN 0.00
            ELSE CAST(other._newpipelineusd AS DECIMAL) 
        END AS newPipeline,
        CASE
            WHEN other._totalwonusd = '' THEN 0.00
            ELSE CAST(other._totalwonusd AS DECIMAL) 
        END AS totalWon,
        CONCAT(other._buyingstagestart, other._buyingstageend) AS currentCombo
    -- FROM `x-marketing.tecsys_mysql.tecsys_db_buying_stage_accounts` AS other
    FROM `x-marketing.tecsys_mysql.tecsys_db_buying_stage_accounts` AS other
    JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
    ON other._campaignid = campaign._campaignid
    ORDER BY other._6sensedomain
),
previous_combos AS (
    SELECT
        *,
        LAG(currentCombo) OVER(
            PARTITION BY campaignID, company, country
            ORDER BY extractDate
        ) previousCombo
    FROM main_info
),
numberOfWeeksPurchase AS (
    SELECT
        company AS purchaseCompany,
        -- campaignID AS _campaignID,
        COUNT(1) AS totalWeekPurchase
    FROM main_info
    WHERE buyingStageStart = 'Purchase'
    GROUP BY company
),
numberOfWeeksDecision AS (
    SELECT
        company AS decisionCompany,
        -- campaignID AS _campaignID,
        COUNT(1) AS totalWeekDecision
    FROM main_info
    WHERE buyingStageStart = 'Decision'
    GROUP BY company
),
match_combos AS (
    SELECT
        *,
        SUM(CASE WHEN currentCombo != previousCombo THEN 1 ELSE 0 END) OVER(
            PARTITION BY campaignID, company, country
            ORDER BY extractDate
        ) breakPoint
    FROM previous_combos
),
partition_by_groupID AS (
    SELECT
        * EXCEPT(currentCombo, previousCombo, breakPoint),
        ROW_NUMBER() OVER(
            PARTITION BY campaignID, company, country, breakPoint
            ORDER BY extractDate
        ) AS stagnantWeekCount
    FROM match_combos
)
SELECT 
  * EXCEPT(purchaseCompany,decisionCompany)
FROM partition_by_groupID
LEFT JOIN numberOfWeeksPurchase ON purchaseCompany = company
LEFT JOIN numberOfWeeksDecision ON decisionCompany = company;

/* 9. Buying Stage Cohort */

CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_buying_stage_cohort2 AS
SELECT
    campaignID,
    campaignName,
    -- campaignIndustry,
    -- campaignObjective,
    campaignType,
    -- campaignRegion,
    extractDate,
    0 AS buyingStageStartOrder,
    buyingStageStart,
    SUM(CASE WHEN buyingStageEnd = 'Target' THEN accounts ELSE NULL END) AS target,
    SUM(CASE WHEN buyingStageEnd = 'Awareness' THEN accounts ELSE NULL END) AS awareness,
    SUM(CASE WHEN buyingStageEnd = 'Consideration' THEN accounts ELSE NULL END) AS consideration,
    SUM(CASE WHEN buyingStageEnd = 'Decision' THEN accounts ELSE NULL END) AS decision,
    SUM(CASE WHEN buyingStageEnd = 'Purchase' THEN accounts ELSE NULL END) AS purchase
FROM (
    SELECT 
        campaignID,
        campaignName,
        -- campaignIndustry,
        -- campaignObjective,
        campaignType,
        -- campaignRegion,
        extractDate,
        buyingStageStart,
        buyingStageEnd,
        COUNT(*) AS accounts
    FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_accounts2`
    GROUP BY 1, 2, 3, 4, 5, 6
)
GROUP BY 1, 2, 3, 4, 5, 6;


/* Misc. Buying Stage Cohort Check */

CREATE OR REPLACE TABLE tecsys_6sense.tecsys_db_buying_stage_cohort_check2 AS
SELECT
    campaignID,
    campaignName,
    -- campaignIndustry,
    -- campaignObjective,
    campaignType,
    -- campaignRegion,
    extractDate,
    SUM(CASE WHEN buyingStageStart = 'Target' THEN 1 ELSE NULL END) AS target,
    SUM(CASE WHEN buyingStageStart = 'Awareness' THEN 1 ELSE NULL END) AS awareness,
    SUM(CASE WHEN buyingStageStart = 'Consideration' THEN 1 ELSE NULL END) AS consideration,
    SUM(CASE WHEN buyingStageStart = 'Decision' THEN 1 ELSE NULL END) AS decision,
    SUM(CASE WHEN buyingStageStart = 'Purchase' THEN 1 ELSE NULL END) AS purchase
FROM (
    SELECT DISTINCT
        campaignID,
        campaignName,
        -- campaignIndustry,
        -- campaignObjective,
        campaignType,
        -- campaignRegion,
        extractDate,
        buyingStageStart
    FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2`
    WHERE buyingStageStartOrder = 0
)
GROUP BY 1, 2, 3, 4;

/* Insert Missing Rows In Buying Stage Cohort */

INSERT INTO `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` 
(
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    buyingStageStartOrder, 
    buyingStageStart
)
SELECT 
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    0, 
    'Target' 
FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort_check2` WHERE target IS NULL;

INSERT INTO `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` 
(
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    buyingStageStartOrder, 
    buyingStageStart
)
SELECT 
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    0, 
    'Awareness' 
FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort_check2` WHERE awareness IS NULL;

INSERT INTO `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` 
(
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    buyingStageStartOrder, 
    buyingStageStart
)
SELECT 
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate,  
    0, 
    'Consideration' 
FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort_check2` WHERE consideration IS NULL;

INSERT INTO `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` 
(
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    buyingStageStartOrder, 
    buyingStageStart
)
SELECT 
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    0, 
    'Decision' 
FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort_check2` WHERE decision IS NULL;

INSERT INTO `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` 
(
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    buyingStageStartOrder, 
    buyingStageStart
)
SELECT 
    campaignID, 
    campaignName, 
    -- campaignIndustry, 
    -- campaignObjective, 
    campaignType, 
    -- campaignRegion, 
    extractDate, 
    0, 
    'Purchase' 
FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort_check2` WHERE purchase IS NULL;

/* Update Row Order In Buying Stage Cohort */

UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` SET buyingStageStartOrder = 1 WHERE buyingStageStart = 'Target';
UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` SET buyingStageStartOrder = 2 WHERE buyingStageStart = 'Awareness';
UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` SET buyingStageStartOrder = 3 WHERE buyingStageStart = 'Consideration';
UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` SET buyingStageStartOrder = 4 WHERE buyingStageStart = 'Decision';
UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage_cohort2` SET buyingStageStartOrder = 5 WHERE buyingStageStart = 'Purchase';

/* 10. Buying Stage */

CREATE OR REPLACE TABLE `x-marketing.tecsys_6sense.tecsys_db_buying_stage2` AS
SELECT 
    campaign._campaignid AS campaignID,
    CASE
      WHEN campaign._campaignname LIKE "%EXTEND%"
      THEN REPLACE(campaign._campaignname, ' EXTEND', '')
      ELSE campaign._campaignname
    END AS campaignName,
    -- campaign._campaignname AS campaignName,
    -- campaign._campaignindustry AS campaignIndustry,
    -- campaign._campaignobjective AS campaignObjective,
    campaign._campaigntype AS campaignType,
    -- campaign._campaignregion AS campaignRegion,
    PARSE_DATE('%e/%m/%Y', campaign._campaignMonth) AS campaignDate,
    CASE
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{2}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%m/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{2}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%d/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}[-/]\d{1}[-/]\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', REPLACE(other._extractdate, '-', '/'))
        WHEN REGEXP_CONTAINS(other._extractdate, r'^\d{1}/\d{1}/\d{4}$') THEN
            PARSE_DATE('%e/%e/%Y', other._extractdate)
        ELSE
            DATE(_extractDate)
    END AS extractDate,
    -- PARSE_DATE('%e/%m/%Y', other._extractdate) AS extractDate,
    _timeframe  AS timeframe,
    CASE
        WHEN _timeframe != 'Dec-22'
        THEN (
            CASE
                WHEN LEFT(other._timeframe, STRPOS(other._timeframe, ' - ')) LIKE '%,%' THEN PARSE_DATE('%h %e, %Y', LEFT(other._timeframe, STRPOS(other._timeframe, ' - ')))
                ELSE PARSE_DATE('%h %e %Y', CONCAT(LEFT(other._timeframe, STRPOS(other._timeframe, ' - ')), ' ', RIGHT(other._timeframe, 4)))
            END 
        ) ELSE '2022-12-01'
    END AS startDate,
    CAST(NULL AS DATE) AS endDate,
    CAST(NULL AS STRING) AS latestWeek,
    SUM(CASE WHEN other._buyingstage = 'Target' AND other._numberofaccounts != '' THEN CAST(other._numberofaccounts AS INT) ELSE 0 END) AS targetStage,
    SUM(CASE WHEN other._buyingstage = 'Awareness' AND other._numberofaccounts != '' THEN CAST(other._numberofaccounts AS INT) ELSE 0 END) AS awarenessStage,
    SUM(CASE WHEN other._buyingstage = 'Consideration' AND other._numberofaccounts != '' THEN CAST(other._numberofaccounts AS INT) ELSE 0 END) AS considerationStage,
    SUM(CASE WHEN other._buyingstage = 'Decision' AND other._numberofaccounts != '' THEN CAST(other._numberofaccounts AS INT) ELSE 0 END) AS decisionStage,
    SUM(CASE WHEN other._buyingstage = 'Purchase' AND other._numberofaccounts != '' THEN CAST(other._numberofaccounts AS INT) ELSE 0 END) AS purchaseStage,
    SUM(CASE WHEN other._buyingstage = 'Target' AND other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS targetPipeline, 
    SUM(CASE WHEN other._buyingstage = 'Awareness' AND other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS awarenessPipeline,
    SUM(CASE WHEN other._buyingstage = 'Consideration' AND other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS considerationPipeline,
    SUM(CASE WHEN other._buyingstage = 'Decision' AND other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS decisionPipeline,
    SUM(CASE WHEN other._buyingstage = 'Purchase' AND other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS purchasePipeline,
    SUM(CASE WHEN other._buyingstage = 'Target' AND other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS targetWon,
    SUM(CASE WHEN other._buyingstage = 'Awareness' AND other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS awarenessWon,
    SUM(CASE WHEN other._buyingstage = 'Consideration' AND other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS considerationWon,
    SUM(CASE WHEN other._buyingstage = 'Decision' AND other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS decisionWon,
    SUM(CASE WHEN other._buyingstage = 'Purchase' AND other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS purchaseWon,
    SUM(CASE WHEN other._newpipelineusd != '' THEN CAST(other._newpipelineusd AS DECIMAL) ELSE 0 END) AS totalPipeline,
    SUM(CASE WHEN other._totalwonusd != '' THEN CAST(other._totalwonusd AS DECIMAL) ELSE 0 END) AS totalWon,
    CAST(0 AS INT) AS previousTargetStage,
    CAST(0 AS INT) AS previousAwarenessStage,
    CAST(0 AS INT) AS previousConsiderationStage,
    CAST(0 AS INT) AS previousDecisionStage,
    CAST(0 AS INT) AS previousPurchaseStage,
    CAST(0 AS NUMERIC) AS targetChange,
    CAST(0 AS NUMERIC) AS awarenessChange,
    CAST(0 AS NUMERIC) AS considerationChange,
    CAST(0 AS NUMERIC) AS decisionChange,
    CAST(0 AS NUMERIC) AS purchaseChange,
    -- ROW_NUMBER() OVER(
    --     PARTITION BY campaign._campaignid
    --     ORDER BY other._extractdate DESC
    -- ) AS rownum
FROM `x-marketing.tecsys_mysql.tecsys_db_buying_stage` AS other
JOIN `x-marketing.tecsys_mysql.tecsys_db_campaign_info` AS campaign
ON other._campaignid = campaign._campaignid
GROUP BY 1,2,3,4,5,6,7;
/* Update end date of the week */

UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage2` 
SET endDate = DATE_ADD(startDate, INTERVAL 6 DAY)
WHERE endDate IS NULL;

/* Calculate the changes on a weekly basis */

UPDATE `x-marketing.tecsys_6sense.tecsys_db_buying_stage2` main
SET main.previousTargetStage = sub.previousTargetStage,
    main.previousAwarenessStage = sub.previousAwarenessStage,
    main.previousConsiderationStage = sub.previousConsiderationStage,
    main.previousDecisionStage = sub.previousDecisionStage,
    main.previousPurchaseStage = sub.previousPurchaseStage,
    main.targetChange = CAST(sub.targetChanges AS DECIMAL),
    main.awarenessChange = CAST(sub.awarenessChanges AS DECIMAL),
    main.considerationChange = CAST(sub.considerationChanges AS DECIMAL),
    main.decisionChange = CAST(sub.decisionChanges AS DECIMAL),
    main.purchaseChange = CAST(sub.purchaseChanges AS DECIMAL),
    main.latestWeek = CAST(sub.latestWeek AS STRING)
FROM (
    SELECT DISTINCT
        after.campaignID,
        after.extractDate,
        after.campaignName,
        after.timeframe,
        before.targetStage AS previousTargetStage,
        before.awarenessStage AS previousAwarenessStage,
        before.considerationStage AS previousConsiderationStage,
        before.decisionStage AS previousDecisionStage, 
        before.purchaseStage AS previousPurchaseStage,
        CASE 
            WHEN before.targetStage != 0 
            THEN (after.targetStage - before.targetStage) / before.targetStage 
            ELSE (after.targetStage - before.targetStage) / 1 
        END AS targetChanges,
        CASE 
            WHEN before.awarenessStage != 0 
            THEN (after.awarenessStage - before.awarenessStage) / before.awarenessStage 
            ELSE (after.awarenessStage - before.awarenessStage) / 1 
        END AS awarenessChanges,
        CASE 
            WHEN before.considerationStage != 0 
            THEN (after.considerationStage - before.considerationStage) / before.considerationStage 
            ELSE (after.considerationStage - before.considerationStage) / 1 
        END AS considerationChanges,
        CASE 
            WHEN before.decisionStage != 0 
            THEN (after.decisionStage - before.decisionStage) / before.decisionStage 
            ELSE (after.decisionStage - before.decisionStage) / 1 
        END AS decisionChanges,
        CASE 
            WHEN before.purchaseStage != 0 
            THEN (after.purchaseStage - before.purchaseStage) / before.purchaseStage 
            ELSE (after.purchaseStage - before.purchaseStage) / 1 
        END AS purchaseChanges,
        ROW_NUMBER() OVER(
            PARTITION BY after.campaignName, after.campaignDate
            ORDER BY after.extractDate DESC
        ) AS latestWeek
    FROM `x-marketing.tecsys_6sense.tecsys_db_buying_stage2` after 
    JOIN `x-marketing.tecsys_6sense.tecsys_db_buying_stage2` before
    ON 
    after.startDate = DATE_ADD(before.startDate, INTERVAL 7 DAY)
    AND 
    after.campaignName = before.campaignName
) sub
WHERE main.extractDate = sub.extractDate
AND main.campaignName = sub.campaignName
AND main.timeframe = sub.timeframe;





