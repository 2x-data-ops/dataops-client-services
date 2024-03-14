-- CREATE OR REPLACE TABLE `devo.linkedin_ads_performance` AS

-- WITH LI_ads AS (
--   SELECT
--     creative_id,
--     start_at AS _startDate,
--     one_click_leads AS _leads,
--     card_impressions AS _reach,
--     cost_in_usd AS _spent,
--     impressions AS _impressions,
--     clicks AS _clicks,
--     external_website_conversions AS _conversions
--   FROM
--     `devo_linkedin_ads.ad_analytics_by_creative`
-- ),
-- ads_title AS (
--   SELECT
--     SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
--     campaign_id
--   FROM
--     `devo_linkedin_ads.creatives`
-- ),
-- campaigns AS (
--   SELECT
--     id AS campaignID,
--     name AS _campaignName,
--     status,
--     cost_type,
--     daily_budget.amount AS dailyBudget,
--     campaign_group_id
--   FROM
--     `devo_linkedin_ads.campaigns`
-- ),
-- -- campaign_group AS (
-- --   SELECT
-- --     id AS groupID,
-- --     name AS _groupName,
-- --     status
-- --   FROM
-- --     `devo_linkedin_ads.campaign_groups`
-- -- ),
-- _all AS (
-- SELECT
--   LI_ads.*,
--   campaigns.campaignID,
--   campaigns._campaignName,
--   -- campaign_group.groupID,
--   -- campaign_group._groupName,
--   campaigns.dailyBudget,
--   campaigns.cost_type,
--   -- campaign_group.status
-- FROM
--   LI_ads
-- RIGHT JOIN
--   ads_title
-- ON
--   CAST(LI_ads.creative_id AS STRING) = ads_title.cID
-- JOIN
--   campaigns
-- ON
--   ads_title.campaign_id = campaigns.campaignID
-- -- JOIN
-- --   campaign_group
-- -- ON
-- --   campaigns.campaign_group_id = campaign_group.groupID
-- ), total_ads AS (
--   SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
--   FROM _all
-- )
-- , daily_budget_per_ad_per_campaign AS (
--   SELECT *,
--           CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
--         ELSE 0 
--         END
--          AS dailyBudget_per_ad
--   FROM total_ads
-- ) SELECT * FROM daily_budget_per_ad_per_campaign;




--ads optimization
TRUNCATE TABLE `devo.dashboard_optimization_ads` ;
INSERT INTO `x-marketing.devo.dashboard_optimization_ads` (
  _adid, 
  _adname, 
  _adcopy, 
  _screenshot, 
  _ctacopy, 
  _designtemplate, 
  _size, 
  _platform, 
  _segment, 
  _designcolor,
  _designimages,
  _designblurp,
  _logos,
  _copymessaging,
  _copyassettype,
  _copytone,
  _copyproductcompanyname,
  _copystatisticproofpoint,
  _ctacopysofthard,  
  _date, 
  _spend, 
  _clicks, 
  _impressions,
  _campaignname,
  _campaignid,
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition
  )
WITH
  linkedin_ads AS (
    SELECT
      CAST(creative_id AS STRING) AS _adid,
      start_at AS _date,
      SUM(cost_in_usd) AS _spend, 
      SUM(clicks) AS _clicks, 
      SUM(impressions) AS _impressions, 
    FROM
      `x-marketing.devo_linkedin_ads.ad_analytics_by_creative`
    WHERE 
    start_at IS NOT NULL
    GROUP BY 
      creative_id, start_at
  ),
  ads_title AS (
    SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `x-marketing.devo_linkedin_ads.creatives`
  ),
  campaigns AS (
    SELECT 
    id AS _campaignID,
    name AS _campaignname
    FROM `x-marketing.devo_linkedin_ads.campaigns`
  ),
  linkedin_airtable AS (
    SELECT
      _adid, 
      _adname, 
      _adcopy, 
      _screenshot, 
      _ctacopy, 
      _designtemplate,
      _size, 
      _platform, 
      _segment,
      _designcolor,
      _designimages,
      _designblurp,
      _logos,
      _copymessaging,
      _copyassettype,
      _copytone,
      _copyproductcompanyname,
      _copystatisticproofpoint,
      _ctacopysofthard
    FROM
    `x-marketing.devo_mysql.optimization_airtable_ads_linkedin` 
    WHERE 
      /* _sdc_deleted_at IS NULL 
      AND */ LENGTH(_adid)>2
    GROUP BY ALL
  ),
  linkedin_combined AS (
    SELECT
      linkedin_airtable.*,
      linkedin_ads.* EXCEPT (_adid),
      campaigns._campaignname,
      campaigns._campaignID
    FROM 
      linkedin_ads
    JOIN
      linkedin_airtable ON linkedin_ads._adid = CAST(linkedin_airtable._adid AS STRING)
    LEFT JOIN ads_title ON ads_title.cID = linkedin_ads._adid
    LEFT JOIN campaigns ON campaigns._campaignID = ads_title.campaign_id
  ),
--  s6ense_ads AS (
--     SELECT
--       _adid AS _adid,
--       adName AS _adname,
--       campaignID AS _campaignid,
--       CAST(_date AS TIMESTAMP) AS _date,
--       SUM(spend) AS _spend, 
--       SUM(clicks) AS _clicks, 
--       SUM(impressions) AS _impressions, 
--     FROM
--       `x-marketing.devo.db_6sense_ads_overview`
--     WHERE 
--     _date IS NOT NULL
--     GROUP BY 
--       _adid, adName, campaignID, _date
  -- ),
  -- s6ense_airtable AS (
  --   SELECT
  --     _adid, 
  --     _adname, 
  --     _campaignid, 
  --     _campaignname, 
  --     _adcopy, 
  --     _screenshot, 
  --     _ctacopy, 
  --     _designtemplate,
  --     _size, 
  --     _platform, 
  --     _segment,
  --     _designcolor,
  --     _designimages,
  --     _designblurp,
  --     _logos,
  --     _copymessaging,
  --     _copyassettype,
  --     _copytone,
  --     _copyproductcompanyname,
  --     _copystatisticproofpoint,
  --     _ctacopysofthard
  --   FROM
  --   `x-marketing.devo_mysql.optimization_airtable_ads_6sense` 
  --   WHERE 
  --     /* _sdc_deleted_at IS NULL 
  --     AND */ LENGTH(_adid)>2 and _campaignid IS NOT NULL
  --   GROUP BY 
  --     _adid, _adname, _campaignid, _campaignname, _adcopy, _screenshot, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard
  -- ),
  -- s6ense_combined AS (
  --   SELECT
  --     s6ense_airtable.* EXCEPT(_campaignid,_campaignname),
  --     s6ense_ads.* EXCEPT (_adid,_adname,_campaignid),
  --     s6ense_airtable._campaignname,
  --     CAST(s6ense_ads._campaignid AS INT64)
  --   FROM 
  --     s6ense_ads
  --   JOIN
  --     s6ense_airtable ON s6ense_ads._adid = s6ense_airtable._adid AND s6ense_ads._campaignid = s6ense_airtable._campaignid
  -- ),
_all AS (
SELECT *,
  EXTRACT(YEAR FROM _date) AS year,
  EXTRACT(MONTH FROM _date) AS month,
  EXTRACT(QUARTER FROM _date) AS quarter,
  CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
FROM (
SELECT * FROM linkedin_combined 
)
)
SELECT _all.*,
    --quarter partition (latest vs previous)
    --CASE 1: to compare Q1 new year vs Q4 last year (Current Quarter: Q2)
    CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE()) = 4 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 5 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 6 THEN (
        CASE WHEN year = (SELECT MAX(year) FROM _all) AND quarter = 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 THEN 2 END
      )
    --CASE 2: to compare Q4 last year vs Q3 last year (Current Quarter: Q1)
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) = 1 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 2 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 3 THEN (
      CASE WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 THEN 1
      WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 3 THEN 2 END
    )
      ELSE (
      --CASE 3: to compare previous quarter vs last 2 previous quarter (Current Quarter: Q3 & Q4)
        CASE WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 2 THEN 2
        ELSE NULL END
      )
      END AS _quarterpartition,
FROM _all
GROUP BY ALL