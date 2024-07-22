------------------------Combination of LinkedIn and 6sense-------------------------------

TRUNCATE TABLE `x-marketing.skysafe.dashboard_optimization_ads`;
INSERT INTO `x-marketing.skysafe.dashboard_optimization_ads` (
  _adname, 
  _adid, 
  _campaignid, 
  _date, 
  _spend, 
  _clicks, 
  _impressions,
  _campaignname, 
  _adgroup,
  _adcopy, 
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
  _screenshot,
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition 
  )
WITH 
-- Combine 6sense ads data with airtable data
_6sense_combined AS (
  SELECT
    a.adName AS _adname,
    CAST(a._adid AS STRING) AS _adid,
    a.campaignID AS _campaignid,
    _date,
    a.spend AS _spend,
    CAST(a.clicks AS INT) AS _clicks,
    a.impressions AS _impressions,
    b._campaignname,
    b._adgroup,
    b._adcopy,
    b._ctacopy,
    b._designtemplate,
    b._size,
    b._platform,
    b._segment,
    b._designcolor,
    b._designimages,
    b._designblurp,
    b._logos,
    b._copymessaging,
    b._copyassettype,
    b._copytone,
    b._copyproductcompanyname,
    b._copystatisticproofpoint,
    b._ctacopysofthard,
    b._screenshot
  FROM `x-marketing.skysafe.db_6sense_ads_overview` a
  JOIN `x-marketing.skysafe_mysql.optimization_airtable_ads` b
    ON a._adid = b._adid AND a.campaignID= b._campaignid
  WHERE LENGTH(a._adid) > 2
),

-- Combine LinkedIn ads data with airtable data
linkedin_combined AS (
  SELECT
    b._adname,
    CAST(l.creative_id AS STRING) AS _adid,
    b._campaignid AS _campaignid,
    CAST(l.start_at AS DATE) AS _date,
    l.cost_in_usd AS _spend,
    l.clicks AS _clicks,
    l.impressions AS _impressions,
    c.name AS _campaignname,
    b._adgroup,
    b._adcopy,
    b._ctacopy,
    b._designtemplate,
    b._size,
    b._platform,
    b._segment,
    b._designcolor,
    b._designimages,
    b._designblurp,
    b._logos,
    b._copymessaging,
    b._copyassettype,
    b._copytone,
    b._copyproductcompanyname,
    b._copystatisticproofpoint,
    b._ctacopysofthard,
    b._screenshot
  FROM `x-marketing.skysafe_linkedin_ads.ad_analytics_by_creative` l
  JOIN `x-marketing.skysafe_mysql.optimization_airtable_ads_linkedin` b
    ON CAST(l.creative_id AS STRING) = b._adid
  LEFT JOIN `x-marketing.skysafe_linkedin_ads.creatives` a
    ON SPLIT(SUBSTR(a.id, STRPOS(a.id, 'sponsoredCreative:')+18))[ORDINAL(1)] = CAST(l.creative_id AS STRING)
  LEFT JOIN `x-marketing.skysafe_linkedin_ads.campaigns` c
    ON c.id = a.campaign_id
  WHERE l.start_at IS NOT NULL AND LENGTH(b._adid) > 2
),

-- Combine all data sources
_all AS (
  SELECT
    _combined.*,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM (
  SELECT * FROM linkedin_combined
  UNION ALL
  SELECT * FROM _6sense_combined
) _combined
)

-- Final query with optimized calculations and filtering
SELECT
  a.*,
  CASE
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 4 AND 6 THEN
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = 1 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 1 AND 3 THEN
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 4 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 3 THEN 2
      END
    ELSE
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = (SELECT MAX(quarter) FROM _all) - 1 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = (SELECT MAX(quarter) FROM _all) - 2 THEN 2
      END
  END AS _quarterpartition
FROM
  _all a
GROUP BY ALL;



-------------------------------6sense only-------------------------------------------
TRUNCATE TABLE `x-marketing.liferay.dashboard_optimization_ads`;
INSERT INTO `x-marketing.liferay.dashboard_optimization_ads` ( 
  _adname, 
  _adid,
  _campaignid, 
  _date,
  _spend,
  _clicks,
  _impressions,
  _campaignname, 
  _adgroup,
  _adcopy, 
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
  _screenshot, 
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition
  )
WITH 
-- Combine 6sense ads data with airtable data
_6sense_combined AS (
  SELECT
    a._name AS _adname,
    CAST(_6senseid AS STRING) AS _adid,
    a._campaignid AS _campaignid,
    _date,
    a._spent AS _spend,
    CAST(a._clicks AS INT) AS _clicks,
    a._impressions AS _impressions,
    b._campaignname,
    b._adgroup,
    b._adcopy,
    b._ctacopy,
    b._designtemplate,
    b._size,
    b._platform,
    b._segment,
    b._designcolor,
    b._designimages,
    b._designblurp,
    b._logos,
    b._copymessaging,
    b._copyassettype,
    b._copytone,
    b._copyproductcompanyname,
    b._copystatisticproofpoint,
    b._ctacopysofthard,
    b._screenshot
  FROM `x-marketing.liferay.db_daily_ads_performance` a
  JOIN `x-marketing.liferay_mysql.liferay_optimization_airtable_ads_6sense` b
    ON CAST(_6senseid AS STRING) = b._adid AND a._campaignid = b._campaignid
  WHERE LENGTH(a._6senseid) > 2 AND _datatype = 'Ad'

),

-- Combine all data sources
_all AS (
  SELECT
    _combined.*,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM (
  SELECT * FROM _6sense_combined
) _combined
)

-- Final query with optimized calculations and filtering
SELECT
  a.*,
  CASE
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 4 AND 6 THEN
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = 1 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 1 AND 3 THEN
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 4 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) - 1 AND a.quarter = 3 THEN 2
      END
    ELSE
      CASE
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = (SELECT MAX(quarter) FROM _all) - 1 THEN 1
        WHEN a.year = (SELECT MAX(year) FROM _all) AND a.quarter = (SELECT MAX(quarter) FROM _all) - 2 THEN 2
      END
  END AS _quarterpartition
FROM
  _all a
GROUP BY ALL;