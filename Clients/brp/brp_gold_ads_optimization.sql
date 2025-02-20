TRUNCATE TABLE `x-marketing.brp.dashboard_optimization_ads`;
INSERT INTO `x-marketing.brp.dashboard_optimization_ads` (
  _adid, 
  _adname, 
  _campaignid, 
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
  _date, 
  _spend, 
  _clicks, 
  _impressions,
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition
)
WITH _6sense_ads_campaigns AS (
  SELECT
    _advariation AS _adname,
    CAST(_adid AS STRING) _adid,
    _campaign_id AS _campaignid,
    _linkedincampaignid,
    _date,
    CAST(_spend AS NUMERIC) AS _spend, 
    CAST(_clicks AS INT) AS _clicks, 
    _impressions AS _impressions, 
  FROM `x-marketing.brp.db_6sense_ad_performance`
),
_6sense_airtable AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
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
    _screenshot
  FROM `x-marketing.brp_mysql.optimization_airtable_ads_linkedin`
  WHERE LENGTH(_adid) > 2
),
_6sense_combined AS (
  SELECT
    _6sense_airtable.*,
    _6sense_ads_campaigns.* EXCEPT(_adname,_campaignid,_adid, _linkedincampaignid)
  FROM _6sense_ads_campaigns
  JOIN _6sense_airtable 
    ON _6sense_ads_campaigns._adname = _6sense_airtable._adname 
    AND _6sense_ads_campaigns._linkedincampaignid = _6sense_airtable._campaignid
),
_all AS (
  SELECT 
    *,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM _6sense_combined
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