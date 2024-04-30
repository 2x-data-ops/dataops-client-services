CREATE OR REPLACE TABLE `x-marketing.influitive.db_6sense_ads_overview` AS
SELECT DISTINCT
    main._campaignid AS campaignID,
    main._6senseid AS _adid,
    PARSE_DATE('%m/%d/%Y', _date) AS _date,
    main._sdc_sequence AS _sdc_sequence,
    '' AS adGroup,
    main._name AS adName,
    '' AS adVariation,
    '' AS adSize,
    '' AS dataType,
    _status AS status,
    CASE
        WHEN _startdate = '-' THEN NULL
        WHEN _startdate LIKE '%-%' THEN PARSE_DATE('%e-%h-%y', _startdate) 
        WHEN _startdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _startdate) 
    END AS startDate,
    CASE 
        WHEN _enddate = '-' THEN NULL
        WHEN _enddate LIKE '%-%' THEN PARSE_DATE('%e-%h-%y', _enddate) 
        WHEN _enddate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _enddate) 
    END AS endDate,
    #CAST(REPLACE(_accountreached, ',', '') AS INT) AS accountsReached,
    CASE WHEN _accountreached LIKE '%.%' THEN CAST(_accountreached AS DECIMAL) ELSE CAST(REPLACE(_accountreached, ',', '') AS INT) END AS accountsReached,
    CAST(REPLACE(_impressions, ',', '') AS INT) AS impressions,
    CASE WHEN _clicks LIKE '%.%' THEN CAST(_clicks AS DECIMAL) ELSE CAST(REPLACE(_clicks, ',', '') AS INT) END AS clicks,
    CAST(REPLACE(_ctr, '%', '') AS DECIMAL) / 100 AS ctr,
    CAST(REPLACE(_accountctr, '%', '') AS DECIMAL) / 100 AS actr,
    #CAST(REPLACE(REPLACE(_ecpm, ',', ''), '$', '') AS DECIMAL) AS cpm,
    CAST(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(_ecpm, '(', '-'),
            ')', ''
          ),
          ',', ''
        ),
        '$', ''
      ) AS DECIMAL
    ) AS cpm, -- change values with () to negative '-'
    CAST(REPLACE(_ecpc, '$', '') AS DECIMAL) AS cpc,
    CAST(REPLACE(_vtr, '%', '') AS DECIMAL) / 100 AS vtr,
    CAST(REPLACE(_accountvtr, '%', '') AS DECIMAL) / 100 AS avtr,
    CASE WHEN _budget = '-' THEN 0 
    ELSE CAST(REPLACE(REPLACE(_budget, ',', ''), '$', '') AS DECIMAL) END AS budget,
    CAST(REPLACE(REPLACE(_spend, ',', ''), '$', '') AS DECIMAL) AS spend
FROM `x-marketing.influitive_mysql.influitive_db_daily_campaign_performance` main
WHERE _name <> "" AND _datatype ='Ad';



TRUNCATE TABLE `x-marketing.influitive.dashboard_opimization_ads`;
INSERT INTO `x-marketing.influitive.dashboard_opimization_ads` (
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
WITH
_6sense_ads_campaigns AS (
  SELECT
    adName AS _adname,
    CAST(_adid AS STRING) _adid,
    campaignID AS _campaignid,
    CASE WHEN _date = '2023-11-26' THEN '2024-01-01' ELSE _date END AS _date,
    spend AS _spend, 
    CAST(clicks AS INT) AS _clicks, 
    impressions AS _impressions, 
  FROM
    `x-marketing.influitive.db_6sense_ads_overview`
    --WHERE LENGTH(spend)>1
    --AND LENGTH(startDate)>3
  GROUP BY 
    1,2,3,4,5,6,7
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
  FROM
   `x-marketing.influitive_mysql.influitive_optimization_airtable_ads`
  WHERE 
    /*_sdc_deleted_at IS NULL 
    AND*/ LENGTH(_adid)>2
  GROUP BY 
    _adid, _adname, _campaignid, _campaignname, _adgroup, _adcopy, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard, _screenshot
),
_6sense_combined AS (
SELECT
  _6sense_airtable.* ,
  _6sense_ads_campaigns.* EXCEPT(_adname,_campaignid,_adid)
FROM
  _6sense_ads_campaigns
LEFT JOIN
  _6sense_airtable ON _6sense_ads_campaigns._adid = _6sense_airtable._adid AND _6sense_ads_campaigns._campaignid = _6sense_airtable._campaignid
),
linkedin_ads AS (
    SELECT
      CAST(creative_id AS STRING) AS _adid,
      CAST(start_at AS DATE) AS _date,
      SUM(cost_in_usd) AS _spend, 
      SUM(clicks) AS _clicks, 
      SUM(impressions) AS _impressions, 
    FROM
      `x-marketing.influitive_linkedin_ads.ad_analytics_by_creative`
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
    `x-marketing.influitive_linkedin_ads.creatives`
  ),
  campaigns AS (
    SELECT 
    id AS _campaignID,
    name AS _campaignname
    FROM `x-marketing.influitive_linkedin_ads.campaigns`
),
linkedin_airtable AS (
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
  FROM
   `x-marketing.influitive_mysql.influitive_optimization_airtable_ads_linkedin`
  WHERE 
    /*_sdc_deleted_at IS NULL 
    AND*/ LENGTH(_adid)>2
  GROUP BY 
    _adid, _adname, _campaignid, _campaignname, _adgroup, _adcopy, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard, _screenshot
  ),
  linkedin_combined AS (
    SELECT
      linkedin_airtable.*,
      linkedin_ads.* EXCEPT (_adid)
    FROM 
      linkedin_ads
    JOIN
      linkedin_airtable ON linkedin_ads._adid = CAST(linkedin_airtable._adid AS STRING)
    LEFT JOIN ads_title ON ads_title.cID = linkedin_ads._adid
    LEFT JOIN campaigns ON campaigns._campaignID = ads_title.campaign_id
  ),
_all AS (
SELECT *,
  EXTRACT(YEAR FROM _date) AS year,
  EXTRACT(MONTH FROM _date) AS month,
  EXTRACT(QUARTER FROM _date) AS quarter,
  CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
FROM (
      SELECT * FROM linkedin_combined
      UNION ALL
      SELECT * FROM _6sense_combined
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30;