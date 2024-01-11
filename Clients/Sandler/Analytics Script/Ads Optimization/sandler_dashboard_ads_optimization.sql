--ads overview 6sense
CREATE OR REPLACE TABLE `x-marketing.sandler.db_6sense_ads_overview` AS
SELECT DISTINCT
    main._campaignid AS campaignID,
    main._adid AS _adid,
    PARSE_DATE('%m/%d/%Y', _date) AS _date,
    main._sdc_sequence AS _sdc_sequence,
    _adgroup AS adGroup,
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
    CAST(REPLACE(_accountreached, ',', '') AS INT) AS accountsReached,
    CAST(REPLACE(_impressions, ',', '') AS INT) AS impressions,
    CAST(REPLACE(_clicks, ',', '') AS INT) AS clicks,
    CAST(REPLACE(_ctr, '%', '') AS DECIMAL) / 100 AS ctr,
    CAST(REPLACE(_accountctr, '%', '') AS DECIMAL) / 100 AS actr,
    CAST(REPLACE(REPLACE(_ecpm, ',', ''), '$', '') AS DECIMAL) AS cpm,
    CAST(REPLACE(_ecpc, '$', '') AS DECIMAL) AS cpc,
    CAST(REPLACE(_vtr, '%', '') AS DECIMAL) / 100 AS vtr,
    CAST(REPLACE(_accountvtr, '%', '') AS DECIMAL) / 100 AS avtr,
    CASE WHEN _budget = '-' THEN 0 
    ELSE CAST(REPLACE(REPLACE(_budget, ',', ''), '$', '') AS DECIMAL) END AS budget,
    CAST(REPLACE(REPLACE(_spend, ',', ''), '$', '') AS DECIMAL) AS spend
FROM `x-marketing.sandler_mysql.db_6sense_ads_overview` main
WHERE _name <> "";

--ads optimization
TRUNCATE TABLE `sandler.dashboard_opimization_ads` ;
INSERT INTO `x-marketing.sandler.dashboard_opimization_ads` (
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
  _campaignid
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
      `x-marketing.sandler_linkedin_ads.ad_analytics_by_creative`
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
    `x-marketing.sandler_linkedin_ads.creatives`
  ),
  campaigns AS (
    SELECT 
    id AS _campaignID,
    name AS _campaignname
    FROM `x-marketing.sandler_linkedin_ads.campaigns`
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
    `x-marketing.sandler_mysql.optimization_airtable_ads_linkedin` 
    WHERE 
      /* _sdc_deleted_at IS NULL 
      AND */ LENGTH(_adid)>2
    GROUP BY 
      _adid, _adname, _adcopy, _screenshot, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard
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
 s6ense_ads AS (
    SELECT
      _adid AS _adid,
      adName AS _adname,
      campaignID AS _campaignid,
      CAST(_date AS TIMESTAMP) AS _date,
      SUM(spend) AS _spend, 
      SUM(clicks) AS _clicks, 
      SUM(impressions) AS _impressions, 
    FROM
      `x-marketing.sandler.db_6sense_ads_overview`
    WHERE 
    _date IS NOT NULL
    GROUP BY 
      _adid, adName, campaignID, _date
  ),
  s6ense_airtable AS (
    SELECT
      _adid, 
      _adname, 
      _campaignid, 
      _campaignname, 
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
    `x-marketing.sandler_mysql.optimization_airtable_ads_6sense` 
    WHERE 
      /* _sdc_deleted_at IS NULL 
      AND */ LENGTH(_adid)>2 and _campaignid IS NOT NULL
    GROUP BY 
      _adid, _adname, _campaignid, _campaignname, _adcopy, _screenshot, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard
  ),
  s6ense_combined AS (
    SELECT
      s6ense_airtable.* EXCEPT(_campaignid,_campaignname),
      s6ense_ads.* EXCEPT (_adid,_adname,_campaignid),
      s6ense_airtable._campaignname,
      CAST(s6ense_ads._campaignid AS INT64)
    FROM 
      s6ense_ads
    JOIN
      s6ense_airtable ON s6ense_ads._adid = s6ense_airtable._adid AND s6ense_ads._campaignid = s6ense_airtable._campaignid
  )
SELECT * FROM linkedin_combined 
UNION DISTINCT
SELECT * FROM s6ense_combined
;
