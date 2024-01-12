--6sense ads overview
CREATE OR REPLACE TABLE `x-marketing.faro.db_6sense_ads_overview` AS
SELECT DISTINCT
    main._campaignid AS campaignID,
    main._6senseid AS _adid,
    PARSE_DATE('%m/%d/%Y', _week) AS _date,
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
FROM `x-marketing.faro_mysql.db_6sense_ads_data` main
WHERE _name <> "" AND _datatype ='Ad';


--ads optimization
TRUNCATE TABLE `x-marketing.faro.dashboard_optimization_ads`;
INSERT INTO `x-marketing.faro.dashboard_optimization_ads` (
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
  _shorten_segment
  )
WITH
_6sense_ads_campaigns AS (
  SELECT
    adName AS _adname,
    CAST(_adid AS STRING) _adid,
    campaignID AS _campaignid,
    _date AS _date,
    spend AS _spend, 
    CAST(clicks AS INT) AS _clicks, 
    impressions AS _impressions, 
  FROM
    `x-marketing.faro.db_6sense_ads_overview` 
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
   `x-marketing.faro_mysql.optimization_airtable_ads`
  WHERE 
    LENGTH(_adid)>2
  GROUP BY 
    _adid, _adname, _campaignid, _campaignname, _adgroup, _adcopy, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard, _screenshot
)
SELECT 
    _6sense_airtable.*,
    _6sense_ads_campaigns.* EXCEPT(_adname,_campaignid,_adid),
    CASE WHEN _6sense_airtable._segment LIKE '%HoloBuilder%' THEN 'HB + PCM'
    WHEN _6sense_airtable._segment LIKE 'SMB%' THEN 'SMB'
    WHEN _6sense_airtable._segment LIKE 'SMB%' THEN 'SMB'
    WHEN _6sense_airtable._segment LIKE 'SMB%' THEN 'SMB'
    END AS _shorten_segment
FROM _6sense_ads_campaigns
INNER JOIN _6sense_airtable ON _6sense_ads_campaigns._adid = _6sense_airtable._adid AND _6sense_ads_campaigns._campaignid = _6sense_airtable._campaignid;