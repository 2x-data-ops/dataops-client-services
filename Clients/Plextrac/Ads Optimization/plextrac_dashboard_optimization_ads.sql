TRUNCATE TABLE `x-marketing.plextrac.dashboard_opimization_ads`;
INSERT INTO `x-marketing.plextrac.dashboard_opimization_ads` (
  _adid, 
  _adname, 
  _campaignid, 
  _campaignname, 
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
  _impressions
  )
WITH
ads_campaigns AS (
  SELECT
    _advariation AS _adname,
    CAST(_adid AS STRING) _adid,
    _campaignid AS _campaignid,
    CASE WHEN _date = '0023-08-13' THEN '2023-08-13'
    WHEN _date = '0023-08-14' THEN '2023-08-14'
    WHEN _date = '0023-08-15' THEN '2023-08-15'
    WHEN _date = '0023-08-16' THEN '2023-08-16'
    WHEN _date = '0023-08-17' THEN '2023-08-17'
    WHEN _date = '0023-08-18' THEN '2023-08-18'
    WHEN _date = '0023-08-19' THEN '2023-08-19'
    WHEN _date = '0023-08-20' THEN '2023-08-20'
    WHEN _date = '0023-08-21' THEN '2023-08-21'
    WHEN _date = '0023-08-22' THEN '2023-08-22'
    WHEN _date = '0023-08-23' THEN '2023-08-23'
    WHEN _date = '0023-08-24' THEN '2023-08-24'
    WHEN _date = '0023-08-25' THEN '2023-08-25'
    WHEN _date = '0023-08-26' THEN '2023-08-26'
    WHEN _date = '0023-08-27' THEN '2023-08-27'
    WHEN _date = '0023-08-28' THEN '2023-08-28'
    WHEN _date = '0023-08-29' THEN '2023-08-29'
    WHEN _date = '0023-08-30' THEN '2023-08-30'
    WHEN _date = '0023-08-31' THEN '2023-08-31'
    WHEN _date = '0023-09-01' THEN '2023-09-01'
    WHEN _date = '0023-09-02' THEN '2023-09-02'
    ELSE _date END AS _date,
    _spend AS _spend, 
    _clicks AS _clicks, 
    _impressions AS _impressions, 
  FROM
    `x-marketing.plextrac.db_6sense_ads_performance`
    --WHERE LENGTH(spend)>1
    --AND LENGTH(startDate)>3
  GROUP BY 
    1,2,3,4,5,6,7
),
airtable AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
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
   `x-marketing.plextrac_mysql.optimization_airtable_ads` 
  WHERE 
    /*_sdc_deleted_at IS NULL 
    AND*/ LENGTH(_adid)>2
  GROUP BY 
    _adid, _adname, _campaignid, _campaignname, _adcopy, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard, _screenshot
)
SELECT
  airtable.* ,
  ads_campaigns.* EXCEPT(_adname,_campaignid,_adid)
FROM
  ads_campaigns
INNER JOIN
  airtable ON ads_campaigns._adid = airtable._adid AND ads_campaigns._campaignid = airtable._campaignid;