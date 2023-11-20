--ads optimization

TRUNCATE TABLE `3x.dashboard_opimization_ads` ;
INSERT INTO `x-marketing.3x.dashboard_opimization_ads` (
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
      _6senseid AS _adid,
      _name AS _adname,
      _campaignid,
      _date,
      CAST(REPLACE(REPLACE(_spend, '$',''),',','') AS NUMERIC) AS _spend, 
      CAST(_clicks AS INTEGER) AS _clicks, 
      CAST(REPLACE(_impressions, ',','') AS INTEGER) AS _impressions
    FROM `x-marketing.webtrack_ipcompany.db_6sense_3x_campaign_performance_newer`
    WHERE _datatype = 'Ad' AND _sdc_deleted_at IS NULL
  ),
  campaigns AS (
    SELECT DISTINCT _campaignid,
      _name AS _campaignname
    FROM `x-marketing.webtrack_ipcompany.db_6sense_3x_campaign_performance_newer`
    WHERE _campaigntype = 'LinkedIn' AND _datatype = 'Campaign' AND _sdc_deleted_at IS NULL
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
    `x-marketing.x_mysql.optimization_airtable_3x_ads_linkedin` 
    WHERE LENGTH(_adid)>2
    GROUP BY 
      _adid, _adname, _adcopy, _screenshot, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard
  ),
  linkedin_combined AS (
    SELECT
      linkedin_airtable.*,
      linkedin_ads.* EXCEPT (_adid,_campaignid,_adname),
      campaigns._campaignname,
      CAST(campaigns._campaignid AS INT64)
    FROM 
      linkedin_ads
    JOIN
      linkedin_airtable ON linkedin_ads._adid = CAST(linkedin_airtable._adid AS STRING)
    JOIN campaigns ON campaigns._campaignid = linkedin_ads._campaignid
  ),
 s6ense_ads AS (
    SELECT
      _6senseid AS _adid,
      _name AS _adname,
      _campaignid,
      _date,
      CAST(REPLACE(REPLACE(_spend, '$',''),',','') AS NUMERIC) AS _spend, 
      CAST(_clicks AS INTEGER) AS _clicks, 
      CAST(REPLACE(_impressions, ',','') AS INTEGER) AS _impressions
    FROM `x-marketing.webtrack_ipcompany.db_6sense_3x_campaign_performance_newer`
    WHERE _datatype = 'Ad' AND _sdc_deleted_at IS NULL
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
    `x-marketing.x_mysql.optimization_airtable_3x_ads_6sense` 
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
