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
      _6senseid AS _adid,
      _name AS _adname,
      _campaignid,
      _date,
      CAST(REPLACE(REPLACE(_spend, '$',''),',','') AS NUMERIC) AS _spend, 
      CAST(_clicks AS INTEGER) AS _clicks, 
      CAST(REPLACE(_impressions, ',','') AS INTEGER) AS _impressions
    FROM `x-marketing.x_mysql.db_6sense_3x_campaign_performance_newer`
    WHERE _datatype = 'Ad' --AND _sdc_deleted_at IS NULL
  ),
  campaigns AS (
    SELECT DISTINCT _campaignid,
      _name AS _campaignname
    FROM `x-marketing.x_mysql.db_6sense_3x_campaign_performance_newer`
    WHERE _campaigntype = 'LinkedIn' AND _datatype = 'Campaign' --AND _sdc_deleted_at IS NULL
  ),
  linkedin_airtable AS (
    SELECT
      _adid, 
      _6senseadid, --IF LINKEDIN ADS WAS CONNECTED TO 6SENSE ONLY. ELSE IGNORE THIS FIELD
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
      _adid, _6senseadid, _adname, _adcopy, _screenshot, _ctacopy, _designtemplate,_size, _platform, _segment,_designcolor,_designimages,_designblurp,_logos,_copymessaging,_copyassettype,_copytone,_copyproductcompanyname,_copystatisticproofpoint,_ctacopysofthard
  ),
  linkedin_combined AS (
    SELECT
      linkedin_airtable.* EXCEPT(_6senseadid),
      linkedin_ads.* EXCEPT (_adid,_campaignid,_adname),
      campaigns._campaignname,
      CAST(campaigns._campaignid AS INT64) AS _campaignid
    FROM 
      linkedin_ads
    JOIN
      linkedin_airtable ON linkedin_ads._adid = CAST(linkedin_airtable._6senseadid AS STRING)
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
    FROM `x-marketing.x_mysql.db_6sense_3x_campaign_performance_newer`
    WHERE _datatype = 'Ad' --AND _sdc_deleted_at IS NULL
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
  ),
  _all AS (
    SELECT *,
     EXTRACT(YEAR FROM _date) AS year,
  EXTRACT(MONTH FROM _date) AS month,
  EXTRACT(QUARTER FROM _date) AS quarter,
  CONCAT('Q',EXTRACT(YEAR FROM _date),'-',EXTRACT(QUARTER FROM _date)) AS quarteryear
FROM (
SELECT * FROM linkedin_combined 
UNION DISTINCT
SELECT * FROM s6ense_combined
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

------------------------------------------------
-------------- Content Analytics ---------------
------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.3x.ads_content_analytics` AS
TRUNCATE TABLE `x-marketing.3x.ads_content_analytics`;
INSERT INTO `x-marketing.3x.ads_content_analytics`
WITH ads_log AS (
    SELECT 
        *
    FROM `3x.dashboard_opimization_ads`
),
content AS (
  SELECT 
      * EXCEPT(rownum)
  FROM ( 
      SELECT 
          * EXCEPT(
              _sdc_batched_at, 
              _sdc_received_at,
              _sdc_sequence, 
              _sdc_table_version,
              _status
          ),
          -- Stage is set over here
          'Awareness' AS _stage,
          ROW_NUMBER() OVER(
              PARTITION BY _adid
              ORDER BY _sdc_received_at DESC
          ) AS rownum
      FROM 
          `x-marketing.x_mysql.optimization_airtable_3x_ads_6sense`
      WHERE _platform != ''
  ) airtable
  JOIN `x-marketing.x_mysql.db_airtable_3x_content_inventory` CI
  ON airtable._websiteurl = CI._homeURL
  WHERE rownum = 1
)
SELECT
    ads_log.*,
    content._contentitem,
    content._contenttype,
    content._gatingstrategy,
    content._homeurl,
    content._summary,
    content._status,
    content._buyerstage,
    content._vertical,
    content._persona,
    content._jobtitles,
    content._industry,
FROM ads_log
LEFT JOIN content
ON ads_log._adid = content._adid

