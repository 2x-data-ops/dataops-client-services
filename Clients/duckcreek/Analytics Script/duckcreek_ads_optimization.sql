WITH 
linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS DATE) AS _date,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions, 
  FROM
    `x-marketing.duckcreek_linkedin.ad_analytics_by_creative`
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
    `x-marketing.duckcreek_linkedin.creatives`
),
campaigns AS (
  SELECT 
    id AS _campaignID,
    name AS _campaignname
  FROM `x-marketing.duckcreek_linkedin.campaigns`
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
    `x-marketing.duckcreek_mysql.optimization_airtable_ads_linkedin`
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
    )
)
SELECT 
    _all.*,
    CASE 
        --quarter partition (latest vs previous)
        --CASE 1: to compare Q1 new year vs Q4 last year (Current Quarter: Q2)
        WHEN 
            EXTRACT(MONTH FROM CURRENT_DATE()) = 4 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 5 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 6 
            THEN (
            CASE 
                WHEN year = (SELECT MAX(year) FROM _all) AND quarter = 1 
                THEN 1
                WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 
                THEN 2 
            END
            )
        --CASE 2: to compare Q4 last year vs Q3 last year (Current Quarter: Q1)
        WHEN EXTRACT(MONTH FROM CURRENT_DATE()) = 1 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 2 OR EXTRACT(MONTH FROM CURRENT_DATE()) = 3 
        THEN (
        CASE 
            WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 
            THEN 1
            WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 3 
            THEN 2 
        END
        )
        ELSE (
        --CASE 3: to compare previous quarter vs last 2 previous quarter (Current Quarter: Q3 & Q4)
        CASE 
            WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 1 
            THEN 1
            WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 2 
            THEN 2
            ELSE NULL 
        END
        )
    END AS _quarterpartition,
FROM _all
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30;