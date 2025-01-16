CREATE OR REPLACE TABLE
  `x-marketing.televerde_broadcom.li_demographic_company_industry` AS
WITH
  company_industry AS (
  SELECT
    SAFE_CAST( _clicks AS INT64) AS clicks,
    CASE
      WHEN _conversionrate = "" THEN 0
      WHEN _conversionrate LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_conversionrate, '%', '') AS DECIMAL) / 100
  END
    AS conversionrate,
    CAST(_extractdateyyyymmdd AS DATE)AS extractDate,
    CASE
      WHEN _percentoftotalconversions = "" THEN 0
      WHEN _percentoftotalconversions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE( _percentoftotalconversions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalconversions,
    _segmentdetails AS company_industry_segment,
    SAFE_CAST( _clicks AS INT64) / CAST(_impressions AS INT64) AS ctr,
    SAFE_CAST(_conversions AS INT64) AS conversions,
    CASE
      WHEN _percentoftotalimpressions = "" THEN 0
      WHEN _percentoftotalimpressions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalimpressions,
    CASE
      WHEN _percentoftotalclicks = "" THEN 0
      WHEN _percentoftotalclicks LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalclicks, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalclicks,
    CAST(_impressions AS INT64) AS impressions,
    _creativeid AS creative_id,
    _segmentdetails,
    _solutionarea,
    _campaignid,
    _campaignname,
    _campaigngroup,
    _creativename,
  FROM
    `x-marketing.televerde_mysql_2.db_li_demographic_new`
  WHERE
    _demographictype = 'Company Industry Segment' )
SELECT
  DISTINCT #campaign_id,
  b.campaignID,
  #campaign_name AS _campaignName,
  #campaign_group AS _groupName,
  b._campaignName,
  b._groupName,
  b.creative_id,
  #creative_name AS _ads_name,
  b._ads_name,
  b.solution_area,
  clicks,
  conversionrate,
  extractDate,
  percentoftotalconversions,
  company_industry_segment,
  ctr,
  conversions,
  percentoftotalimpressions,
  percentoftotalclicks,
  impressions
FROM
  company_industry
LEFT JOIN
  `x-marketing.televerde_broadcom.linkedin_ads_performance` b
ON
  CAST(company_industry .creative_id AS STRING) = b.creative_id;

  
CREATE OR REPLACE TABLE
  `x-marketing.televerde_broadcom.li_demographic_company_name` AS
WITH
  company_industry AS (
  SELECT
    SAFE_CAST( _clicks AS INT64) AS clicks,
    CASE
      WHEN _conversionrate = "" THEN 0
      WHEN _conversionrate LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_conversionrate, '%', '') AS DECIMAL) / 100
  END
    AS conversionrate,
    CAST(_extractdateyyyymmdd AS DATE)AS extractDate,
    CASE
      WHEN _percentoftotalconversions = "" THEN 0
      WHEN _percentoftotalconversions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE( _percentoftotalconversions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalconversions,
    _segmentdetails AS company_industry_segment,
    SAFE_CAST( _clicks AS INT64) / CAST(_impressions AS INT64) AS ctr,
    SAFE_CAST(_conversions AS INT64) AS conversions,
    CASE
      WHEN _percentoftotalimpressions = "" THEN 0
      WHEN _percentoftotalimpressions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalimpressions,
    CASE
      WHEN _percentoftotalclicks = "" THEN 0
      WHEN _percentoftotalclicks LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalclicks, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalclicks,
    CAST(_impressions AS INT64) AS impressions,
    _creativeid AS creative_id,
    _segmentdetails,
    _solutionarea,
    _campaignid,
    _campaignname,
    _campaigngroup,
    _creativename,
  FROM
    `x-marketing.televerde_mysql_2.db_li_demographic_new`
  WHERE
    _demographictype = 'Company Name Segment' )
SELECT
  DISTINCT #campaign_id,
  b.campaignID,
  #campaign_name AS _campaignName,
  #campaign_group AS _groupName,
  b._campaignName,
  b._groupName,
  b.creative_id,
  #creative_name AS _ads_name,
  b._ads_name,
  b.solution_area,
  clicks,
  conversionrate,
  extractDate,
  percentoftotalconversions,
  company_industry_segment,
  ctr,
  conversions,
  percentoftotalimpressions,
  percentoftotalclicks,
  impressions
FROM
  company_industry
LEFT JOIN
  `x-marketing.televerde_broadcom.linkedin_ads_performance` b
ON
  CAST(company_industry .creative_id AS STRING) = b.creative_id;


CREATE OR REPLACE TABLE
  `x-marketing.televerde_broadcom.li_demographic_country_region` AS
WITH
  company_industry AS (
  SELECT
    SAFE_CAST( _clicks AS INT64) AS clicks,
    CASE
      WHEN _conversionrate = "" THEN 0
      WHEN _conversionrate LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_conversionrate, '%', '') AS DECIMAL) / 100
  END
    AS conversionrate,
    CAST(_extractdateyyyymmdd AS DATE)AS extractDate,
    CASE
      WHEN _percentoftotalconversions = "" THEN 0
      WHEN _percentoftotalconversions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE( _percentoftotalconversions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalconversions,
    _segmentdetails AS company_industry_segment,
    SAFE_CAST( _clicks AS INT64) / CAST(_impressions AS INT64) AS ctr,
    SAFE_CAST(_conversions AS INT64) AS conversions,
    CASE
      WHEN _percentoftotalimpressions = "" THEN 0
      WHEN _percentoftotalimpressions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalimpressions,
    CASE
      WHEN _percentoftotalclicks = "" THEN 0
      WHEN _percentoftotalclicks LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalclicks, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalclicks,
    CAST(_impressions AS INT64) AS impressions,
    _creativeid AS creative_id,
    _segmentdetails,
    _solutionarea,
    _campaignid,
    _campaignname,
    _campaigngroup,
    _creativename,
  FROM
    `x-marketing.televerde_mysql_2.db_li_demographic_new`
  WHERE
    _demographictype = 'Contextual Country/Region Segment' )
SELECT
  DISTINCT #campaign_id,
  b.campaignID,
  #campaign_name AS _campaignName,
  #campaign_group AS _groupName,
  b._campaignName,
  b._groupName,
  b.creative_id,
  #creative_name AS _ads_name,
  b._ads_name,
  b.solution_area,
  clicks,
  conversionrate,
  extractDate,
  percentoftotalconversions,
  company_industry_segment,
  ctr,
  conversions,
  percentoftotalimpressions,
  percentoftotalclicks,
  impressions
FROM
  company_industry
LEFT JOIN
  `x-marketing.televerde_broadcom.linkedin_ads_performance` b
ON
  CAST(company_industry .creative_id AS STRING) = b.creative_id;
CREATE OR REPLACE TABLE
  `x-marketing.televerde_broadcom.li_demographic_job_function` AS
WITH
  company_industry AS (
  SELECT
    SAFE_CAST( _clicks AS INT64) AS clicks,
    CASE
      WHEN _conversionrate = "" THEN 0
      WHEN _conversionrate LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_conversionrate, '%', '') AS DECIMAL) / 100
  END
    AS conversionrate,
    CAST(_extractdateyyyymmdd AS DATE)AS extractDate,
    CASE
      WHEN _percentoftotalconversions = "" THEN 0
      WHEN _percentoftotalconversions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE( _percentoftotalconversions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalconversions,
    _segmentdetails AS company_industry_segment,
    SAFE_CAST( _clicks AS INT64) / CAST(_impressions AS INT64) AS ctr,
    SAFE_CAST(_conversions AS INT64) AS conversions,
    CASE
      WHEN _percentoftotalimpressions = "" THEN 0
      WHEN _percentoftotalimpressions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalimpressions,
    CASE
      WHEN _percentoftotalclicks = "" THEN 0
      WHEN _percentoftotalclicks LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalclicks, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalclicks,
    CAST(_impressions AS INT64) AS impressions,
    _creativeid AS creative_id,
    _segmentdetails,
    _solutionarea,
    _campaignid,
    _campaignname,
    _campaigngroup,
    _creativename,
  FROM
    `x-marketing.televerde_mysql_2.db_li_demographic_new`
  WHERE
    _demographictype = 'Job Function Segment' )
SELECT
  DISTINCT #campaign_id,
  b.campaignID,
  #campaign_name AS _campaignName,
  #campaign_group AS _groupName,
  b._campaignName,
  b._groupName,
  b.creative_id,
  #creative_name AS _ads_name,
  b._ads_name,
  b.solution_area,
  clicks,
  conversionrate,
  extractDate,
  percentoftotalconversions,
  company_industry_segment,
  ctr,
  conversions,
  percentoftotalimpressions,
  percentoftotalclicks,
  impressions
FROM
  company_industry
LEFT JOIN
  `x-marketing.televerde_broadcom.linkedin_ads_performance` b
ON
  CAST(company_industry .creative_id AS STRING) = b.creative_id;


CREATE OR REPLACE TABLE
  `x-marketing.televerde_broadcom.li_demographic_job_seniority` AS
WITH
  company_industry AS (
  SELECT
    SAFE_CAST( _clicks AS INT64) AS clicks,
    CASE
      WHEN _conversionrate = "" THEN 0
      WHEN _conversionrate LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_conversionrate, '%', '') AS DECIMAL) / 100
  END
    AS conversionrate,
    CAST(_extractdateyyyymmdd AS DATE)AS extractDate,
    CASE
      WHEN _percentoftotalconversions = "" THEN 0
      WHEN _percentoftotalconversions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE( _percentoftotalconversions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalconversions,
    _segmentdetails AS company_industry_segment,
    SAFE_CAST( _clicks AS INT64) / CAST(_impressions AS INT64) AS ctr,
    SAFE_CAST(_conversions AS INT64) AS conversions,
    CASE
      WHEN _percentoftotalimpressions = "" THEN 0
      WHEN _percentoftotalimpressions LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalimpressions,
    CASE
      WHEN _percentoftotalclicks = "" THEN 0
      WHEN _percentoftotalclicks LIKE '%-%' THEN 0
      ELSE CAST(REPLACE(_percentoftotalclicks, '%', '') AS DECIMAL) / 100
  END
    AS percentoftotalclicks,
    CAST(_impressions AS INT64) AS impressions,
    _creativeid AS creative_id,
    _segmentdetails,
    _solutionarea,
    _campaignid,
    _campaignname,
    _campaigngroup,
    _creativename,
  FROM
    `x-marketing.televerde_mysql_2.db_li_demographic_new`
  WHERE
    _demographictype = 'Job Seniority Segment' )
SELECT
  DISTINCT #campaign_id,
  b.campaignID,
  #campaign_name AS _campaignName,
  #campaign_group AS _groupName,
  b._campaignName,
  b._groupName,
  b.creative_id,
  #creative_name AS _ads_name,
  b._ads_name,
  b.solution_area,
  clicks,
  conversionrate,
  extractDate,
  percentoftotalconversions,
  company_industry_segment,
  ctr,
  conversions,
  percentoftotalimpressions,
  percentoftotalclicks,
  impressions
FROM
  company_industry
LEFT JOIN
  `x-marketing.televerde_broadcom.linkedin_ads_performance` b
ON
  CAST(company_industry .creative_id AS STRING) = b.creative_id;