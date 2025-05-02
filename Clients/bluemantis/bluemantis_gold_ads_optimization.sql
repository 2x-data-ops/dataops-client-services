TRUNCATE TABLE `x-marketing.bluemantis.dashboard_optimization_ads`;
INSERT INTO `x-marketing.bluemantis.dashboard_optimization_ads` (
  _ad_id,
  _ad_name,
  _campaign_name,
  _ad_group,
  _cta_copy,
  _size,
  _platform,
  _business_segment,
  _logo,
  _messaging,
  _product_company_name,
  _statistic_proof_point,
  _introduction_text,
  _headline_text,
  _ad_visual,
  _campaign_initiated_by,
  _date,
  _spend,
  _clicks,
  _impressions,
  _conversions,
  _campaign_status,
  _campaign_id,
  _year,
  _month,
  _quarter,
  _quarter_year,
  _quarter_partition
)
WITH linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS TIMESTAMP) AS _date,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions,
    SUM(external_website_conversions) AS _conversions
  FROM `x-marketing.bluemantis_linkedin_ads.ad_analytics_by_creative`
  WHERE start_at IS NOT NULL
  GROUP BY creative_id, start_at
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.bluemantis_linkedin_ads.creatives`
),
campaigns AS (
  SELECT 
    id AS _campaignID,
    name AS _campaignname,
    INITCAP(status) AS _campaign_status
  FROM `x-marketing.bluemantis_linkedin_ads.campaigns`
),
s6ense_ads AS (
  SELECT
    _ad_id,
    _ad_variation AS _ad_name,
    _campaign_id AS _campaign_id,
    CAST(_date AS TIMESTAMP) AS _date,
    SUM(_spend) AS _spend, 
    SUM(CAST(_clicks AS INT64)) AS _clicks, 
    SUM(_impressions) AS _impressions,
    NULL AS _conversions,
    _campaign_status
  FROM `x-marketing.bluemantis.db_6sense_ad_performance`
  WHERE _date IS NOT NULL
  GROUP BY _ad_id, _ad_variation, _campaign_id, _date, _campaign_status
),
linkedin_airtable AS (
  SELECT
    _ad_id, 
    _ad_name, 
    CASE 
      WHEN _campaign_id = "" THEN NULL 
      ELSE _campaign_id 
    END AS _campaign_id,  
    _campaign_name, 
    _ad_group, 
    _cta_copy, 
    _size, 
    _platform, 
    _business_segment,
    _logo,
    _messaging,
    _product_company_name,
    _statistic_proof_point,
    _introduction_text,
    _headline_text,
    _ad_visual,
    _campaign_initiated_by
  FROM `x-marketing.bluemantis_google_sheets.db_ads_optimization`
  WHERE LENGTH(_ad_id) > 2 
    AND _campaign_id IS NOT NULL
    AND _platform = 'LinkedIn'
  GROUP BY ALL
),
s6sense_airtable AS (
  SELECT
    _ad_id, 
    _ad_name, 
    CASE 
      WHEN _campaign_id = "" THEN NULL 
      ELSE _campaign_id 
    END AS _campaign_id,  
    _campaign_name, 
    _ad_group, 
    _cta_copy, 
    _size, 
    _platform, 
    _business_segment,
    _logo,
    _messaging,
    _product_company_name,
    _statistic_proof_point,
    _introduction_text,
    _headline_text,
    _ad_visual,
    _campaign_initiated_by 
  FROM `x-marketing.bluemantis_google_sheets.db_ads_optimization`
  WHERE LENGTH(_ad_id) > 2 
    AND _campaign_id IS NOT NULL
    AND _platform = '6sense'
  GROUP BY ALL
),
linkedin_combined AS (
  SELECT
    linkedin_airtable.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions,
    campaigns._campaign_status
  FROM linkedin_ads
  JOIN linkedin_airtable 
    ON linkedin_ads._adid = CAST(linkedin_airtable._ad_id AS STRING)
  RIGHT JOIN ads_title 
    ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN campaigns 
    ON campaigns._campaignID = ads_title.campaign_id
  WHERE _date IS NOT NULL
),
s6sense_combined AS (
  SELECT
    s6ense_ads._ad_id,
    s6sense_airtable.* EXCEPT(_ad_id),
    s6ense_ads._date,
    s6ense_ads._spend,
    s6ense_ads._clicks,
    s6ense_ads._impressions,
    s6ense_ads._conversions,
    s6ense_ads._campaign_status
  FROM s6ense_ads
  JOIN s6sense_airtable 
    ON s6ense_ads._ad_id = s6sense_airtable._ad_id 
    AND s6ense_ads._campaign_id = s6sense_airtable._campaign_id
),
platform_consolidation AS (
  SELECT
    *
  FROM linkedin_combined
  UNION ALL
  SELECT
    *
  FROM s6sense_combined
),
_all AS (
  SELECT 
    * EXCEPT (_campaign_id),
    CAST(_campaign_id AS INT64) AS _campaign_id,
    EXTRACT(YEAR FROM _date) AS _year,
    EXTRACT(MONTH FROM _date) AS _month,
    EXTRACT(QUARTER FROM _date) AS _quarter,
    CONCAT('Q', EXTRACT(YEAR FROM _date), '-', EXTRACT(QUARTER FROM _date)) AS _quarter_year
  FROM platform_consolidation
)
SELECT 
  _all.*,
  CASE 
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (4, 5, 6) THEN 
      CASE 
        WHEN _year = (SELECT MAX(_year) FROM _all) AND _quarter = 1 THEN 1
        WHEN _year = (SELECT MAX(_year) FROM _all) - 1 AND _quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (1, 2, 3) THEN 
      CASE 
        WHEN _year = (SELECT MAX(_year) - 1 FROM _all) AND _quarter = 4 THEN 1
        WHEN _year = (SELECT MAX(_year) - 1 FROM _all) AND _quarter = 3 THEN 2
      END
    ELSE 
      CASE 
        WHEN _year = (SELECT MAX(_year) FROM _all) AND _quarter = (SELECT MAX(_quarter) FROM _all) - 1 THEN 1
        WHEN _year = (SELECT MAX(_year) FROM _all) AND _quarter = (SELECT MAX(_quarter) FROM _all) - 2 THEN 2
      END
  END AS _quarter_partition
FROM _all
GROUP BY ALL;