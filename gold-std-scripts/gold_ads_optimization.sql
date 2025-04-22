TRUNCATE TABLE `sandler.dashboard_opimization_ads`;

INSERT INTO `x-marketing.sandler.dashboard_opimization_ads` (
  _adid,
  _adname,
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
  _painPoint,
  _pillars,
  _instance,
  _date,
  _spend,
  _clicks,
  _impressions,
  _conversions,
  _campaignid,
  year,
  month,
  quarter,
  quarteryear,
  _quarterpartition
)
WITH linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS TIMESTAMP) AS _date,
    SUM(cost_in_usd) AS _spend,
    SUM(clicks) AS _clicks,
    SUM(impressions) AS _impressions,
    SUM(external_website_conversions) AS _conversions
  FROM `x-marketing.sandler_linkedin_ads_v2.ad_analytics_by_creative`
  WHERE start_at IS NOT NULL
  GROUP BY creative_id, start_at
),
ads_title AS (
  SELECT
    REGEXP_EXTRACT(id, r'\d+') AS cID,
    campaign_id
  FROM `x-marketing.sandler_linkedin_ads_v2.creatives`
),
campaigns AS (
  SELECT
    id AS _campaignID,
    name AS _campaignname
  FROM `x-marketing.sandler_linkedin_ads_v2.campaigns`
),
s6ense_ads AS (
  SELECT
    _adid,
    adName AS _adname,
    campaignID AS _campaignid,
    CAST(_date AS TIMESTAMP) AS _date,
    SUM(spend) AS _spend,
    SUM(CAST(clicks AS INT64)) AS _clicks,
    SUM(impressions) AS _impressions,
    NULL AS _conversions
  FROM `x-marketing.sandler.db_6sense_ads_overview`
  WHERE _date IS NOT NULL
  GROUP BY _adid, adName, campaignID, _date
),
airtable_google_sheets AS (
  SELECT
    _ad_id AS _adid,
    _ad_name AS _adname,
    _campaign_id AS _campaignid,
    _campaign_name AS _campaignname,
    _ad_group AS _adgroup,
    _text_on_image _adcopy,
    _cta_on_image AS _ctacopy,
    _template AS _designtemplate,
    _size,
    _platform,
    _business_segment AS _segment,
    _color AS _designcolor,
    _image AS _designimages,
    _blurb AS _designblurp,
    _logo AS _logos,
    _messaging AS _copymessaging,
    _asset_type AS _copyassettype,
    _tone AS _copytone,
    _product_company_name AS _copyproductcompanyname,
    _statistic_proof_point AS _copystatisticproofpoint,
    _cta_copy AS _ctacopysofthard,
    _ad_visual AS _screenshot,
    _pain_point AS _painPoint,
    _pillars,
    _instance
  FROM `x-marketing.sandler_google_sheets.db_ads_optimization`
  WHERE LENGTH(_ad_id) > 2
    AND _campaign_id IS NOT NULL
),
linkedin_airtable_network AS (
  SELECT
    *
  FROM airtable_google_sheets
  WHERE _platform = 'LinkedIn'
    AND _instance = 'Sandler Network'
  GROUP BY ALL
),
linkedin_airtable_franchise AS (
  SELECT
    *
  FROM airtable_google_sheets
  WHERE _platform = 'LinkedIn'
    AND _instance = 'Sandler Enterprise'
  GROUP BY ALL
),
s6sense_airtable AS (
  SELECT
    *
  FROM airtable_google_sheets
  WHERE _platform = '6sense'
    AND _instance = 'Sandler Enterprise'
  GROUP BY ALL
),
linkedin_combined_network AS (
  SELECT
    linkedin_airtable_network.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions
  FROM linkedin_ads
  JOIN linkedin_airtable_network
    ON linkedin_ads._adid = CAST(linkedin_airtable_network._adid AS STRING)
  RIGHT JOIN ads_title
    ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN campaigns
    ON campaigns._campaignID = ads_title.campaign_id
),
linkedin_combined_sandler AS (
  SELECT
    linkedin_airtable_franchise.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions
  FROM linkedin_ads
  JOIN linkedin_airtable_franchise
    ON linkedin_ads._adid = CAST(linkedin_airtable_franchise._adid AS STRING)
  LEFT JOIN ads_title
    ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN campaigns
    ON campaigns._campaignID = ads_title.campaign_id
),
s6sense_combined_sandler AS (
  SELECT
    s6ense_ads._adid,
    s6sense_airtable._adname,
    s6sense_airtable._campaignid,
    s6sense_airtable._campaignname,
    s6sense_airtable._adgroup,
    s6sense_airtable._adcopy,
    s6sense_airtable._ctacopy,
    s6sense_airtable._designtemplate,
    s6sense_airtable._size,
    s6sense_airtable._platform,
    s6sense_airtable._segment,
    s6sense_airtable._designcolor,
    s6sense_airtable._designimages,
    s6sense_airtable._designblurp,
    s6sense_airtable._logos,
    s6sense_airtable._copymessaging,
    s6sense_airtable._copyassettype,
    s6sense_airtable._copytone,
    s6sense_airtable._copyproductcompanyname,
    s6sense_airtable._copystatisticproofpoint,
    s6sense_airtable._ctacopysofthard,
    s6sense_airtable._screenshot,
    s6sense_airtable._painPoint,
    s6sense_airtable._pillars,
    s6sense_airtable._instance,
    s6ense_ads._date,
    s6ense_ads._spend,
    s6ense_ads._clicks,
    s6ense_ads._impressions,
    s6ense_ads._conversions
  FROM s6ense_ads
  JOIN s6sense_airtable
    ON s6ense_ads._adid = s6sense_airtable._adid
    AND s6ense_ads._campaignid = s6sense_airtable._campaignid
),
ad_counts AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.sandler_google_ads.ads` ad
    ON ad.ad_group_id = report.ad_group_id
  JOIN `x-marketing.sandler_google_sheets.db_ads_optimization` airtable
    ON airtable._ad_id = CAST(ad.id AS STRING)
  WHERE ad.name IS NOT NULL
    AND airtable._platform = 'Google Ads'
  GROUP BY 1, 2, 3, 4
),
adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    airtable._ad_name AS _adname,
    '' AS _adcopy,
    airtable._ad_visual AS _screenshot,
    '' AS _ctacopy,
    report.ad_group_id,
    report.ad_group_name,
    report.campaign_id,
    report.campaign_name,
    ad.name AS ad_name,
    report.date AS _date,
    airtable._size AS _adsize,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent,
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks,
    impressions / c.ad_count AS adjusted_impressions,
    airtable._pain_point AS _painpoint,
    airtable._pillars,
    ad_count
  FROM `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.sandler_google_ads.ads` ad
    ON ad.ad_group_id = report.ad_group_id
  JOIN `x-marketing.sandler_google_sheets.db_ads_optimization` airtable
    ON airtable._ad_id = CAST(ad.id AS STRING)
  JOIN ad_counts AS c
    ON ad.ad_group_id = c.ad_group_id
    AND report.date = c.date
  WHERE ad.name IS NOT NULL
    AND airtable._platform = 'Google Ads'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ad.id, campaign_id, report.date ORDER BY report.date DESC) = 1
),
google_display_combined AS (
  SELECT
    _adid,
    ad_name,
    CAST(campaign_id AS STRING) AS _campaignID,
    campaign_name,
    ad_group_name,
    '' AS _adcopy,
    '' AS _ctacopy,
    '' AS _designtemplate,
    _adsize AS _size,
    'Google Display' AS _platform,
    '' AS _segment,
    '' AS _designcolor,
    '' AS _designimages,
    '' AS _designblurp,
    '' AS _logos,
    '' AS _copymessaging,
    '' AS _copyassettype,
    '' AS _copytone,
    '' AS _copyproductcompanyname,
    '' AS _copystatisticproofpoint,
    '' AS _ctacopysofthard,
    _screenshot,
    _painPoint,
    _pillars,
    'Sandler Enterprise' AS _instance,
    _date,
    SUM(CAST(adjusted_spent AS NUMERIC)) AS total_spent,
    SUM(CAST(adjusted_clicks AS NUMERIC)) AS total_clicks,
    SUM(CAST(adjusted_impressions AS NUMERIC)) AS total_impressions,
    SUM(CAST(adjusted_conversions AS NUMERIC)) AS total_conversions,
  FROM adjusted_metrics
  GROUP BY ALL
  ORDER BY campaign_name, _date DESC
),
google_overview AS (
  SELECT
    CAST(id AS STRING) AS _adid,
    '' AS ad_name,
    CAST(campaign_id AS STRING) AS _campaignID,
    campaign_name,
    ad_group_name,
    '' AS _adcopy,
    '' AS _ctacopy,
    '' AS _designtemplate,
    '' AS _size,
    'Google SEM' AS _platform,
    '' AS _segment,
    '' AS _designcolor,
    '' AS _designimages,
    '' AS _designblurp,
    '' AS _logos,
    '' AS _copymessaging,
    '' AS _copyassettype,
    '' AS _copytone,
    '' AS _copyproductcompanyname,
    '' AS _copystatisticproofpoint,
    '' AS _ctacopysofthard,
    '' _screenshot,
    '' _painPoint,
    '' _pillars,
    'Sandler Enterprise' AS _instance,
    DATE AS _date,
    CAST(cost_micros / 1000000 AS NUMERIC) AS _spent,
    clicks AS _clicks,
    impressions AS _impressions,
    conversions AS _conversions,
  FROM `x-marketing.sandler_google_ads.ad_performance_report` report
  QUALIFY RANK() OVER (
    PARTITION BY DATE, campaign_id, ad_group_id, id
    ORDER BY _sdc_received_at DESC
  ) = 1
),
google_aggregated AS (
  SELECT
    * EXCEPT (_spent, _clicks, _impressions, _conversions),
    SUM(_spent) AS _spent,
    SUM(_clicks) AS _clicks,
    SUM(_impressions) AS _impressions,
    SUM(_conversions) AS _conversions
  FROM google_overview
  GROUP BY ALL
),
google_sem AS (
  SELECT
    *
  FROM google_aggregated
),
union_data AS (
  SELECT
    *
  FROM linkedin_combined_network
  UNION ALL
  SELECT
    *
  FROM linkedin_combined_sandler
  UNION ALL
  SELECT
    *
  FROM s6sense_combined_sandler
  UNION ALL
  SELECT
    *
  FROM google_display_combined
  UNION ALL
  SELECT
    *
  FROM google_sem
),
_all AS (
  SELECT
    * EXCEPT (_campaignID),
    CAST(_campaignID AS INT64) AS _campaignid,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM _date), '-', EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM union_data
)
SELECT
  _all.*,
  CASE 
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (4, 5, 6) THEN 
      CASE 
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) - 1 AND quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (1, 2, 3) THEN 
      CASE 
        WHEN year = (SELECT MAX(year) - 1 FROM _all) AND quarter = 4 THEN 1
        WHEN year = (SELECT MAX(year) - 1 FROM _all) AND quarter = 3 THEN 2
      END
    ELSE 
      CASE 
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 1 THEN 1
        WHEN year = (SELECT MAX(year) FROM _all) AND quarter = (SELECT MAX(quarter) FROM _all) - 2 THEN 2
      END
  END AS _quarterpartition
FROM _all
GROUP BY ALL;