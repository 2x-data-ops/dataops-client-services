TRUNCATE TABLE `sandler.dashboard_opimization_ads`;

INSERT INTO `x-marketing.sandler.dashboard_opimization_ads` (
  _ad_id,
  _ad_name,
  _campaign_name,
  _ad_group,
  _ad_copy,
  _cta_copy,
  _design_template,
  _size,
  _platform,
  _segment,
  _design_color,
  _design_images,
  _design_blurp,
  _logos,
  _copy_messaging,
  _copy_asset_type,
  _copy_tone,
  _copy_product_company_name,
  _copy_statistic_proof_point,
  _cta_copy_soft_hard,
  _screenshot,
  _pain_point,
  _pillars,
  _instance,
  _date,
  _spend,
  _clicks,
  _impressions,
  _conversions,
  _campaign_id,
  _year,
  _month,
  _quarter,
  _quarter_year,
  _quarter_partition
)
WITH linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _ad_id,
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
    REGEXP_EXTRACT(id, r'\d+') AS _cid,
    campaign_id AS _campaign_id
  FROM `x-marketing.sandler_linkedin_ads_v2.creatives`
),
campaigns AS (
  SELECT
    id AS _campaign_id,
    name AS _campaign_name
  FROM `x-marketing.sandler_linkedin_ads_v2.campaigns`
),
s6ense_ads AS (
  SELECT
    _adid AS _ad_id,
    adName AS _ad_name,
    campaignID AS _campaign_id,
    CAST(_date AS TIMESTAMP) AS _date,
    SUM(spend) AS _spend,
    SUM(CAST(clicks AS INT64)) AS _clicks,
    SUM(impressions) AS _impressions,
    NULL AS _conversions
  FROM `x-marketing.sandler.db_6sense_ads_overview`
  WHERE _date IS NOT NULL
  GROUP BY _ad_id, _ad_name, _campaign_id, _date
),
airtable_google_sheets AS (
  SELECT
    _ad_id,
    _ad_name,
    _campaign_id,
    _campaign_name,
    _ad_group,
    _text_on_image _ad_copy,
    _cta_on_image AS _cta_copy,
    _template AS _design_template,
    _size,
    _platform,
    _business_segment AS _segment,
    _color AS _design_color,
    _image AS _design_images,
    _blurb AS _design_blurp,
    _logo AS _logos,
    _messaging AS _copy_messaging,
    _asset_type AS _copy_asset_type,
    _tone AS _copy_tone,
    _product_company_name AS _copy_product_company_name,
    _statistic_proof_point AS _copy_statistic_proof_point,
    _cta_copy AS _cta_copy_soft_hard,
    _ad_visual AS _screenshot,
    _pain_point AS _pain_point,
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
    ON linkedin_ads._ad_id = CAST(linkedin_airtable_network._ad_id AS STRING)
  RIGHT JOIN ads_title
    ON ads_title._cid = linkedin_ads._ad_id
  LEFT JOIN campaigns
    ON campaigns._campaign_id = ads_title._campaign_id
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
    ON linkedin_ads._ad_id = CAST(linkedin_airtable_franchise._ad_id AS STRING)
  LEFT JOIN ads_title
    ON ads_title._cid = linkedin_ads._ad_id
  LEFT JOIN campaigns
    ON campaigns._campaign_id = ads_title._campaign_id
),
s6sense_combined_sandler AS (
  SELECT
    s6ense_ads._ad_id,
    s6sense_airtable._ad_name,
    s6sense_airtable._campaign_id,
    s6sense_airtable._campaign_name,
    s6sense_airtable._ad_group,
    s6sense_airtable._ad_copy,
    s6sense_airtable._cta_copy,
    s6sense_airtable._design_template,
    s6sense_airtable._size,
    s6sense_airtable._platform,
    s6sense_airtable._segment,
    s6sense_airtable._design_color,
    s6sense_airtable._design_images,
    s6sense_airtable._design_blurp,
    s6sense_airtable._logos,
    s6sense_airtable._copy_messaging,
    s6sense_airtable._copy_asset_type,
    s6sense_airtable._copy_tone,
    s6sense_airtable._copy_product_company_name,
    s6sense_airtable._copy_statistic_proof_point,
    s6sense_airtable._cta_copy_soft_hard,
    s6sense_airtable._screenshot,
    s6sense_airtable._pain_point,
    s6sense_airtable._pillars,
    s6sense_airtable._instance,
    s6ense_ads._date,
    s6ense_ads._spend,
    s6ense_ads._clicks,
    s6ense_ads._impressions,
    s6ense_ads._conversions
  FROM s6ense_ads
  JOIN s6sense_airtable
    ON s6ense_ads._ad_id = s6sense_airtable._ad_id
    AND s6ense_ads._campaign_id = s6sense_airtable._campaign_id
),
ad_counts AS (
  SELECT
    ad.ad_group_id AS _ad_group_id,
    ad_group_name AS _ad_group_name,
    campaign_name AS _campaign_name,
    date AS _date,
    COUNT(DISTINCT ad.id) AS _ad_count
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
    CAST(ad.id AS STRING) AS _ad_id,
    airtable._ad_name AS _ad_name_airtable,
    '' AS _ad_copy,
    airtable._ad_visual AS _screenshot,
    '' AS _cta_copy,
    report.ad_group_id AS _ad_group_id,
    report.ad_group_name AS _ad_group_name,
    report.campaign_id AS _campaign_id,
    report.campaign_name AS _campaign_name,
    ad.name AS _ad_name,
    report.date AS _date,
    airtable._size AS _ad_size,
    EXTRACT(YEAR FROM report.date) AS _year,
    EXTRACT(MONTH FROM report.date) AS _month,
    EXTRACT(QUARTER FROM report.date) AS _quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS _quarter_year,
    cost_micros / 1000000 / c._ad_count AS _adjusted_spent,
    conversions / c._ad_count AS _adjusted_conversions,
    clicks / c._ad_count AS _adjusted_clicks,
    impressions / c._ad_count AS _adjusted_impressions,
    airtable._pain_point,
    airtable._pillars,
    c._ad_count
  FROM `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.sandler_google_ads.ads` ad
    ON ad.ad_group_id = report.ad_group_id
  JOIN `x-marketing.sandler_google_sheets.db_ads_optimization` airtable
    ON airtable._ad_id = CAST(ad.id AS STRING)
  JOIN ad_counts AS c
    ON ad.ad_group_id = c._ad_group_id
    AND report.date = c._date
  WHERE ad.name IS NOT NULL
    AND airtable._platform = 'Google Ads'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _ad_id, _campaign_id, _date ORDER BY _date DESC) = 1
),
google_display_combined AS (
  SELECT
    _ad_id,
    _ad_name,
    CAST(_campaign_id AS STRING),
    _campaign_name,
    _ad_group_name,
    '' AS _ad_copy,
    '' AS _cta_copy,
    '' AS _design_template,
    _ad_size AS _size,
    'Google Display' AS _platform,
    '' AS _segment,
    '' AS _design_color,
    '' AS _design_images,
    '' AS _design_blurp,
    '' AS _logos,
    '' AS _copy_messaging,
    '' AS _copy_asset_type,
    '' AS _copy_tone,
    '' AS _copy_product_company_name,
    '' AS _copy_statistic_proof_point,
    '' AS _cta_copy_soft_hard,
    _screenshot,
    _pain_point,
    _pillars,
    'Sandler Enterprise' AS _instance,
    _date,
    SUM(CAST(_adjusted_spent AS NUMERIC)) AS _total_spent,
    SUM(CAST(_adjusted_clicks AS NUMERIC)) AS _total_clicks,
    SUM(CAST(_adjusted_impressions AS NUMERIC)) AS _total_impressions,
    SUM(CAST(_adjusted_conversions AS NUMERIC)) AS _total_conversions,
  FROM adjusted_metrics
  GROUP BY ALL
  ORDER BY _campaign_name, _date DESC
),
google_overview AS (
  SELECT
    CAST(id AS STRING) AS _ad_id,
    '' AS _ad_name,
    CAST(campaign_id AS STRING),
    campaign_name AS _campaign_name,
    ad_group_name AS _ad_group_name,
    '' AS _ad_copy,
    '' AS _cta_copy,
    '' AS _design_template,
    '' AS _size,
    'Google SEM' AS _platform,
    '' AS _segment,
    '' AS _design_color,
    '' AS _design_images,
    '' AS _design_blurp,
    '' AS _logos,
    '' AS _copy_messaging,
    '' AS _copy_asset_type,
    '' AS _copy_tone,
    '' AS _copy_product_company_name,
    '' AS _copy_statistic_proof_point,
    '' AS _cta_copy_soft_hard,
    '' AS _screenshot,
    '' AS _pain_point,
    '' AS _pillars,
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
combined AS (
  SELECT
    * EXCEPT (_campaign_id),
    CAST(_campaign_id AS INT64) AS _campaign_id,
    EXTRACT(YEAR FROM _date) AS _year,
    EXTRACT(MONTH FROM _date) AS _month,
    EXTRACT(QUARTER FROM _date) AS _quarter,
    CONCAT('Q', EXTRACT(YEAR FROM _date), '-', EXTRACT(QUARTER FROM _date)) AS _quarter_year
  FROM union_data
)
SELECT
  combined.*,
  CASE 
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (4, 5, 6) THEN 
      CASE 
        WHEN _year = (SELECT MAX(_year) FROM combined) AND _quarter = 1 THEN 1
        WHEN _year = (SELECT MAX(_year) FROM combined) - 1 AND _quarter = 4 THEN 2
      END
    WHEN EXTRACT(MONTH FROM CURRENT_DATE()) IN (1, 2, 3) THEN 
      CASE 
        WHEN _year = (SELECT MAX(_year) - 1 FROM combined) AND _quarter = 4 THEN 1
        WHEN _year = (SELECT MAX(_year) - 1 FROM combined) AND _quarter = 3 THEN 2
      END
    ELSE 
      CASE 
        WHEN _year = (SELECT MAX(_year) FROM combined) AND _quarter = (SELECT MAX(_quarter) FROM combined) - 1 THEN 1
        WHEN _year = (SELECT MAX(_year) FROM combined) AND _quarter = (SELECT MAX(_quarter) FROM combined) - 2 THEN 2
      END
  END AS _quarter_partition
FROM combined
GROUP BY ALL;