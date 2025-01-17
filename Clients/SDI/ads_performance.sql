CREATE OR REPLACE TABLE `x-marketing.sdi.consolidate_ad_performance` AS
WITH unique_rows_google_ads AS (
  SELECT
    ads.campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ads.ad_group_id,
    ad_group_name,
    date AS day,
    ads.id AS ad_id,
    cost_micros/1000000 AS spent,
    impressions,
    clicks,
    conversions,
   -- INITCAP(campaign_status) AS campaign_status
  FROM `x-marketing.sdi_google_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
    ORDER BY ads._sdc_received_at DESC) = 1
),

google_ads AS (
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    day,
    ad_id,
    --campaign_status,
    'Google' AS _platform,
    SUM(spent) AS spent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions
  FROM unique_rows_google_ads
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

unique_rows_campaign_level AS (
  SELECT
    ads.campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    SAFE_CAST('' AS INT64) AS ad_group_id, --ads.ad_group_id, -- Not in campaign performance
    '' AS ad_group_name, --ad_group_name, -- Not in campaign performance
    date AS day,
    SAFE_CAST('' AS INT64) AS ad_id, --ads.id AS ad_id, -- Not in campaign performance
    cost_micros/1000000 AS spent,
    impressions,
    clicks,
    conversions,
    --INITCAP(campaign_status) AS campaign_status
  FROM `x-marketing.sdi_google_ads.campaign_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id
    ORDER BY ads._sdc_received_at DESC) = 1
),

google_campaign_level AS (
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    day,
    ad_id,
    --campaign_status,
    'Google' AS _platform,
    SUM(spent) AS spent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions
  FROM unique_rows_campaign_level
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

ad_counts_display AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM `x-marketing.sdi_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.sdi_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
 --WHERE ad.name IS NOT NULL
  GROUP BY ad.ad_group_id, ad_group_name, campaign_name, date
),

adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    --airtable._adname,
    '' AS _adcopy,
    --_screenshot,
    '' AS _ctacopy,
    report.ad_group_id, 
    report.ad_group_name, 
    report.campaign_id,
    report.campaign_name,
    CASE 
      WHEN report.campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN report.campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN report.campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN report.campaign_name LIKE '%UK%' THEN 'UK'
      WHEN report.campaign_name LIKE '%US%' THEN 'US'
      WHEN report.campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN report.campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN report.campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    expanded_dynamic_search_ad.description AS ad_name, 
    report.date AS _date,
    --report.campaign_status,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent, 
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks, 
    impressions / c.ad_count AS adjusted_impressions,
    ad_count
  FROM `x-marketing.sdi_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.sdi_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
  JOIN ad_counts_display c 
    ON ad.ad_group_id = c.ad_group_id 
    AND report.date = c.date
  --WHERE ad.name IS NOT NULL
  QUALIFY RANK() OVER (
    PARTITION BY ad.id, campaign_id, report.date 
    ORDER BY report.date DESC) = 1
),

google_display AS (
  SELECT
    CAST(campaign_id AS INT64) AS campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    _date AS day,
    CAST(_adid AS INT64) AS ad_id,
    --campaign_status,
    'Google Display' AS _platform, 
    SUM(CAST(adjusted_spent AS FLOAT64)) AS spent,
    SUM(CAST(adjusted_impressions AS INT64)) AS impressions,
    SUM(CAST(adjusted_clicks AS INT64)) AS clicks,
    SUM(CAST(adjusted_conversions AS FLOAT64)) AS conversions,
  FROM adjusted_metrics
  GROUP BY ALL
),

LI_ads AS (
  SELECT
    start_at AS _date,
    creative_id,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
  FROM `x-marketing.sdi_linkedin_ads_v2.ad_analytics_by_creative` 

),
LI_ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.sdi_linkedin_ads_v2.creatives`

),
LI_campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    CASE 
      WHEN name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN name LIKE '%US/CA%' THEN 'US/CA'
      WHEN name LIKE '%Germany%' THEN 'Germany'
      WHEN name LIKE '%UK%' THEN 'UK'
      WHEN name LIKE '%US%' THEN 'US'
      WHEN name LIKE '%APAC%' THEN 'APAC'
      WHEN name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM `x-marketing.sdi_linkedin_ads_v2.campaigns`
),

LI_campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status)
  FROM `x-marketing.sdi_linkedin_ads_v2.campaign_groups`
),

linkedin_ads AS (
  SELECT
    LI_campaigns.campaignID,
    LI_campaigns._campaignNames,
    LI_campaigns.campaign_country_region,
    LI_campaign_group.groupID,
    LI_campaign_group._groupName,
    LI_ads._date,
    LI_ads.creative_id,
    LI_campaigns.status,
    'LinkedIn' AS _platform,
    LI_ads._spent,
    LI_ads._impressions,
    LI_ads._clicks,
    LI_ads._conversions
  FROM LI_ads
  RIGHT JOIN LI_ads_title
    ON CAST(LI_ads.creative_id AS STRING) = LI_ads_title.cID
  LEFT JOIN LI_campaigns
    ON LI_ads_title.campaign_id = LI_campaigns.campaignID
  LEFT JOIN LI_campaign_group
    ON LI_campaigns.campaign_group_id = LI_campaign_group.groupID
  WHERE _date IS NOT NULL
) , campaign_status AS (
  SELECT DISTINCT
    campaign_id,
    campaign_name,
    campaign_status,
  FROM `x-marketing.sdi_google_ads.campaign_performance_report` report
  QUALIFY RANK() OVER (
    PARTITION BY campaign_id 
    ORDER BY date DESC,report._sdc_received_at DESC) = 1
)
, google_combine AS (
  SELECT 
  * , "Google Ads" AS _segment
  FROM google_ads
  UNION ALL
  SELECT 
  * , "Google Campaign"
  FROM google_campaign_level
  UNION ALL
  SELECT 
  *, 'Google Display'
  FROM google_display
)
, google_status AS (
  SELECT google_combine.campaign_id,
  google_combine.campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  day,
  ad_id,
  status.campaign_status,
  _platform,
  spent,
  impressions,
  clicks,
  conversions,
  _segment

  FROM google_combine 
  LEFT JOIN campaign_status status ON google_combine.campaign_id = status.campaign_id
) 
SELECT * 
FROM google_status
UNION ALL
SELECT 
  * , 'LinkedIn'
FROM linkedin_ads ;

---------------Linkedin Ads------------------

CREATE OR REPLACE TABLE `x-marketing.sdi.linkedin_ads_performance` AS
WITH LI_airtable AS (
    SELECT
        _adid, 
        _adtitle AS _adname, 
        _campaignid,  
        _campaignname, 
        _adgroup,
        _adcopy, 
        _ctacopy, 
        IF(LENGTH(_designtemplate) > 0, _designtemplate, _layout) AS _layout,
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
        _creativedirections
    FROM  `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_linkedin`
    
    WHERE LENGTH(_adid) > 2
)
, LI_ads AS (
    SELECT
        date_range.start.year AS _start_year, 
        date_range.start.month AS _start_month, 
        date_range.start.day AS _start_day,
        date_range.end.month AS _end_month,
        date_range.end.year AS _end_year, 
        date_range.end.day AS _end_day,
        LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS _last_start_day,
        TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS _start_week,
        TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS _start_quater,
        TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS _start_month_num,
        FORMAT_DATETIME('%A', start_at) AS _weekday,
        FORMAT_DATE('%B', start_at) AS _start_month_name,
        EXTRACT(WEEK FROM start_at) AS _start_week_num,
        EXTRACT(DATE FROM start_at) AS _date,
        CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
        creative_id AS _adid,
        DATE(start_at) AS _startdate,
        DATE(end_at) AS _enddate,
        --approximate_unique_impressions AS _reach,
        impressions AS _impressions,
        clicks AS _clicks,
        external_website_conversions AS _conversions,
        cost_in_usd AS _spent,
        one_click_leads AS _leads,
        landing_page_clicks AS _landing_pages_clicks,
        ---video_views AS _video_views,
        one_click_lead_form_opens AS _lead_form_opens,
        video_starts AS _video_play,
        video_first_quartile_completions AS _video_views_25percent,
        video_midpoint_completions AS _video_views_50percent,
        video_third_quartile_completions AS _video_views_75percent,
        video_completions AS _video_completions
    FROM `sdi_linkedin_ads_v2.ad_analytics_by_creative`
  
    ORDER BY start_at DESC
)
, ads_title AS (
    SELECT
        SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS _adid,
        campaign_id AS _campaignid,
        account_id AS _account_id,
        REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
    FROM `sdi_linkedin_ads_v2.creatives` c
    LEFT JOIN `sdi_linkedin_ads_v2.accounts` acc 
        ON acc.id = account_id
)
, campaigns AS (
    SELECT
        id AS _campaignid,
        name AS _campaignname,
        status AS _campaign_status,
        cost_type AS _cost_type,
        daily_budget.amount AS _daily_budget,
        campaign_group_id AS _campaign_group_id,
    FROM `sdi_linkedin_ads_v2.campaigns`
    
)
, campaign_group AS (
    SELECT
        id AS _campaign_group_id, 
        name AS _campaign_group_name, 
        status AS _campaign_group_status
    FROM `sdi_linkedin_ads_v2.campaign_groups`
)
, _all AS (
    SELECT
        LI_ads._start_year, 
        LI_ads._start_month, 
        LI_ads._start_day,
        LI_ads._end_month,
        LI_ads._end_year, 
        LI_ads._end_day,
        LI_ads._last_start_day,
        LI_ads._start_week,
        LI_ads._start_quater,
        LI_ads._start_month_num,
        LI_ads._weekday,
        LI_ads._start_month_name,
        LI_ads._start_week_num,
        LI_ads._date,
        LI_ads._quater_startdate,
        LI_ads._adid,
        LI_ads._startdate,
        LI_ads._enddate,
        --LI_ads._reach,
        LI_ads._impressions,
        LI_ads._clicks,
        LI_ads._conversions,
        LI_ads._spent,
        LI_ads._leads,
        LI_ads._landing_pages_clicks,
        --LI_ads._video_views,
        LI_ads._lead_form_opens,
        LI_ads._video_play,
        LI_ads._video_views_25percent,
        LI_ads._video_views_50percent,
        LI_ads._video_views_75percent,
        LI_ads._video_completions,
        LI_airtable._adcopy, 
        LI_airtable._ctacopy, 
        LI_airtable._layout,
        LI_airtable._size, 
        "LinkedIn" AS _platform, 
        LI_airtable._segment,
        LI_airtable._designcolor,
        LI_airtable._designimages,
        LI_airtable._designblurp,
        LI_airtable._logos,
        LI_airtable._copymessaging,
        LI_airtable._copyassettype,
        LI_airtable._copytone,
        LI_airtable._copyproductcompanyname,
        LI_airtable._copystatisticproofpoint,
        LI_airtable._ctacopysofthard, 
        LI_airtable._screenshot,
        LI_airtable._creativedirections,
        campaigns._campaignid,
        campaigns._campaignname,
        campaigns._campaign_status,
        campaign_group._campaign_group_id,
        campaign_group._campaign_group_name,
        campaigns._daily_budget,
        campaigns._cost_type,
        campaign_group._campaign_group_status,
        ads_title._account_id,
        ads_title._account_name
    FROM LI_ads
    RIGHT JOIN ads_title 
        ON CAST( LI_ads._adid AS STRING) = ads_title._adid
    LEFT JOIN campaigns 
        ON ads_title._campaignid = campaigns._campaignid 
    LEFT JOIN campaign_group 
        ON campaigns._campaign_group_id = campaign_group._campaign_group_id
    LEFT JOIN LI_airtable 
        ON CAST( LI_ads._adid AS STRING) = CAST(LI_airtable._adid AS STRING)
)
, total_ads AS (
    SELECT 
        *, 
        count(_adid) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
    FROM _all
)
, daily_budget_per_ad_per_campaign AS (
    SELECT 
        *,
        CASE WHEN ads_per_campaign > 0 THEN _daily_budget / ads_per_campaign
        ELSE 0 END AS dailyBudget_per_ad
    FROM total_ads
)
    SELECT 
        daily_budget_per_ad_per_campaign.*
    FROM daily_budget_per_ad_per_campaign;

---------------Google Ads----------------------
---Google Campaign Performance
TRUNCATE TABLE `x-marketing.sdi.google_search_campaign_performance`;
INSERT INTO `x-marketing.sdi.google_search_campaign_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  day,	
  currency,	
  campaign_status,	
  customer_time_zone,	
  campaign_advertising_channel_type,	
  cost,	
  impressions,	
  search_impressions,	
  clicks,	
  conversions,	
  view_through_conv,	
  conv_value
)

WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    date AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    conversions,
    view_through_conversions AS view_through_conv,
    conversions_value AS conv_value,
    campaign_status,
    customer_time_zone,
    INITCAP(campaign_advertising_channel_type) AS campaign_advertising_channel_type,
  FROM `x-marketing.sdi_google_ads.campaign_performance_report` report
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id 
    ORDER BY report._sdc_received_at DESC) = 1

)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  day,
  currency,
  campaign_status,
  customer_time_zone,
  campaign_advertising_channel_type,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(conv_value) AS conv_value
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY day, campaign_id;

-- Google Search Ads Variation Performance
TRUNCATE TABLE `x-marketing.sdi.google_search_adsvariation_performance`;
INSERT INTO  `x-marketing.sdi.google_search_adsvariation_performance`
 (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  day,	
  ad_id,	
  ads_status,
  primary_status,primary_status_reasons,
  headlines,	
  final_urls,	
  currency,	
  ad_group_status,	
  customer_time_zone,	
  campaign_status,
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv,	
  conv_value
) 
WITH ads AS (
   SELECT 
   id AS as_id, 
   status,
   primary_status,primary_status_reasons 
   FROM  `x-marketing.sdi_google_ads.ads` 
),unique_rows AS (
  SELECT
    ads.campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ads.ad_group_id,
    ad_group_name,
    date AS day,
    ads.id AS ad_id,
    CONCAT (primary_status,'-',primary_status_reasons) AS ads_status,
    primary_status,primary_status_reasons,
    CASE
      WHEN ads.type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(ads.responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
    END AS headlines,
    TRIM(ads.final_urls, "[']") AS final_urls,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    conversions_value AS conv_value,
    ad_group_status,
    customer_time_zone,
    INITCAP(campaign_status) AS campaign_status
  FROM `sdi_google_ads.ad_performance_report` ads
  LEFT JOIN ads ads_name ON ads.id = ads_name.as_id
  QUALIFY RANK() OVER (
    PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
    ORDER BY ads._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  day,
  ad_id,
  ads_status,
  primary_status,primary_status_reasons,
  headlines,
  final_urls,
  currency,
  ad_group_status,
  customer_time_zone,
  campaign_status,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(conv_value) AS conv_value
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16
ORDER BY day, campaign_id, ad_group_id, ad_id;

-- Google Seach Keyword Performance
TRUNCATE TABLE `x-marketing.sdi.google_search_keyword_performance`;
INSERT INTO `x-marketing.sdi.google_search_keyword_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  match_type,	
  keyword,	
  quality_score,	
  day,	
  currency,	
  ad_group_criterion_status,	
  customer_time_zone,	
  campaign_status, ad_group_status,
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ad_group_id,
    ad_group_name,
    ad_group_criterion_keyword.match_type AS match_type,
    ad_group_criterion_keyword.text AS keyword,
    ad_group_criterion_quality_info.quality_score AS quality_score,
    date AS day,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    ad_group_criterion_status,
    customer_time_zone,
    campaign_status,
     ad_group_status
  FROM `x-marketing.sdi_google_ads.keywords_performance_report` keywords
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
    ORDER BY keywords._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  match_type,
  keyword,
  quality_score,
  day,
  currency,
  ad_group_criterion_status,
  customer_time_zone,
  campaign_status,
  ad_group_status,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14
ORDER BY day, campaign_id, ad_group_id, keyword;

-- Google Search Query Performance
TRUNCATE TABLE `x-marketing.sdi.google_search_query_performance`;
INSERT INTO `x-marketing.sdi.google_search_query_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  keyword,	
  search_term,	
  day,	
  currency,	
  campaign_status,	
  ad_group_status,	
  customer_time_zone,	
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)

WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ad_group_id,
    ad_group_name,
    keyword.info.text AS keyword,
    -- search_term_match_type AS match_type,
    search_term_view_search_term AS search_term,
    date AS day,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    ad_group_status,
    customer_time_zone
  FROM `x-marketing.sdi_google_ads.search_query_performance_report`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date, campaign_id, ad_group_id, keyword/*.info.text*/, search_term_view_search_term
    ORDER BY _sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  keyword,
  -- match_type,
  search_term,
  day,
  currency,
  campaign_status,
  ad_group_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,9,10, 11,12
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;

-- Google Display Campaign Performance
TRUNCATE TABLE `x-marketing.sdi.google_display_campaign_performance`;
INSERT INTO `x-marketing.sdi.google_display_campaign_performance`  (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  day,	
  currency,	
  budget,	
  campaign_status,	
  customer_time_zone,	
  cost,	
  impressions,	
  active_view_impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)

WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    date AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget,
    cost_micros/1000000 AS cost,
    impressions,
    active_view_impressions,
    CASE
      WHEN ad_network_type = 'DISPLAY' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    customer_time_zone
  FROM `x-marketing.sdi_google_ads.campaign_performance_report` report
  WHERE campaign_advertising_channel_type = 'DISPLAY'
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id
    ORDER BY report._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  day,
  currency,
  budget,
  campaign_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(active_view_impressions) AS active_view_impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5,6,7,8
ORDER BY day, campaign_id;

--- google video performance
-- TRUNCATE TABLE `x-marketing.sdi.video_performance`;
-- INSERT INTO `x-marketing.sdi.video_performance` (
--   campaign_id,
--   campaign_name,	
--   campaign_country_region,	
--   day,	
--   currency,	
--   network_type,	
--   video_title,	
--   video_channel_id,	
--   group_status,	
--   campaign_status,	
--   customer_time_zone,	
--   cost,	
--   impressions,
--   search_impressions,
--   clicks,
--   conversions,
--   view_through_conv,
--   view_views
-- )
/*
WITH unique_rows AS (
  SELECT
    report.campaign_id, 
    report.campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    report.date AS day, 
    report.customer_currency_code AS currency,
    report.cost_micros/1000000 AS cost, 
    report.impressions, 
    CASE
      WHEN report.ad_network_type = 'VIDEO'
      THEN report.impressions
      ELSE NULL
    END search_impressions,
    report.clicks, 
    report.conversions, 
    report.view_through_conversions AS view_through_conv,
    report.ad_network_type AS network_type, 
    video_title AS video_title,
    video_channel_id,
    video_id,
    ad_group_status AS group_status,
    report.campaign_status AS campaign_status,
    report.video_views AS _view_views,
    customer_time_zone
  FROM `x-marketing.sdi_google_ads.video_performance_report` report 
  QUALIFY RANK() OVER (
  PARTITION BY date, campaign_id,report.video_id
    ORDER BY report._sdc_received_at DESC) = 1
)
SELECT
  campaign_id, 
  campaign_name, 
  campaign_country_region,
  day,
  currency,
  network_type,
  video_title,
  video_channel_id,
  group_status,
  campaign_status,
  customer_time_zone,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(_view_views) AS view_views
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5,6,7,8,9,10, 11
ORDER BY day, campaign_id;*/