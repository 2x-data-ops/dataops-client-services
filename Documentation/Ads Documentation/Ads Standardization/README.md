# Ads Data Standardization

# **LinkedIn ads**

### Raw Data Source

| Table Name | Data Source | Description |
| --- | --- | --- |
| account_user | Stitch – linkedin_ads | User’s role for the particular account |
| accounts | Stitch – linkedin_ads
   | LinkedIn Campaign Manager account details (can integrate more than 1 account) |
| ad_analytics_by_campaign | Stitch – linkedin_ads | Daily performance by various metrics in campaign level
   |
| ad_analytics_by_creative | Stitch – linkedin_ads | Daily performance by various metrics in ads level
   |
| campaign_groups | Stitch – linkedin_ads | List of campaign groups |
| campaigns | Stitch – linkedin_ads | List of campaigns |
| creatives | Stitch – linkedin_ads | List of ads |
| video_ads | Stitch – linkedin_ads | List of video ads |
| optimization_airtable_ads_linkedin | Airtable – MySQL Playground | Ads/Campaign details and structure that are not available in the raw data from Stitch |

### ERD

![ERD  ads (6).png](Ads%20Data%20Standardization%200e36d6322af346be97a4a03884c38767/26e9124b-8f9b-4fd4-a847-fd095178cadd.png)

### Data Dictionary

| Field (BQ) |  Field (Looker)
   | View | Data Dependency | Definition | Formula | Sample |
| --- | --- | --- | --- | --- | --- | --- |
| _date | Date | Current Day | ad_analytics_by_creative | Date in year -month-day derived from the start_at field |  | 2022-02-11 |
| _quarter_startdate | Current Quarter | Current Day, Current Quarter | ad_analytics_by_creative  | Quarter in quarter-year derived from the start_at field |  | Q1-2024 |
| _creative_id | Ads (Creative) ID | Current Day | ad_analytics_by_creative | Ad ID that associated with the unique identification for each ads | 
  
   | 168641874 |
| _leads | Leads Generated  | Current Day | ad_analytics_by_creative | Leads generation by daily for the specific ads derived from the one_click_leads |  | 0,1,2,3,4 |
| _spent | Ad Spent | Current Day | ad_analytics_by_creative | Number of daily spent in USD for the specific ads derived from the cost_in_usd |  | $4.83 |
| _impressions | Impressions | Current Day | ad_analytics_by_creative | Number of daily impressions for the specific ads |  | 301 |
| _clicks | Clicks | Current Day | ad_analytics_by_creative
   | Number of daily clicks for the specific ads |  | 4 |
| _conversions | Conversions | Current Day | ad_analytics_by_creative | Number of daily conversions for the specific ads derived from external_website_conversions |  | 2 |
| _landing_pages_click | Landing page clicks | Current Day   | ad_analytics_by_creative | Number of daily landing page clicks for the specific ads |  | 3   |
| _video_views   | Video view | Current Day | ad_analytics_by_creative | Number of daily video view |  | 3  |
| _account_id | LI Campaign Manager’s Account ID | Current Day | campaigns | LI Campaign Manager’s account ID that associated with the unique identification for the specific account |  | 507253375 |
| _campaign_id | Campaign ID | Current Day | campaigns | Campaign ID that associated with the unique identification for the specific campaign |  | 292838653 |
| _campaign_name | Campaign Name | Current Day | campaigns | Campaign Name for the specific campaign ID |  | [B2B DG] - Webinar - UK |
| _campaign_status | Campaign Status | Current Day | campaigns | Status of the campaign run | INITCAP(status) | Completed, Paused, Active  |
| _campaign_objective | Campaign Objective | Current Day | campaigns | Specific objective for each campaign run associated with the objective_type field | INITCAP(REPLACE(objective_type,"_"," ")) | Video View, Website Conversion |
| _daily_budget | Daily budget | Current Day | campaigns | Daily budget for each campaign |  | $50 |
| _cost_type | Cost Type | Current Day | campaigns | Type of metrics being used to measure the spent performance based on campaign objective that had been set |  | CPM, CPC   |
| _group_id   | Ad Group ID | Current Day | campaign_groups | Ad Group ID that associated with the unique identification for each ad group |  | 624838843 |
| _group_name | Ad Group Name | Current Day | campaign_groups | Ad Group name for the specific ad group ID |  | Spend_Acct, Emburse It   |
| _campaign_group_status | Ad Group Status | Current Day | campaign_groups | Status of ad group run | INITCAP(status) | Completed, Paused, Active  |
| - | CTR | Current Day | Aggregated Field - Dashboard | Click through rate | SUM(_clicks) / SUM(_impressions) | 3.34% |
| - | CPC | Current Day | Aggregated Field - Dashboard | Cost-per-clicks | SUM(_clicks) / SUM(_spent) | $5.55 |
| - | CPM | Current Day | Aggregated Field - Dashboard | Cost-per-mille | SUM(_spent) / SUM(_impressions) x 1000 | $10.2 |
| _ads_per_campaign | Ads per campaign | Current Day | ad_analytics_by_creative, creatives, campaigns | Count of ads that associate to the particular campaign | COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames) | 2,3,4,5 |
| _daily_budget_per_ad | Daily Budget per ads | Current Day | ad_analytics_by_creative, creatives, campaigns
   | Reduced daily budget by ads level to ensure the campaign budget distribute equally to avoid the duplication | CASE WHEN ads_per_campaign > 0  THEN dailyBudget / ads_per_campaign ELSE 0 END | 0.7683 |
| _ad_name | Ad Name | Current Day | optimization_airtable_ads_linkedin | Name of the respective ad based on the ad ID |  | Lettuce-Jan-2024-V3 |
| _screenshot | Screenshot of ad graphic | Current Day | optimization_airtable_ads_linkedin | Graphic for each ad either image, video etc |  | https://dp.2x.marketing/airtable-images/m/[2024-03-27-00-34-58]___Capture.PNG |

### Gold Standard Script Example - emburse_ads_performance

```sql
WITH LI_ads AS (
 SELECT
    EXTRACT(DATE FROM start_at) AS _date,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views
  FROM
    `emburse_linkedin_ads.ad_analytics_by_creative` 
    ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `emburse_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id,
    INITCAP(REPLACE(objective_type,"_"," ")) AS campaign_objective
  FROM
    `emburse_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status) AS status
  FROM
    `emburse_linkedin_ads.campaign_groups`
),
_all AS (
SELECT
  LI_ads.*,
  campaigns.account_id,
  campaigns.campaignID,
  campaigns._campaignNames,
  campaigns.campaign_country_region,
  campaigns.status AS _campaign_status,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaigns.campaign_objective,
  campaign_group.status AS _campaign_group_status
FROM
  LI_ads
RIGHT JOIN
  ads_title
ON
  CAST(LI_ads.creative_id AS STRING) = ads_title.cID
LEFT JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
LEFT JOIN
  campaign_group
ON
  campaigns.campaign_group_id = campaign_group.groupID
),
total_ads AS (
  SELECT *, COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
  FROM _all
),
daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
      AS dailyBudget_per_ad
  FROM total_ads
) 
SELECT * FROM daily_budget_per_ad_per_campaign
WHERE _date IS NOT NULL;
```

# Ads Optimization / Ads Consolidation Report

### Raw Data Source

| Table Name | Data Source | Description |
| --- | --- | --- |
| account_user | Stitch – linkedin_ads | User’s role for the particular account |
| accounts | Stitch – linkedin_ads | LinkedIn Campaign Manager account details (can integrate more than 1 account) |
| ad_analytics_by_campaign | Stitch – linkedin_ads | Daily performance by various metrics in campaign level |
| ad_analytics_by_creative | Stitch – linkedin_ads | Daily performance by various metrics in ads level |
| campaign_groups | Stitch – linkedin_ads | List of campaign groups |
| campaigns | Stitch – linkedin_ads | List of campaigns |
| creatives | Stitch – linkedin_ads | List of ads |
| video_ads | Stitch – linkedin_ads | List of video ads |
| optimization_airtable_ads_linkedin | Airtable – MySQL Playground | Ads/Campaign details and structure that are not available in the raw data from Stitch |
| db_6sense_daily_campaign_performance | CSV2SQL - MySQL Playground | Daily performance by various metrics in ads, campaign and ad group level respectively |
| optimization_airtable_ads_6sense | Airtable – MySQL Playground | Ads/Campaign details and structure that are not available in the raw data from Stitch |
| optimization_airtable_ads_google_display | Airtable – MySQL Playground | Ads/Campaign details and structure that are not available in the raw data from Stitch |
| account_performance_report | Stitch – google_ads | Overall performance for every metrics from each account |
| accounts | Stitch – google_ads | List of accounts that connected to one integration |
| ad_group_performance_report | Stitch – google_ads | Overall performance for every metrics from ad group level (Google Search and Display included) |
| ad_groups | Stitch – google_ads | List of ad groups (Google Search and Display included) |
| ad_performance_report | Stitch – google_ads | Overall performance for every metrics from ad level (Google Search only) |
| ads | Stitch – google_ads | List of ad (Google Search and Display included – ad name only available for Google Display) |
| campaign_performance_report | Stitch – google_ads | Overall performance for every metrics from campaign level (Google Search and Display included) |
| campaigns | Stitch – google_ads | List of campaign (Google Search and Display included) |
| keywords_performance_report | Stitch – google_ads | Overall Google Search’s keywords performance for each metrics |
| search_query_performance_report | Stitch – google_ads | 
  Overall Google Search’s search
  term performance based on each keywords for each metrics
   |
| video_performance_report | Stitch – google_ads | Overall video performance for each metrics |
| optimization_airtable_ads_google | Airtable – MySQL Playground
   | Ads/Campaign details and structure that are not available in the raw data from Stitch |

### ERD

![ERD  ads (7).png](Ads%20Data%20Standardization%200e36d6322af346be97a4a03884c38767/ERD__ads_(7).png)

### Gold Standard Script - sandler_ads_optimization

```sql
WITH
linkedin_ads AS (
    SELECT
      CAST(creative_id AS STRING) AS _adid,
      CAST(start_at AS TIMESTAMP) AS _date,
      SUM(cost_in_usd) AS _spend, 
      SUM(clicks) AS _clicks, 
      SUM(impressions) AS _impressions,
      SUM(external_website_conversions) AS _conversions
    FROM
      `x-marketing.sandler_linkedin_ads_v2.ad_analytics_by_creative`
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
      `x-marketing.sandler_linkedin_ads_v2.creatives`
),
campaigns AS (
    SELECT 
      id AS _campaignID,
      name AS _campaignname
    FROM 
      `x-marketing.sandler_linkedin_ads_v2.campaigns`
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
    FROM
      `x-marketing.sandler.db_6sense_ads_overview`
    WHERE 
      _date IS NOT NULL
    GROUP BY 
      _adid, adName, campaignID, _date   
),
linkedin_airtable_network AS (
  SELECT
    _adid, 
    _adname, 
    CASE WHEN _campaignid = "" THEN NULL ELSE _campaignid END AS _campaignid,  
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
    'Sandler Network' AS _instance
  FROM
    `x-marketing.sandlernetwork_mysql.sandlernetwork_optimization_airtable_ads_linkedin`
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
  GROUP BY ALL
),
linkedin_airtable_franchise AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
    '' AS _adgroup,
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
    'Sandler' AS _instance
  FROM
    `x-marketing.sandler_mysql.optimization_airtable_ads_linkedin`
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
  GROUP BY ALL
),
s6sense_airtable AS (
  SELECT
    _adid, 
    _adname, 
    _campaignid,  
    _campaignname, 
    '' AS _adgroup,
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
    'Sandler' AS _instance
  FROM
    `x-marketing.sandler_mysql.optimization_airtable_ads_6sense` 
  WHERE 
    LENGTH(_adid) > 2 AND _campaignid IS NOT NULL
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
  FROM 
    linkedin_ads
  JOIN
    linkedin_airtable_network ON linkedin_ads._adid = CAST(linkedin_airtable_network._adid AS STRING)
  RIGHT JOIN 
    ads_title ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN 
    campaigns ON campaigns._campaignID = ads_title.campaign_id
),
linkedin_combined_sandler AS (
  SELECT
    linkedin_airtable_franchise.*,
    linkedin_ads._date,
    linkedin_ads._spend,
    linkedin_ads._clicks,
    linkedin_ads._impressions,
    linkedin_ads._conversions
  FROM 
    linkedin_ads
  JOIN
    linkedin_airtable_franchise ON linkedin_ads._adid = CAST(linkedin_airtable_franchise._adid AS STRING)
  LEFT JOIN 
    ads_title ON ads_title.cID = linkedin_ads._adid
  LEFT JOIN 
    campaigns ON campaigns._campaignID = ads_title.campaign_id
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
    s6sense_airtable._instance,
    s6ense_ads._date,
    s6ense_ads._spend,
    s6ense_ads._clicks,
    s6ense_ads._impressions,
    s6ense_ads._conversions
  FROM 
    s6ense_ads
  JOIN
    s6sense_airtable ON s6ense_ads._adid = s6sense_airtable._adid AND s6ense_ads._campaignid = s6sense_airtable._campaignid
),
google_display_combined AS (

WITH ad_counts AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM
    `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN
    `x-marketing.sandler_google_ads.ads` ad ON ad.ad_group_id = report.ad_group_id
  JOIN
    `x-marketing.sandler_mysql.db_airtable_google_display_ads` airtable ON airtable._adid = CAST(ad.id AS STRING)
  WHERE
    ad.name IS NOT NULL
  GROUP BY
    ad.ad_group_id,
    ad_group_name,
    campaign_name,
    date
),

adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    airtable._adname,
    '' AS _adcopy,
    _screenshot,
    '' AS _ctacopy,
    report.ad_group_id, 
    report.ad_group_name, 
    report.campaign_id,
    report.campaign_name, 
    ad.name AS ad_name, 
    report.date AS _date,
    airtable._adsize,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent, 
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks, 
    impressions / c.ad_count AS adjusted_impressions,
    ad_count
  FROM
    `x-marketing.sandler_google_ads.ad_group_performance_report` report
  JOIN
    `x-marketing.sandler_google_ads.ads` ad ON ad.ad_group_id = report.ad_group_id
  JOIN
    `x-marketing.sandler_mysql.db_airtable_google_display_ads` airtable ON airtable._adid = CAST(ad.id AS STRING)
  JOIN
    ad_counts c ON ad.ad_group_id = c.ad_group_id AND report.date = c.date
  WHERE
    ad.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ad.id, campaign_id, report.date ORDER BY report.date DESC) = 1
)

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
  'Sandler' AS _instance,
  _date,
  SUM(CAST(adjusted_spent AS NUMERIC)) AS total_spent,
  SUM(CAST(adjusted_clicks AS NUMERIC)) AS total_clicks,
  SUM(CAST(adjusted_impressions AS NUMERIC)) AS total_impressions,
  SUM(CAST(adjusted_conversions AS NUMERIC)) AS total_conversions,
FROM
  adjusted_metrics
GROUP BY ALL
ORDER BY
  campaign_name, _date DESC  

),

google_sem AS (
  WITH google_overview AS (
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
  'Sandler' AS _instance,
  date AS _date,
  CAST(cost_micros / 1000000 AS NUMERIC) AS _spent,
  clicks AS _clicks,
  impressions AS _impressions,
  conversions AS _conversions,
  FROM
    `x-marketing.sandler_google_ads.ad_performance_report` report
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id, ad_group_id, id ORDER BY _sdc_received_at DESC) = 1
),
 
 aggregated AS (
  SELECT * EXCEPT (_spent, _clicks, _impressions, _conversions),
  SUM(_spent) AS _spent,
  SUM(_clicks) AS _clicks,
  SUM(_impressions) AS _impressions,
  SUM(_conversions) AS _conversions
  FROM google_overview
  GROUP BY ALL
 )
 SELECT *
 FROM aggregated

),
_all AS (
  SELECT * EXCEPT (_campaignID),
    CAST(_campaignID AS INT64) AS _campaignid,
    EXTRACT(YEAR FROM _date) AS year,
    EXTRACT(MONTH FROM _date) AS month,
    EXTRACT(QUARTER FROM _date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM _date), '-', EXTRACT(QUARTER FROM _date)) AS quarteryear
  FROM (
    SELECT * FROM linkedin_combined_network
    UNION ALL
    SELECT * FROM linkedin_combined_sandler
    UNION ALL
    SELECT * FROM s6sense_combined_sandler
    UNION ALL
    SELECT * FROM google_display_combined
    UNION ALL
    SELECT * FROM google_sem
  )
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
FROM 
  _all
GROUP BY 
  ALL;
```

# Google Ads

### Raw Data Source

| Table Name | Data Source | Description |
| --- | --- | --- |
| account_performance_report | Stitch – google_ads | Overall performance for every metrics from each account |
| accounts | Stitch – google_ads | List of accounts that connected to one integration |
| ad_group_performance_report | Stitch – google_ads | Overall performance for every metrics from ad group level (Google Search and Display included) |
| ad_groups | Stitch – google_ads | List of ad groups (Google Search and Display included) |
| ad_performance_report | Stitch – google_ads | Overall performance for every metrics from ad level (Google Search only) |
| ads | Stitch – google_ads | List of ad (Google Search and Display included – ad name only available for Google Display) |
| campaign_performance_report | Stitch – google_ads | Overall performance for every metrics from campaign level (Google Search and Display included) |
| campaigns | Stitch – google_ads | List of campaign (Google Search and Display included) |
| keywords_performance_report | Stitch – google_ads | Overall Google Search’s keywords performance for each metrics |
| search_query_performance_report | Stitch – google_ads | Overall Google Search’s search term performance based on each keywords for each metrics |
| video_performance_report | Stitch – google_ads | Overall video performance for each metrics |
| optimization_airtable_ads_google | Airtable – MySQL Playground | Ads/Campaign details and structure that are not available in the raw data from Stitch |

### Gold Standard Script - emburse_ads_performance

```sql
---------------Google Ads----------------------
---Google Search Campaign Performance
CREATE OR REPLACE TABLE emburse.google_search_campaign_performance AS
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
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
      campaign_status,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM
      `emburse_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'SEARCH'
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    day,
    currency,
    campaign_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6, 7
  ORDER BY
  day, campaign_id;

-- Google Search Ads Variation Performance

CREATE OR REPLACE TABLE emburse.google_search_adsvariation_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      ads.campaign_id,
      campaign_name,
      ads.ad_group_id,
      ad_group_name,
      date AS day,
      ads.id AS ad_id,
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
      ad_group_status,
      RANK() OVER(
        PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
        ORDER BY ads._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.ad_performance_report` ads
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    day,
    ad_id,
    headlines,
    final_urls,
    currency,
    ad_group_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ORDER BY
   day, campaign_id, ad_group_id, ad_id;

-- Google Seach Keyword Performance

CREATE OR REPLACE TABLE emburse.google_search_keyword_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
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
      RANK() OVER (
        PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
        ORDER BY keywords._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.keywords_performance_report` keywords
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    match_type,
    keyword,
    quality_score,
    day,
    currency,
    ad_group_criterion_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ORDER BY
  day, campaign_id, ad_group_id, keyword;

-- Google Search Query Performance

CREATE OR REPLACE TABLE emburse.google_search_query_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
      SELECT
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        keyword.info.text AS keyword,
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
        RANK() OVER(
          PARTITION BY date, campaign_id, ad_group_id, keyword.info.text, search_term_view_search_term
          ORDER BY _sdc_received_at DESC
        ) AS _rank
      FROM `emburse_google_ads.search_query_performance_report`
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    keyword,
    search_term,
    day,
    currency,
    campaign_status,
    ad_group_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM 
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8,9,10
ORDER BY
  day, campaign_id, ad_group_id, keyword, search_term;

-- Google Display Campaign Performance

CREATE OR REPLACE TABLE emburse.google_display_campaign_performance AS

WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
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
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM 
      `emburse_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'DISPLAY'
  )
  WHERE _rank = 1
)
  SELECT
    campaign_id,
    campaign_name,
    day,
    currency,
    budget,
    campaign_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(active_view_impressions) AS active_view_impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6
ORDER BY
  day, campaign_id;

--- google video performance

CREATE OR REPLACE TABLE `x-marketing.emburse.video_performance` AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank)
    FROM (
SELECT
            report.campaign_id, 
            report.campaign_name,
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
            RANK() OVER(
                PARTITION BY date, campaign_id,report.video_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.emburse_google_ads.video_performance_report` report 
    )
    WHERE _rank = 1
)
    SELECT
        campaign_id, 
        campaign_name, 
        day,
        currency,
        network_type,
        video_title,
        video_channel_id,
        group_status,
        campaign_status,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv,
        SUM(_view_views) AS view_views,
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5,6,7,8,9
    ORDER BY day, campaign_id;
```