----Linkedin Ads Performance

TRUNCATE TABLE`x-marketing.lumivero.linkedin_ads_performance`;
INSERT INTO`x-marketing.lumivero.linkedin_ads_performance` ( 
  _start_year,
  _start_month,
  _start_day,
  _end_month,
  _end_year,
  _end_day,
  _last_start_day,
  _start_week,
  _start_quater,
  _start_month_num,
  _weekday,
  _start_month_name,
  _start_week_num,
  _date,
  _quater_startdate,
  _adid,
  _startdate,
  _enddate,
  _impressions,
  _clicks,
  _conversions,
  _spent,
  _leads,
  _landing_pages_clicks,
  _lead_form_opens,
  _video_play,
  _video_views_25percent,
  _video_views_50percent,
  _video_views_75percent,
  _video_completions,
  _total_engagements, 
    _other_engagements, 
    _sends, 
    _opens,
  _cta_copy,
  _layout,
  _size,
  _platform,
  _business_segment,
  _color,
  _image,
  _blurb,
  _logo,
  _messaging,
  _asset_type,
  _tone,
  _product_company_name,
  _statistic_proof_point,
  _screenshot,
  _creativedirections,
  _live_date,
  _campaignid,
  _campaignname,
  _campaign_status,
  _campaign_group_id,
  _campaign_group_name,
  _daily_budget,
  _cost_type,
  _campaign_group_status,
  _account_id,
  _account_name,
  _ads_per_campaign,
  _dailyBudget_per_ad 
)
WITH LI_airtable AS (
  SELECT
    _ad_id AS _adid,
    _ad_type,
    _ad_variation,
    _ad_name,
    _ad_name_length,
    _introduction_text,
    _intro_text_length,
    _headline_text,
    _headline_text_length,
    _platform,
    _business_segment,
    _landing_page_url,
    _ad_visual,
    IF( _live_date != '', PARSE_TIMESTAMP('%d/%m/%Y', _live_date), NULL ) AS _live_date,
    _completed_date,
    _status,
    _ad_group_length,
    _job_title,
    _industry,
    _size,
    _ad_title,
    _ad_title_naming,
    _ad_title_length,
    _sponsored_text,
    _sponsored_text_length,
    _body_text,
    _body_text_length,
    _text_on_image,
    _text_on_image_length,
    _cta_on_image,
    _cta_on_image_length,
    _cta_copy,
    _attachment_file_type,
    _layout,
    _color,
    _image,
    _blurb,
    _logo,
    _messaging,
    _asset_type,
    _tone,
    _product_company_name,
    _stage,
    _campaign_objective,
    _main_keywords,
    _template,
    _statistic_proof_point
  FROM `x-marketing.lumivero_google_sheets.db_ads_optimization`
  WHERE _platform = 'LinkedIn' 
),LI_ads AS (
  SELECT
    date_range.start.year AS _start_year,
    date_range.start.month AS _start_month,
    date_range.start.day AS _start_day,
    date_range.END.month AS _end_month,
    date_range.END.year AS _end_year,
    date_range.END.day AS _end_day,
    LAST_DAY( CAST(start_at AS DATE),WEEK(MONDAY)) AS _last_start_day,
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
    --video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions,
    total_engagements AS _total_engagements, 
    other_engagements AS _other_engagements, 
    sends AS _sends, 
    opens AS _opens, 
  FROM`x-marketing.lumivero_linkedin_ads.ad_analytics_by_creative`
  ORDER BY start_at DESC 
),ads_title AS (
  SELECT
    SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS _adid,
    campaign_id AS _campaignid,
    account_id AS _account_id,
    REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
  FROM`x-marketing.lumivero_linkedin_ads.creatives` c
  LEFT JOIN`x-marketing.lumivero_linkedin_ads.accounts` acc
    ON acc.id = account_id 
),campaigns AS (
  SELECT
    id AS _campaignid,
    name AS _campaignname,
    status AS _campaign_status,
    cost_type AS _cost_type,
    daily_budget.amount AS _daily_budget,
    campaign_group_id AS _campaign_group_id,
  FROM`x-marketing.lumivero_linkedin_ads.campaigns` 
),campaign_group AS (
  SELECT
    id AS _campaign_group_id,
    name AS _campaign_group_name,
    status AS _campaign_group_status
  FROM`x-marketing.lumivero_linkedin_ads.campaign_groups` 
),_all AS (
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
     _total_engagements, 
    _other_engagements, 
    _sends, 
    _opens,
    LI_airtable._cta_copy,
    LI_airtable._layout,
    LI_airtable._size,
    "LinkedIn" AS _platform,
    LI_airtable._business_segment,
    LI_airtable._color,
    LI_airtable._image,
    LI_airtable._blurb,
    LI_airtable._logo,
    LI_airtable._messaging,
    LI_airtable._asset_type,
    LI_airtable._tone,
    LI_airtable._product_company_name,
    LI_airtable._statistic_proof_point,
    LI_airtable._ad_visual AS _screenshot,
    LI_airtable._ad_title_naming AS _creativedirections,
    LI_airtable._live_date,
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
),total_ads AS (
  SELECT
    *,
    COUNT(_adid) OVER (PARTITION BY _startDate, _campaignName ) AS _ads_per_campaign
  FROM _all 
), daily_budget_per_ad_per_campaign AS (
  SELECT
    *,
    CASE WHEN _ads_per_campaign > 0 THEN _daily_budget / _ads_per_campaign 
    ELSE 0 END AS _dailyBudget_per_ad
  FROM total_ads 
)
SELECT
  daily_budget_per_ad_per_campaign.*
FROM daily_budget_per_ad_per_campaign;

---Bings Ads Performance
---bing keyword performance
TRUNCATE TABLE`x-marketing.lumivero.bing_keyword_performance`;
INSERT INTO `x-marketing.lumivero.bing_keyword_performance` ( 
  adgroupid,
  timeperiod,
  campaignname,
  campaign_country_region,
  campaignid,
  addistribution,
  currencycode,
  cost,
  impressions,
  clicks,
  AbsoluteTopImpressionRatePercent,
  ctr,
  avgcpc,
  conversions,
  conversionrate,
  deliveredmatchtype,
  keyword,
  qualityscore,
  bidmatchtype,
  dailybudget 
)
WITH keywords AS (
  SELECT
    adgroupid,
    keywords.timeperiod,
    keywords.campaignname,
    CASE WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL END AS campaign_country_region,
    keywords.campaignid,
    addistribution,
    keywords.currencycode,
    keywords.spend AS cost,
    keywords.impressions,
    keywords.clicks,
    CAST(REPLACE(keywords.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent,
    keywords.ctr,
    keywords.averagecpc AS avgcpc,
    keywords.conversions,
    keywords.conversionrate,
    keywords.deliveredmatchtype,
    keywords.keyword,
    keywords.qualityscore,
    bidmatchtype
  FROM `x-marketing.lumivero_bing_ads.keyword_performance_report` keywords
  QUALIFY
    RANK() OVER (PARTITION BY keywords.timeperiod, keywords.keywordid ORDER BY keywords._sdc_report_datetime DESC) = 1 
),budget AS (
  SELECT
    id AS campaignid,
    dailybudget
  FROM`x-marketing.lumivero_bing_ads.campaigns` 
)
SELECT
  keywords.*,
  budget.dailybudget
FROM keywords
JOIN budget
  ON keywords.campaignid = budget.campaignid;

---bing ads variation performance
TRUNCATE TABLE`x-marketing.lumivero.bing_adsvariation_performance`;
INSERT INTO`x-marketing.lumivero.bing_adsvariation_performance` ( 
  adgroupid,
  adgroupname, 
  adgroupstatus,
  -- titlepart1,
  -- titlepart2,
  -- titlepart3,
  campaignname,
  campaign_country_region,
  campaignid,
  basecampaignid, 
  campaigntype, 
  campaignstatus,
  adid,
  timeperiod,
  currencycode,
  cost,
  impressions,
  clicks,
  AbsoluteTopImpressionRatePercent,
  conversions,
  conversionrate,
  _sdc_report_datetime,
  dailybudget 
)
WITH
  ads AS (
  SELECT
    adgroupid,
    adgroupname, 
    adgroupstatus,
    --titlepart1,
    --titlepart2,
    --titlepart3,
    ads.campaignname,
    CASE
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL
  END
    AS campaign_country_region,
    ads.campaignid,
    basecampaignid, 
    campaigntype, 
    campaignstatus,
    ads.adid,
    ads.timeperiod,
    ads.currencycode,
    ads.spend AS cost,
    ads.impressions,
    ads.clicks,
    CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent,
    ads.conversions,
    ads.conversionrate,
    ads._sdc_report_datetime
  FROM `x-marketing.lumivero_bing_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (PARTITION BY ads.timeperiod, ads.adid ORDER BY ads._sdc_report_datetime DESC) = 1 
),budget AS (
  SELECT
    id AS campaignid,
    dailybudget
  FROM`x-marketing.lumivero_bing_ads.campaigns` 
)
SELECT
  ads.*,
  budget.dailybudget
FROM ads
JOIN budget
  ON ads.campaignid = budget.campaignid;

---Bing Campaign Performance
TRUNCATE TABLE `x-marketing.lumivero.bing_campaign_performance`;
INSERT INTO `x-marketing.lumivero.bing_campaign_performance` (
  timeperiod,	
  campaignname,	
  campaign_country_region,	
  campaignid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  conversions,	
  campaign_status,
  dailybudget
)

WITH campaign AS (
  SELECT
    campaign.timeperiod, 
    campaign.campaignname,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    campaign.campaignid, 
    addistribution,
    campaign.currencycode, 
    campaign.spend AS cost,
    campaign.impressions,
    campaign.clicks,
    CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64) AS AbsoluteTopImpressionRatePercent, 
    campaign.conversions,
    campaign.campaignstatus AS campaign_status
  FROM `x-marketing.lumivero_bing_ads.campaign_performance_report` campaign
  QUALIFY RANK() OVER (
    PARTITION BY campaign.timeperiod, campaign.campaignid
    ORDER BY campaign._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.lumivero_bing_ads.campaigns`
)
SELECT 
  campaign.*, 
  budget.dailybudget 
FROM campaign
JOIN budget 
  ON campaign.campaignid = budget.campaignid;

---Bing Ad Group Performance
TRUNCATE TABLE `x-marketing.lumivero.bing_adgroup_performance`;
INSERT INTO `x-marketing.lumivero.bing_adgroup_performance` (
  timeperiod,	
  adgroupid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  ctr,	
  avgcpc,	
  conversions,	
  conversionrate,
  campaignname, 
  basecampaignid, 
  campaigntype, 
  campaignstatus,	
  dailybudget
)

WITH adgroups AS (
  SELECT
    adgroups.timeperiod, 
    adgroups.adgroupid, 
    addistribution, 
    campaignid,
    adgroups.currencycode, 
    adgroups.allreturnonadspend AS cost, 
    adgroups.impressions , 
    adgroups.clicks, 
    CAST(REPLACE(adgroups.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
    adgroups.ctr, 
    adgroups.averagecpc AS avgcpc,
    adgroups.conversions , 
    adgroups.conversionrate,
    campaignname, 
    basecampaignid, 
    campaigntype, 
    campaignstatus
  FROM `x-marketing.lumivero_bing_ads.ad_group_performance_report` adgroups
  QUALIFY RANK() OVER (
    PARTITION BY adgroups.timeperiod, adgroups.adgroupid
    ORDER BY adgroups._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.lumivero_bing_ads.campaigns`
)
SELECT 
  adgroups.* EXCEPT (campaignid), 
  budget.dailybudget
FROM adgroups
JOIN budget 
  ON adgroups.campaignid = budget.campaignid;

---Bing Search Query Performance
TRUNCATE TABLE `x-marketing.lumivero.bing_search_query_performance`;
INSERT INTO `x-marketing.lumivero.bing_search_query_performance` (
  campaignid,	
  campaignname,	
  campaign_country_region,	
  adgroupid,	
  adgroupname,	
  keyword,	
  search_term,	
  day,	
  cost,	
  impressions,	
  clicks,	
  conversions,	
  campaignstatus,	
  adgroupstatus
)

SELECT
  campaignid,
  campaignname,
  CASE 
    WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
    WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
    WHEN campaignname LIKE '%Germany%' THEN 'Germany'
    WHEN campaignname LIKE '%UK%' THEN 'UK'
    WHEN campaignname LIKE '%US%' THEN 'US'
    WHEN campaignname LIKE '%APAC%' THEN 'APAC'
    WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
    WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
    ELSE NULL 
  END AS campaign_country_region,
  adgroupid,
  adgroupname,
  keyword AS keyword,
  -- search_term_match_type AS match_type,
  searchquery AS search_term,
  timeperiod AS day,
  spend AS cost,
  impressions,
  clicks,
  conversions,
  campaignstatus,
  adgroupstatus
FROM `x-marketing.lumivero_bing_ads.search_query_performance_report`
QUALIFY RANK() OVER (
  PARTITION BY timeperiod, campaignid, adgroupid, keyword, searchquery
  ORDER BY _sdc_received_at DESC) = 1;