CREATE OR REPLACE TABLE `x-marketing.exiger.db_6sense_daily_campaign_performance` AS

WITH 
sixsense_airtable AS (
SELECT DISTINCT 
    Campaign_Name AS _campaign_name, 
    Campaign_ID AS _campaign_id, 
    Ad_ID AS _ad_id, 
    Ad_Type AS _ad_type, 
    Ad_Variation AS _ad_variation, 
    Ad_Name AS _ad_name, 
    Ad_Name_Length__100_char__limit_ AS _ad_name_length_100_char_limit, 
    Introduction_Text AS _introduction_text, 
    Intro_Text_Length__125_char__limit_ AS _intro_text_length_125_char_limit, 
    Headline_Text AS _headline_text, 
    Headline_Text_Length__60_char__limit_ AS _headline_text_length_60_char_limit, 
    Platform AS _platform, 
    Business_Segment AS _business_segment, 
    Landing_Page_URL AS _landing_page_url, 
    Ad_Visual AS _ad_visual, 
    Live_Date AS _live_date, 
    Completed_Date AS _completed_date, 
    Status AS _status, 
    Ad_Group AS _ad_group, 
    Ad_Group_ID AS _ad_group_id, 
    Ad_Group_Length AS _ad_group_length, 
    Job_Title AS _job_title, 
    Industry AS _industry, 
    Size AS _size, 
    Ad_Title AS _ad_title, 
    Ad_Title_Naming AS _ad_title_naming, 
    Ad_Title_Length AS _ad_title_length, 
    Sponsored_Text AS _sponsored_text, 
    Sponsored_Text_Length AS _sponsored_text_length, 
    Body_text AS _body_text, 
    Body_text_length__300_char__limit_ AS _body_text_length_300_char_limit, 
    Text_on_Image AS _text_on_image, 
    Text_on_Image_Length__55_char__limit_ AS _text_on_image_length_55_char_limit, 
    CTA_on_Image AS _cta_on_image, 
    CTA_on_Image_Length__20_char__limit_ AS _cta_on_image_length_20_char_limit, 
    utm_source AS _utm_source, 
    utm_medium AS _utm_medium, 
    utm_campaign AS _utm_campaign, 
    utm_content AS _utm_content, 
    utm_term AS _utm_term, 
    utm_id AS _utm_id, 
    Final_UTM_tracking_URL AS _final_utm_tracking_url, 
    Final_Ad_Files_Folder AS _final_ad_files_folder 
FROM `x-marketing.exiger_google_sheets.db_ads_optimization` 
WHERE Platform = '6sense'
),
 sixsense_campaigns AS (
  SELECT DISTINCT
    _6senseid AS _campaignid,
    _name AS _campaignname

  FROM `x-marketing.exiger_mysql.db_daily_campaign_performance`

  WHERE _datatype = 'Campaign'
)

, sixsense_base AS (
  SELECT
    base._6senseid AS _adid,
    base._name AS _adname,
    SAFE_CAST(base._campaignid AS INT64) AS _campaignid,
    sixsense_campaigns._campaignname,
    SAFE_CAST(base._adgroupid AS INT64) AS _adgroup
    
  FROM `x-marketing.exiger_mysql.db_daily_campaign_performance` AS base
  LEFT JOIN sixsense_campaigns
    ON base._campaignid = sixsense_campaigns._campaignid

  WHERE base._datatype = 'Ad'

  GROUP BY ALL
)

, sixsense_performance AS (
  SELECT
    performance._enddate, 
    performance._6senseid,
    performance._clicks, 
    performance._6senseid AS _adid, 
    performance._status, 
    performance._adgroupid, 
    performance._viewability, 
    performance._name, 
     CASE 
                WHEN performance._date LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', performance._date)
                ELSE PARSE_DATE('%F', performance._date)
            END AS _extractdate, 
    performance._accountreached, 
    performance._accountvtr, 
    performance._ecpm, 
    performance._ecpc, 
    performance._vtr, 
    performance._campaignid, 
    performance._datatype, 
    performance._viewthroughs, 
    performance._spend, 
    performance._campaigntype, 
    performance._startdate, 
    performance._accountctr, 
    '' AS _cpc, 
         CASE 
                WHEN performance._date LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y',performance._date)
                ELSE PARSE_DATE('%F', performance._date)
            END AS _date,
    performance._budget, 
    performance._impressions AS _impressions,
    performance._ctr

  FROM `x-marketing.exiger_mysql.db_daily_campaign_performance` AS performance
)

SELECT
  performance._enddate, 
  performance._6senseid, 
  performance._clicks, 
  performance._adid, 
  performance._status, 
  performance._adgroupid, 
  performance._viewability, 
  performance._name, 
  performance._extractdate, 
  performance._accountreached, 
  performance._accountvtr, 
  performance._ecpm, 
  performance._ecpc, 
  performance._vtr, 
  performance._campaignid, 
  performance._datatype, 
  performance._viewthroughs, 
  performance._spend, 
  performance._campaigntype, 
  performance._startdate, 
  performance._accountctr, 
  performance._cpc, 
  performance._date, 
  performance._budget, 
  performance._impressions, 
  performance._ctr,
  sixsense_base._adname,   
  sixsense_base._campaignname, 
  sixsense_base._adgroup, 
  "6Sense" AS _platform,
  sixsense_airtable._campaign_name, 
  sixsense_airtable._campaign_id, 
  sixsense_airtable._ad_type, 
  sixsense_airtable._ad_variation, 
  sixsense_airtable._ad_name, 
  sixsense_airtable._ad_name_length_100_char_limit, 
  sixsense_airtable._introduction_text, 
  sixsense_airtable._intro_text_length_125_char_limit, 
  sixsense_airtable._headline_text, 
  sixsense_airtable._headline_text_length_60_char_limit, 
  sixsense_airtable._business_segment, 
  sixsense_airtable._landing_page_url, 
  sixsense_airtable._ad_visual, 
  sixsense_airtable._live_date, 
  sixsense_airtable._completed_date, 
  sixsense_airtable._ad_group, 
  sixsense_airtable._ad_group_id, 
  sixsense_airtable._ad_group_length, 
  sixsense_airtable._job_title, 
  sixsense_airtable._industry, 
  sixsense_airtable._size, 
  sixsense_airtable._ad_title, 
  sixsense_airtable._ad_title_naming, 
  sixsense_airtable._ad_title_length, 
  sixsense_airtable._sponsored_text, 
 sixsense_airtable._sponsored_text_length, 
  sixsense_airtable._body_text, 
  sixsense_airtable._body_text_length_300_char_limit, 
  sixsense_airtable._text_on_image, 
  sixsense_airtable._text_on_image_length_55_char_limit, 
  sixsense_airtable._cta_on_image, 
  sixsense_airtable._cta_on_image_length_20_char_limit, 
  sixsense_airtable._utm_source, 
  sixsense_airtable._utm_medium, 
  sixsense_airtable._utm_campaign, 
  sixsense_airtable._utm_content, 
  sixsense_airtable._utm_term, 
  sixsense_airtable._utm_id, 
  sixsense_airtable._final_utm_tracking_url, 
  sixsense_airtable._final_ad_files_folder 


FROM sixsense_performance AS performance
LEFT JOIN sixsense_base 
  ON sixsense_base._adid = performance._adid
LEFT JOIN sixsense_airtable 
  ON sixsense_base._adid = CAST(sixsense_airtable._ad_id AS STRING);


CREATE OR REPLACE TABLE `x-marketing.exiger.6sense_ads_performance` AS

WITH 
sixsense_airtable AS (
SELECT DISTINCT 
    Campaign_Name AS _campaign_name, 
    Campaign_ID AS _campaign_id, 
    Ad_ID AS _ad_id, 
    Ad_Type AS _ad_type, 
    Ad_Variation AS _ad_variation, 
    Ad_Name AS _ad_name, 
    Ad_Name_Length__100_char__limit_ AS _ad_name_length_100_char_limit, 
    Introduction_Text AS _introduction_text, 
    Intro_Text_Length__125_char__limit_ AS _intro_text_length_125_char_limit, 
    Headline_Text AS _headline_text, 
    Headline_Text_Length__60_char__limit_ AS _headline_text_length_60_char_limit, 
    Platform AS _platform, 
    Business_Segment AS _business_segment, 
    Landing_Page_URL AS _landing_page_url, 
    Ad_Visual AS _ad_visual, 
    Live_Date AS _live_date, 
    Completed_Date AS _completed_date, 
    Status AS _status, 
    Ad_Group AS _ad_group, 
    Ad_Group_ID AS _ad_group_id, 
    Ad_Group_Length AS _ad_group_length, 
    Job_Title AS _job_title, 
    Industry AS _industry, 
    Size AS _size, 
    Ad_Title AS _ad_title, 
    Ad_Title_Naming AS _ad_title_naming, 
    Ad_Title_Length AS _ad_title_length, 
    Sponsored_Text AS _sponsored_text, 
    Sponsored_Text_Length AS _sponsored_text_length, 
    Body_text AS _body_text, 
    Body_text_length__300_char__limit_ AS _body_text_length_300_char_limit, 
    Text_on_Image AS _text_on_image, 
    Text_on_Image_Length__55_char__limit_ AS _text_on_image_length_55_char_limit, 
    CTA_on_Image AS _cta_on_image, 
    CTA_on_Image_Length__20_char__limit_ AS _cta_on_image_length_20_char_limit, 
    File_Type AS _file_type, 
    utm_source AS _utm_source, 
    utm_medium AS _utm_medium, 
    utm_campaign AS _utm_campaign, 
    utm_content AS _utm_content, 
    utm_term AS _utm_term, 
    utm_id AS _utm_id, 
    Final_UTM_tracking_URL AS _final_utm_tracking_url, 
    Final_Ad_Files_Folder AS _final_ad_files_folder 
FROM `x-marketing.exiger_google_sheets.db_ads_optimization` 
WHERE Platform = '6sense'
), 
sixsense_campaigns AS (
  SELECT DISTINCT
    _6senseid AS _campaignid,
    _name AS _campaignname
  FROM `x-marketing.exiger.db_6sense_daily_campaign_performance`
  WHERE _datatype = 'Campaign'
), 
sixsense_base AS (  
  SELECT
    base._6senseid AS _adid,
    base._name AS _adname,
    SAFE_CAST(base._campaignid AS INT64) AS _campaignid,
    sixsense_campaigns._campaignname,
    SAFE_CAST(base._adgroupid AS INT64) AS _adgroup,
     _date,
    SAFE_CAST(REPLACE(base._spend, '$', '') AS FLOAT64) AS _spend,
    SAFE_CAST(base._clicks AS INT64) AS _clicks,
    SAFE_CAST(REPLACE(base._impressions , ',', '')  AS INT64) AS _impressions,
    base._accountreached AS _reach,
    0 AS _conversions,
    0 AS _video_views,
  FROM `x-marketing.exiger.db_6sense_daily_campaign_performance` AS base
  LEFT JOIN sixsense_campaigns
    ON base._campaignid = sixsense_campaigns._campaignid
  WHERE base._datatype = 'Ad'
  
)
SELECT
  sixsense_base._adid, 
  sixsense_base._adname, 
  sixsense_base._campaignid,  
  sixsense_base._campaignname, 
  sixsense_base._adgroup,
  "6Sense" AS _platform, 
  sixsense_base._date,
  sixsense_base._spend,
  sixsense_base._clicks,
  sixsense_base._impressions,
  sixsense_base._reach,
  sixsense_base._conversions,
  sixsense_base._video_views,

  sixsense_airtable._campaign_name, 
  sixsense_airtable._campaign_id, 
  sixsense_airtable._ad_type, 
  sixsense_airtable._ad_variation, 
  sixsense_airtable._ad_name, 
  sixsense_airtable._ad_name_length_100_char_limit, 
  sixsense_airtable._introduction_text, 
  sixsense_airtable._intro_text_length_125_char_limit, 
  sixsense_airtable._headline_text, 
  sixsense_airtable._headline_text_length_60_char_limit, 
  sixsense_airtable._business_segment, 
  sixsense_airtable._landing_page_url, 
  sixsense_airtable._ad_visual, 
  sixsense_airtable._live_date, 
  sixsense_airtable._completed_date, 
  sixsense_airtable._ad_group, 
  sixsense_airtable._ad_group_id, 
  sixsense_airtable._ad_group_length, 
  sixsense_airtable._job_title, 
  sixsense_airtable._industry, 
  sixsense_airtable._size, 
  sixsense_airtable._ad_title, 
  sixsense_airtable._ad_title_naming, 
  sixsense_airtable._ad_title_length, 
  sixsense_airtable._sponsored_text, 
 sixsense_airtable._sponsored_text_length, 
  sixsense_airtable._body_text, 
  sixsense_airtable._body_text_length_300_char_limit, 
  sixsense_airtable._text_on_image, 
  sixsense_airtable._text_on_image_length_55_char_limit, 
  sixsense_airtable._cta_on_image, 
  sixsense_airtable._cta_on_image_length_20_char_limit, 
  sixsense_airtable._utm_source, 
  sixsense_airtable._utm_medium, 
  sixsense_airtable._utm_campaign, 
  sixsense_airtable._utm_content, 
  sixsense_airtable._utm_term, 
  sixsense_airtable._utm_id, 
  sixsense_airtable._final_utm_tracking_url, 
  sixsense_airtable._final_ad_files_folder
FROM sixsense_base
LEFT JOIN sixsense_airtable 
  ON sixsense_base._adid = CAST(sixsense_airtable._ad_id AS STRING)
