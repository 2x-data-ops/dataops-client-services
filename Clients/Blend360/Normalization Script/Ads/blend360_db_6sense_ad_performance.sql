CREATE OR REPLACE TABLE `x-marketing.blend360.db_6sense_ad_performance` AS
WITH ads_li AS (
        SELECT DISTINCT
            _licampaignid AS _campaign_id,
            _name AS _advariation,
            _liadid AS _adid,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
            CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
            CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
            CASE 
              WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
              WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
              END AS _date,
        FROM `x-marketing.blend360_mysql.db_6s_li_daily_campaign_performance`
        WHERE _datatype = 'Ad' 
          AND _sdc_deleted_at IS NULL
),

airtable_fields_li AS (

    SELECT 
        DISTINCT _campaignid AS _campaign_id, 
        _adid AS _ad_id,
        _adname,
        _campaignname AS _campaign_name,
        _adgroup AS _ad_group,
        _screenshot,
        _adtype,
        _platform,
        _segment
    FROM
        `x-marketing.blend360_mysql.optimization_airtable_ads_linkedin`

),

combined_data_li AS (

    SELECT
        ads.*,
        airtable_fields._campaign_name,
        airtable_fields._ad_group,
        airtable_fields._screenshot,
        airtable_fields._adtype,
        airtable_fields._platform,
        airtable_fields._segment,
        DATE_TRUNC(_date, MONTH) AS _month_year
    FROM 
        ads_li ads

    JOIN
        airtable_fields_li airtable_fields
    ON 
        ads._advariation = airtable_fields._adname

),

ads_6sense AS (
    SELECT 
        DISTINCT _campaignid AS _campaign_id,
        _name AS _advariation,
        _6senseid AS _adid,
        CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
        CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
        CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
        CASE 
            WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
            WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
        END AS _date,
        FROM `x-marketing.blend360_mysql.db_6s_daily_campaign_performance`
        WHERE _datatype = 'Ad'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid, _6senseid, _date ORDER BY 
            CASE WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                 WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate) END) = 1

),

airtable_fields_6sense AS (

    SELECT 
        DISTINCT _campaignid AS _campaign_id, 
        _adid AS _ad_id,
        _adname,
        _campaignname AS _campaign_name,
        _adgroup AS _ad_group,
        _screenshot,
        _adtype,
        _platform,
        _segment
    FROM
        `x-marketing.blend360_mysql.optimization_airtable_ads_6sense`
),

combined_data_6sense AS (

    SELECT
        ads.*,
        airtable_fields._campaign_name,
        airtable_fields._ad_group,
        airtable_fields._screenshot,
        airtable_fields._adtype,
        airtable_fields._platform,
        airtable_fields._segment,
        DATE_TRUNC(_date, MONTH) AS _month_year
    FROM 
        ads_6sense ads

    JOIN
        airtable_fields_6sense airtable_fields 
    ON (
            ads._adid = airtable_fields._ad_id
        AND 
            ads._campaign_id = airtable_fields._campaign_id
    )
)

SELECT * FROM combined_data_li
UNION ALL
SELECT * FROM combined_data_6sense;