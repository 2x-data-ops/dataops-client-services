TRUNCATE TABLE `ridecell.db_6sense_ad_performance`;

INSERT INTO `ridecell.db_6sense_ad_performance` (
    _campaignid,
    _advariation,
    _adid,
    _accountvtr,
    _spend,
    _clicks,
    _impressions,
    _date,
    _linkedincampaignid,
    _campaign_name,
    _campaign_type,
    _campaign_status,
    _start_date,
    _end_date,
    _budget,
    _newly_engaged_accounts,
    _increased_engagement_accounts,
    ad_group,
    _screenshot,
    _target_accounts,
    _reached_accounts,
    _6qa_accounts,
    _accountctr,
    _occurrence,
    _reduced_newly_engaged_accounts,
    _reduced_increased_engagement_accounts,
    _reduced_target_accounts,
    _reduced_reached_accounts
)

WITH ads AS (
    SELECT DISTINCT
        _campaignid,
        _name AS _advariation,
        _6senseid AS _adid,
        _accountvtr,
        CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
        CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
        SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
        CASE 
            WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
            WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
        END AS _date
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_campaign_performance`
    WHERE _datatype = 'Ad'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid, _6senseid, _date ORDER BY
        CASE 
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate) END) = 1
),
-- Get campaign level fields
campaign_fields AS (
    SELECT
        _campaignid,
        _accountvtr,
        _linkedincampaignid,
        CASE 
            WHEN _budget = '-' THEN NULL
            ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
        END AS _budget,
        CASE 
            WHEN _startDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _startDate)
            WHEN _startDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _startDate)
        END AS _start_date,
        CASE 
            WHEN _endDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _endDate)
            WHEN _endDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _endDate)
        END AS _end_date,
        _status AS _campaign_status,
        _name AS _campaign_name,
        _campaigntype AS _campaign_type, 
        CASE 
            WHEN _accountsnewlyengagedlifetime = '-' THEN 0
            ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
        END AS _newly_engaged_accounts,
        CASE 
            WHEN _accountswithincreasedengagementlifetime = '-' THEN 0
            ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
        END AS _increased_engagement_accounts
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_campaign_performance`
    WHERE _datatype = 'Campaign'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid ORDER BY
        CASE
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate) END DESC) = 1
),
ads_campaign_combined AS (
    SELECT ads.*,
        campaign_fields._linkedincampaignid,
        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        campaign_fields._budget,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts
    FROM ads
    JOIN campaign_fields
    ON ads._campaignid = campaign_fields._campaignid
),
airtable_fields AS (
    SELECT DISTINCT 
        _campaignid, 
        ad_id,
        ad_group,
        '' AS _screenshot
    FROM `x-marketing.ridecell_campaign.Campaigns` 
    WHERE _campaignid IS NOT NULL
),
combined_data AS (
    SELECT
        ads_campaign_combined.*,
        airtable_fields.ad_group,
        '' AS _screenshot
    FROM ads_campaign_combined
    LEFT JOIN airtable_fields 
    ON (
        (ads_campaign_combined._adid = CAST(airtable_fields.ad_id AS STRING)
        AND ads_campaign_combined._campaignid = CAST(airtable_fields._campaignid AS STRING))
        OR (airtable_fields.ad_id IS NULL
        AND ads_campaign_combined._campaignid = CAST(airtable_fields._campaignid AS STRING))
    )
    LEFT JOIN campaign_fields
    ON ads_campaign_combined._campaignid = CAST(campaign_fields._campaignid AS STRING)
),
-- Add campaign numbers to each ad
campaign_numbers AS (
    SELECT
      *
    FROM combined_data 
    -- Get accounts that are being targeted
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid, 
            COUNT(*) AS _target_accounts
        FROM (
            SELECT DISTINCT 
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
                ON main._segmentname = side.segment_name  
        )
        GROUP BY _campaignid
    ) target
    USING(_campaignid)
    -- Get accounts that have been reached
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid, 
            COUNT(*) AS _reached_accounts
        FROM (
            SELECT DISTINCT 
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain 
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
                ON main._segmentname = side.segment_name
            JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` extra
                ON main._6sensecompanyname = extra._6sensecompanyname
                AND main._6sensecountry = extra._6sensecountry
                AND main._6sensedomain = extra._6sensedomain
                AND CAST(side._campaignid AS STRING) = CAST(extra._campaignid AS STRING)
        )
        GROUP BY _campaignid
    ) reach
    USING(_campaignid)
    -- Get accounts that are 6QA
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _6qa_accounts
        FROM (
            SELECT DISTINCT 
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain  
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
                ON main._segmentname = side.segment_name
            JOIN `ridecell.db_6sense_account_current_state` extra
                ON main._6sensecompanyname = extra._6sensecompanyname
                AND main._6sensecountry = extra._6sensecountry
                AND main._6sensedomain = extra._6sensedomain
            WHERE extra._6qa_date IS NOT NULL
        )
        GROUP BY _campaignid
    )
    USING(_campaignid)    
    -- Get actr for each campaign - click divide reach
    LEFT JOIN (
        WITH main AS (
            SELECT DISTINCT 
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
            CAST(side._campaignid AS STRING) AS _campaignid, 
            CASE WHEN extra._clicks != '0' THEN 1 ELSE 0 END AS _clicked_account,
            CASE WHEN extra._campaignid != '' THEN 1 ELSE 0 END AS _reached_account
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain
            JOIN `x-marketing.ridecell.6sense_campaign_list` side 
                ON main._segmentname = side.segment_name
            JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` extra
                ON main._6sensecompanyname = extra._6sensecompanyname
                AND main._6sensecountry = extra._6sensecountry
                AND main._6sensedomain = extra._6sensedomain
        )
        SELECT
            _campaignid,
            SAFE_DIVIDE(SUM(_clicked_account), SUM(_reached_account)) AS _accountctr  
        FROM main
        GROUP BY _campaignid
    )
    USING(_campaignid)
),
-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY _campaignid) AS _occurrence
    FROM campaign_numbers
),
-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (
    SELECT
        *,
        _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
        _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
        _target_accounts / _occurrence AS _reduced_target_accounts,
        _reached_accounts / _occurrence AS _reduced_reached_accounts
    FROM total_ad_occurrence_per_campaign
)
SELECT
    * 
FROM reduced_campaign_numbers;