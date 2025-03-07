----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- 6SENSE ENGAGEMENT LOG


WITH target_accounts AS (

    SELECT * FROM `smartcom.db_6sense_account_current_state`

),
reached_accounts_data AS (
    SELECT
        main._clicks,
        main._influencedformfills,
        main._latestimpression,
        main._extractdate AS _activities_on,
        main._campaignid,
        side._campaignname,
        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    FROM `smartcomm_mysql.smartcommunications_db_reached_account_6sense` main
    JOIN (
        SELECT DISTINCT
            _campaignid,
            _campaignname
        FROM `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
        ) side
    USING (_campaignid)
),
6sense_campaign_reached AS (
    SELECT
        '6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaignname AS _description, 
        1 AS _notes
    FROM reached_accounts_data
)
SELECT * FROM 6sense_campaign_reached


SELECT target_accounts.*,
reached_accounts_data.*
FROM target_accounts
JOIN reached_accounts_data
USING (_country_account)

