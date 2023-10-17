

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid,
        side._name AS _campaignname
    FROM 
        `sandler_mysql.db_segment_target_accounts` main
    
    JOIN 
        `sandler_mysql.db_airtable_6sense_segment` side
    
    USING(_segmentname)

    WHERE
        side._sdc_deleted_at IS NULL

),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (

    SELECT DISTINCT 
        main.*,

        CASE 
            WHEN side._campaignid IS NOT NULL 
            THEN true 
        END 
        AS _is_reached,

        CASE 
            WHEN CAST(REPLACE(side._clicks, ',', '') AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_clicks,

        CASE 
            WHEN CAST(REPLACE(side._impressions, ',', '') AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_impressions,

    FROM 
        target_accounts AS main

    LEFT JOIN 
        `sandler_mysql.db_campaign_reached_account_new` side 

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts;