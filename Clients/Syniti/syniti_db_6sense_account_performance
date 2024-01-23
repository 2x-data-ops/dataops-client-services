
CREATE OR REPLACE TABLE `syniti.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        CASE
            WHEN main._segmentname = 'S4_HANA - NA' THEN 'S4/HANA - NA'
            WHEN main._segmentname = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
            WHEN main._segmentname = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
            ELSE main._segmentname
        END AS _segmentname,              
        side._segment,
        side._campaignid,
        side._campaignid AS _campaign_id,
        side._campaignname AS _campaign_name
        -- CASE 
        --     WHEN side._campaignname = '[Influitive] [6Sense] ABX Low Intent TAM - October 2023' 
        --     THEN '[Influitive] [6Sense] Low Intent TAM - October 2023'
        --     ELSE side._campaignname
        -- END AS _campaign_name -- temporary while fixing airtable

    FROM 
        `syniti_mysql.syniti_db_target_accounts` main
    
    LEFT JOIN 
        `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
    
    ON 
        main._segmentname = side._segment

),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (

    SELECT DISTINCT 

        main.* EXCEPT(_campaignid),

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
        AS _has_impressions

    FROM 
        target_accounts AS main

    LEFT JOIN 
        `syniti_mysql.syniti_db_campaign_reached_accounts` side 

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts;



-- SELECT DISTINCT _segment FROM syniti_mysql.syniti_optimization_airtable_ads_6sense




-- SELECT DISTINCT _segmentname FROM syniti_mysql.syniti_db_target_accounts
