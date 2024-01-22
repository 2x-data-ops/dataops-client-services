CREATE OR REPLACE TABLE `syniti.db_6sense_ad_performance` AS

WITH ads AS (
SELECT *
    EXCEPT (_rownum)
    FROM (
        SELECT DISTINCT
            _campaignid AS _campaign_id,
            _name AS _advariation,
            _6senseid AS _adid,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64
                ) AS _spend,
            CAST(REPLACE(_clicks, ',', '') AS INTEGER) AS _clicks,
            CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
            CASE 
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _extractdate)
                        WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
                    END AS _extractdate,
            ROW_NUMBER() OVER (
                    PARTITION BY _campaignid,
                    _6senseid,
                    _extractdate
                    ORDER BY CASE 
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%e/%m/%Y', _extractdate)
                        WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
                    END
                ) AS _rownum
        FROM `syniti_mysql.syniti_db_campaign_performance`
        WHERE _datatype = 'Ad'
    )
    WHERE _rownum = 1
),
-- Get campaign level fields

campaign_fields AS (
    
    SELECT
        * EXCEPT(_extractdate, _rownum)
    FROM (

        SELECT

            _campaignid AS _campaign_id,
            
            CASE
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END
            AS _extractdate,

            CASE 
                WHEN _startDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _startDate)
                WHEN _startDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _startDate)
            END
            AS _start_date,

            CASE 
                WHEN _endDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _endDate)
                WHEN _endDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _endDate)
            END
            AS _end_date,

            _status AS _campaign_status,
            _name AS _campaign_name,
            _campaigntype AS _campaign_type,
            
            CASE 
                WHEN _accountsnewlyengagedlifetime = '-'
                THEN 0
                ELSE CAST(_accountsnewlyengagedlifetime AS INT64)
            END 
            AS _newly_engaged_accounts,

            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-'
                THEN 0
                ELSE CAST(_accountswithincreasedengagementlifetime AS INT64)
            END 
            AS _increased_engagement_accounts,

            ROW_NUMBER() OVER(
                PARTITION BY _campaignid
                ORDER BY 
                    CASE
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC
            ) 
            AS _rownum

        FROM 
            `syniti_mysql.syniti_db_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),

--airtable later

combined_data AS (

    SELECT

        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        ads.*,
        -- airtable_fields._ad_group,
        -- airtable_fields._screenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts

    FROM 
        ads

    -- LEFT JOIN
    --     airtable_fields 
    -- ON (
    --         ads._ad_id = airtable_fields._ad_id
    --     AND 
    --         ads._campaign_id = airtable_fields._campaign_id
    -- )
    -- OR (
    --         airtable_fields._ad_id IS NULL
    --     AND 
    --         ads._campaign_id = airtable_fields._campaign_id
    -- )

    LEFT JOIN 
        campaign_fields
    ON 
        ads._campaign_id = campaign_fields._campaign_id

),

-- Add campaign numbers to each ad
campaign_numbers AS (

    SELECT
        *
    FROM
        combined_data 

    -- Get accounts that are being targeted
    LEFT JOIN (
        
        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _target_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

        )
        GROUP BY 
            1

    ) target

    USING(_campaign_id)

    -- Get accounts that have been reached
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _reached_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `syniti_mysql.syniti_db_campaign_reached_accounts` extra

            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain,
                _campaignid
            )
            
        )
        GROUP BY 
            1

    ) reach

    USING(_campaign_id)

    -- Get accounts that are 6QA
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _6qa_accounts
        
        FROM (
            
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,

            FROM 
                `syniti_mysql.syniti_db_target_accounts` main
            
            JOIN 
                `syniti_mysql.syniti_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            -- JOIN 
            --     `syniti.db_6sense_account_current_state` extra
            
            -- USING(
            --     _6sensecompanyname,
            --     _6sensecountry,
            --     _6sensedomain
            -- )

            -- WHERE 
            --     extra._6qa_date IS NOT NULL

        )
        GROUP BY 
            1

    )

    USING(_campaign_id)

),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (

    SELECT
    
        *,
        
        COUNT(*) OVER (
            PARTITION BY _campaign_id
        ) 
        AS _occurrence

    FROM 
        campaign_numbers

),

-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (

    SELECT

        *,
        _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
        _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
        _target_accounts / _occurrence AS _reduced_target_accounts,
        _reached_accounts / _occurrence AS _reduced_reached_accounts,
        _6qa_accounts / _occurrence AS _reduced_6qa_accounts

    FROM 
        total_ad_occurrence_per_campaign

)

SELECT * FROM reduced_campaign_numbers;



    