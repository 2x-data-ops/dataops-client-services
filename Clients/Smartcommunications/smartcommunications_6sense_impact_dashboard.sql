-- SCRIPT CONTEXT AND OVERVIEW
-- Analyst needs to build ads performance pages only for the 6sense impact dashboard
-- Currently only ads performance and account performance table is connected in the dashboard






CREATE OR REPLACE TABLE `smartcom.db_6sense_buying_stages_movement` AS

WITH sixsense_stage_order AS (
        SELECT
            'Target' AS _buying_stage,
            1 AS _order
        UNION ALL
        SELECT
            'Awareness' AS _buying_stage,
            2 AS _order
        UNION ALL
        SELECT
            'Consideration' AS _buying_stage,
            3 AS _order
        UNION ALL
        SELECT
            'Decision' AS _buying_stage,
            4 AS _order
        UNION ALL
        SELECT
            'Purchase' AS _buying_stage,
            5 AS _order
    ),
    sixsense_buying_stage_data AS (
        SELECT
            DISTINCT ROW_NUMBER() OVER (
                PARTITION BY _6sense_company_name,
                _6sense_country,
                _6sense_domain
                ORDER BY
                    CASE
                        WHEN extract_date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', extract_date)
                        ELSE PARSE_DATE('%F', extract_date)
                    END DESC
            ) AS _rownum,
            CASE
                WHEN extract_date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', extract_date)
                ELSE PARSE_DATE('%F', extract_date)
            END AS _activities_on,
            _6sense_company_name,
            _6sense_country,
            _6sense_domain,
            CONCAT(
                _6sense_company_name,
                _6sense_country,
                _6sense_domain
            ) AS _country_account,
            '6sense' AS _data_source,
            buying_stage__start AS _previous_stage,
            buying_stage__end AS _current_stage
        FROM
            `smartcom_6sense.buying_stage`
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT main.*
            EXCEPT (_rownum),
            prev._order AS _previous_stage_order,
            curr._order AS _current_stage_order,
                CASE
                    WHEN curr._order > prev._order THEN '+ve'
                    WHEN prev._order > curr._order THEN '-ve'
                    ELSE 'Stagnant'
                END AS _movement
        FROM
            sixsense_buying_stage_data AS main
            LEFT JOIN sixsense_stage_order AS prev
                ON main._previous_stage = prev._buying_stage
            LEFT JOIN sixsense_stage_order AS curr
                ON main._current_stage = curr._buying_stage
        WHERE main._rownum = 1
    )
SELECT *
FROM
    latest_sixsense_buying_stage_with_order_and_movement;


--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------- ACCOUNT CURRENT STATE
--------------------------------------------------------------------------
--------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcom.db_6sense_account_current_state` AS

WITH target_accounts AS (
        SELECT DISTINCT main.*

        FROM (
                SELECT DISTINCT
                    _6sense_company_name,
                    _6sense_country,
                    _6sense_domain,
                    industry__legacy_ AS _6senseindustry,
                    _6sense_employee_range,
                    _6sense_revenue_range,
                    CASE
                        WHEN extract_date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', extract_date)
                        ELSE PARSE_DATE('%F', extract_date)
                    END AS _added_on,
                    '6sense' AS _data_source,

                    CONCAT(_6sense_company_name, _6sense_country, _6sense_domain) AS _country_account
                FROM
                    `smartcom_6sense.target_account` 
            ) main

                            -- Get the earliest date of appearance of each account
                JOIN (

                    SELECT DISTINCT 

                        MIN(
                            CASE 
                                WHEN extract_date LIKE '%/%'
                                THEN PARSE_DATE('%m/%e/%Y', extract_date)
                                ELSE PARSE_DATE('%F', extract_date)
                            END 
                        ) 
                        AS _added_on,

                        CONCAT(_6sense_company_name, _6sense_country, _6sense_domain) AS _country_account
                        
                    FROM
                        `smartcom_6sense.target_account`
                    GROUP BY 
                        2
                    ORDER BY 
                        1 DESC

                ) scenario 

                ON 
                    main._country_account = scenario._country_account 
                AND 
                    main._added_on = scenario._added_on

    ),
    reached_related_info AS (
        SELECT *
        EXCEPT (_rownum)
        FROM (
                SELECT
                    DISTINCT MIN(
                        CASE
                            WHEN extract_date LIKE '%/%'
                            THEN PARSE_DATE('%m/%e/%Y', latest_impression)
                            ELSE PARSE_DATE('%F', latest_impression)
                        END
                    ) OVER (
                        PARTITION BY CONCAT(
                            _6sense_company_name,
                            _6sense_country,
                            _6sense_domain
                        )
                    ) AS _first_impressions,
                    CASE
                        WHEN website_engagement = '-' THEN CAST(NULL AS STRING)
                        ELSE website_engagement
                    END AS _website_engagement,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            _6sense_company_name,
                            _6sense_country,
                            _6sense_domain
                        )
                        ORDER BY
                            CASE
                                WHEN extract_date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', latest_impression)
                                ELSE PARSE_DATE('%F', latest_impression)
                            END DESC
                    ) AS _rownum,
                    CONCAT(
                        _6sense_company_name,
                        _6sense_country,
                        _6sense_domain
                    ) AS _country_account
        FROM
            `smartcom_6sense.reached_account`
        WHERE 
            CAST(campaign_id AS STRING) IN (

                SELECT DISTINCT 
                    CAST(campaign_id__nu AS STRING) AS _campaignid
                FROM 
                    `smartcom_6sense.airtable`
                WHERE 
                    CAST(campaign_id__nu AS STRING) != ''

            )
        )
    WHERE _rownum = 1
),

six_qa_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _6qa_date,

            true _is_6qa,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account

        FROM 
            `smartcomm_mysql.smartcommunications_db_6qa_account`
    
    )

    WHERE 
        _rownum = 1

),

-- Get buying stage info for each account

buying_stage_related_info AS (

    SELECT DISTINCT 
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC 
            ) 
            AS rownum

        FROM
            `smartcom.db_6sense_buying_stages_movement`

    )
    WHERE 
        rownum = 1

),

-- Attach all other data parts to target accounts
combined_data AS (

    SELECT DISTINCT 

        target.*, 
        reached.* EXCEPT(_country_account),
        six_qa.* EXCEPT(_country_account),
        stage.* EXCEPT(_country_account)   

    FROM
        target_accounts AS target

    LEFT JOIN
        reached_related_info AS reached 
    USING(
        _country_account
    )

    LEFT JOIN
        six_qa_related_info AS six_qa 
    USING(
        _country_account
    ) 

    LEFT JOIN
        buying_stage_related_info AS stage
    USING(
        _country_account
    ) 

)

SELECT * FROM combined_data;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ADS PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcom.db_6sense_ad_performance` AS

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
            CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
            CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
            CASE 
                        WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
                        WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
                    END AS _date,
            ROW_NUMBER() OVER (
                    PARTITION BY _campaignid,
                    _6senseid,
                    _date
                    ORDER BY CASE 
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
                    END
                ) AS _rownum
        FROM `x-marketing.smartcomm_mysql.smartcommunications_db_campaign_performance`
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
                ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
            END 
            AS _newly_engaged_accounts,

            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-'
                THEN 0
                ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
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
            `x-marketing.smartcomm_mysql.smartcommunications_db_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),

airtable_fields AS (

    SELECT DISTINCT 

        CAST(campaign_id__nu AS STRING) AS _campaign_id, 
        -- _adid AS _ad_id,
        ad_group AS _ad_group,
        -- screenshot AS _screenshot
        
    FROM
        `x-marketing.smartcom_6sense.airtable`
    WHERE 
        CAST(campaign_id__nu AS STRING) != ''
),
combined_data AS (

    SELECT

        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        ads.*,
        airtable_fields._ad_group,
        -- airtable_fields._screenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts

    FROM 
        ads

    LEFT JOIN
        airtable_fields 
    ON (
        --     ads._adid = airtable_fields._ad_id
        -- AND 
            ads._campaign_id = airtable_fields._campaign_id
    )
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

            _campaignid,
            COUNT(*) AS _target_accounts

        FROM (

            SELECT DISTINCT 

                main._6sense_company_name,
                main._6sense_country,
                main._6sense_domain,
                main.segment_name AS _segmentname,
                side.campaign_id__nu AS _campaignid

            FROM 
                `x-marketing.smartcom_6sense.target_account` main
            
            JOIN 
                `x-marketing.smartcom_6sense.airtable` side
            
            ON 
                main.segment_name = side.segment_name

        )
        GROUP BY 
            1

    ) target

    ON combined_data._campaign_id = CAST(target._campaignid AS STRING)
    -- USING(_campaignid)

    -- Get accounts that have been reached
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _reached_accounts

        FROM (

            SELECT DISTINCT 

                main._6sense_company_name,
                main._6sense_country,
                main._6sense_domain,
                main.segment_name AS _segmentname,
                side.campaign_id__nu AS _campaignid

            FROM 
                `x-marketing.smartcom_6sense.target_account` main
            
            JOIN 
                `x-marketing.smartcom_6sense.airtable` side
            
            ON 
                main.segment_name = side.segment_name

            JOIN 
                `x-marketing.smartcom_6sense.reached_account` extra

            ON main._6sense_company_name = extra._6sense_company_name
            AND main._6sense_country = extra._6sense_country
            AND main._6sense_domain = extra._6sense_domain
            AND side.campaign_id__nu = extra.campaign_id

            -- USING(
            --     _6sense_company_name,
            --     _6sense_country,
            --     _6sense_domain,
            --     _campaign_id
            -- )
            
        )
        GROUP BY 
            1

    ) reach

    ON combined_data._campaign_id = CAST(target._campaignid AS STRING)
    -- USING(_campaign_id)

    -- Get accounts that are 6QA
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid AS _campaign_id,
            COUNT(*) AS _6qa_accounts
        
        FROM (
            
            SELECT DISTINCT 
                main._6sense_company_name,
                main._6sense_country,
                main._6sense_domain,
                main.segment_name AS _segmentname,
                side.campaign_id AS _campaignid,

            FROM 
                `x-marketing.smartcom_6sense.target_account` main
            
            JOIN 
                `x-marketing.smartcom_6sense.airtable` side
            
            ON 
                main.segment_name = side.segment_name

            JOIN 
                `smartcom.db_6sense_account_current_state` extra
            
            USING(
                _6sense_company_name,
                _6sense_country,
                _6sense_domain
            )

            WHERE 
                extra._6qa_date IS NOT NULL

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

SELECT * EXCEPT (_campaignid) FROM reduced_campaign_numbers;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------





CREATE OR REPLACE TABLE `smartcom.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sense_company_name,
        main._6sense_country,
        main._6sense_domain,
        -- CASE
        --     WHEN main.segment_name = 'S4_HANA - NA' THEN 'S4/HANA - NA'
        --     WHEN main.segment_name = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
        --     WHEN main.segment_name = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
        --     ELSE main.segment_name
        -- END AS _segmentname,              
        side.segment_name,
        side.campaign_id__nu AS _campaignid,
        CAST(side.campaign_id__nu AS STRING) AS campaign_id,
        side.campaign_name AS _campaign_name

    FROM 
        `x-marketing.smartcom_6sense.target_account` main
    
    JOIN 
        `x-marketing.smartcom_6sense.airtable` side
    
    ON 
        main.segment_name = side.segment_name

),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (

    SELECT DISTINCT 

        main.* EXCEPT(campaign_id),

        CASE 
            WHEN side.campaign_id IS NOT NULL 
            THEN true 
        END 
        AS _is_reached,

        CASE 
            WHEN CAST(side.clicks AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_clicks,

        CASE 
            WHEN CAST(side.impressions AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_impressions

    FROM 
        target_accounts AS main

    LEFT JOIN (
        SELECT * EXCEPT (campaign_id), CAST(campaign_id AS STRING) AS campaign_id
        FROM
    
        `x-marketing.smartcom_6sense.reached_account` ) side

    USING(
        _6sense_company_name,
        _6sense_country,
        _6sense_domain,
        campaign_id
    )

)

SELECT * FROM reached_accounts;
