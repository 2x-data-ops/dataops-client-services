CREATE OR REPLACE TABLE smartcom.db_6sense_reached_account AS 


-- SELECT
--   reached.*,
--   '' AS 
-- FROM  reached
-- airtable


WITH reached AS (
   SELECT * EXCEPT (_spend),
    CAST(REGEXP_REPLACE(_spend, r'\$', '') AS FLOAT64) AS _spend
   FROM
  smartcomm_mysql.smartcommunications_db_reached_account
),
airtable AS (
SELECT DISTINCT
  _campaignid,
  _campaignname,
  '' AS _campaigntype
FROM `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense` 
)
SELECT
  reached.*,
  airtable.* EXCEPT (_campaignid)
FROM reached
LEFT JOIN airtable
ON reached._campaignid = airtable._campaignid;



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
                PARTITION BY _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
                ORDER BY
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END DESC
            ) AS _rownum,
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _activities_on,
            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            CONCAT(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            ) AS _country_account,
            '6sense' AS _data_source,
            _buyingstagestart AS _previous_stage,
            _buyingstageend AS _current_stage
        FROM
            `smartcomm_mysql.smartcommunications_db_buying_stage`
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
                    _6sensecompanyname,
                    _6sensecountry,
                    _6sensedomain,
                    _industrylegacy AS _6senseindustry,
                    _6senseemployeerange,
                    _6senserevenuerange,
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END AS _added_on,
                    '6sense' AS _data_source,

                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                FROM
                    `smartcomm_mysql.smartcommunications_db_target_account_6sense` 
            ) main

                            -- Get the earliest date of appearance of each account
                JOIN (

                    SELECT DISTINCT 

                        MIN(
                            CASE 
                                WHEN _extractdate LIKE '%/%'
                                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                                ELSE PARSE_DATE('%F', _extractdate)
                            END 
                        ) 
                        AS _added_on,

                        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                        
                    FROM
                        `smartcomm_mysql.smartcommunications_db_target_account_6sense`
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
                            WHEN _extractdate LIKE '%/%'
                            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                            ELSE PARSE_DATE('%F', _latestimpression)
                        END
                    ) OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                    ) AS _first_impressions,
                    CASE
                        WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
                        ELSE _websiteengagement
                    END AS _websiteengagement,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                        ORDER BY
                            CASE
                                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                                ELSE PARSE_DATE('%F', _latestimpression)
                            END DESC
                    ) AS _rownum,
                    CONCAT(
                        _6sensecompanyname,
                        _6sensecountry,
                        _6sensedomain
                    ) AS _country_account
        FROM
            `smartcomm_mysql.smartcommunications_db_reached_account`
        WHERE 
            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid
                FROM 
                    `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
                WHERE 
                    _campaignid != ''

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
            _campaignid,
            _name AS _advariation,
            _6senseid AS _adid,
            _accountctr,
            _accountvtr,
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
        FROM `smartcomm_mysql.smartcommunications_db_campaign_performance`
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

            _campaignid,
            _accountctr,
            _accountvtr,
            
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
            `smartcomm_mysql.smartcommunications_db_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),

airtable_fields AS (

    SELECT DISTINCT 

        _campaignid, 
        _adid,
        _adgroup,
        _screenshot
        
    FROM
        `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
    WHERE 
        _campaignid != ''
),
combined_data AS (

    SELECT

        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        ads.*,
        airtable_fields._adgroup,
        airtable_fields._screenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts

    FROM 
        ads

    LEFT JOIN
        airtable_fields 
    ON (
            ads._adid = airtable_fields._adid
        AND 
            ads._campaignid = airtable_fields._campaignid
    )
    OR (
            airtable_fields._adid IS NULL
        AND 
            ads._campaignid = airtable_fields._campaignid
    )

    LEFT JOIN 
        campaign_fields
    ON 
        ads._campaignid = campaign_fields._campaignid

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

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `smartcomm_mysql.smartcommunications_db_target_account_6sense` main
            
            JOIN 
                `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

        )
        GROUP BY 
            1

    ) target

    USING(_campaignid)

    -- Get accounts that have been reached
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _reached_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `smartcomm_mysql.smartcommunications_db_target_account_6sense` main
            
            JOIN 
                `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `smartcomm_mysql.smartcommunications_db_reached_account` extra

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

    USING(_campaignid)

    -- Get accounts that are 6QA
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _6qa_accounts
        
        FROM (
            
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,

            FROM 
                `smartcomm_mysql.smartcommunications_db_target_account_6sense` main
            
            JOIN 
                `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `smartcom.db_6sense_account_current_state` extra
            
            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            )

            WHERE 
                extra._6qa_date IS NOT NULL

        )
        GROUP BY 
            1

    )

    USING(_campaignid)

),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (

    SELECT
    
        *,
        
        COUNT(*) OVER (
            PARTITION BY _campaignid
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


-- Insert Linkedin data into ad performance
INSERT INTO `smartcom.db_6sense_ad_performance` (
_adid,
_date,
_spend,
_clicks,
_impressions,
_campaign_type,
_campaignid,
_campaign_name,
_campaign_status,
_accountctr,
_accountvtr,
_start_date,
_end_date,
_advariation,
_adgroup,
_newly_engaged_accounts,
_increased_engagement_accounts,
_target_accounts,
_reached_accounts,
_6qa_accounts,
_occurrence,
_reduced_newly_engaged_accounts,
_reduced_increased_engagement_accounts,
_reduced_target_accounts,
_reduced_reached_accounts,
_reduced_6qa_accounts
)
WITH
linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS DATE) AS _date,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions,
    'Linkedin' AS _campaign_type
  FROM
    `smartcomm_linkedin_ads.ad_analytics_by_creative`
  GROUP BY creative_id, start_at
),
creative AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    CAST(campaign_id AS STRING) AS _campaign_id
  FROM `smartcomm_linkedin_ads.creatives`
),
campaign AS (
  SELECT
    name AS _campaign_name,
    CAST(id AS STRING) AS id,
    '' AS _campaign_status,
    CAST(NULL AS STRING) AS _accountctr,
    CAST(NULL AS STRING) AS _accountvtr,
    CAST(NULL AS DATE) AS _start_date,
    CAST(NULL AS DATE) AS _end_date,
    '' AS _advariation,
    '' AS _ad_group,
    CAST(NULL AS INT64) AS _newly_engaged_accounts,
    CAST(NULL AS INT64) AS _increased_engagement_accounts,
    CAST(NULL AS INT64) AS _target_accounts,
    CAST(NULL AS INT64) AS _reached_accounts,
    CAST(NULL AS INT64) AS _6qa_accounts,
    CAST(NULL AS INT64) AS _occurrence,
    CAST(NULL AS INT64) AS _reduced_newly_engaged_accounts,
    CAST(NULL AS INT64) AS _reduced_increased_engagement_accounts,
    CAST(NULL AS INT64) AS _reduced_target_accounts,
    CAST(NULL AS INT64) AS _reduced_reached_accounts,
    CAST(NULL AS INT64) AS _reduced_6qa_accounts
  FROM `smartcomm_linkedin_ads.campaigns`
)
SELECT
  linkedin_ads.*,
  creative.* EXCEPT(cID),
  campaign.* EXCEPT (id)
FROM linkedin_ads
LEFT JOIN creative
ON linkedin_ads._adid = creative.cID
LEFT JOIN campaign
ON campaign.id = creative._campaign_id;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------




CREATE OR REPLACE TABLE `smartcom.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        -- CASE
        --     WHEN main.segment_name = 'S4_HANA - NA' THEN 'S4/HANA - NA'
        --     WHEN main.segment_name = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
        --     WHEN main.segment_name = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
        --     ELSE main.segment_name
        -- END AS _segmentname,              
        side._segment,
        side._campaignid,
        side._campaignname
    FROM 
        `smartcomm_mysql.smartcommunications_db_target_account_6sense` main
    
    JOIN 
        `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense` side
    
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
            WHEN SAFE_CAST(side._clicks AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_clicks,

        CASE 
            WHEN SAFE_CAST(side._impressions AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_impressions

    FROM 
        target_accounts AS main

    LEFT JOIN
    
        `smartcomm_mysql.smartcommunications_db_reached_account` side

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts;