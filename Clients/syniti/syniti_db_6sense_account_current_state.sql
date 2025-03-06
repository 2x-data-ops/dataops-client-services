CREATE
OR
REPLACE
TABLE
    `syniti.db_6sense_account_current_state` AS
WITH target_accounts AS (
        SELECT DISTINCT *

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

                    CONCAT(_6sensecompanyname, _6sensecountry) AS _country_account
                FROM
                    `syniti_mysql.syniti_db_target_accounts` 
            )

    ),
    reached_related_info AS (
        SELECT *
        EXCEPT (_rownum)
        FROM (
                SELECT
                    DISTINCT MIN(
                        CASE
                            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                            ELSE PARSE_DATE('%F', _latestimpression)
                        END
                    ) OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry
                        )
                    ) AS _first_impressions,
                    CASE
                        WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
                        ELSE _websiteengagement
                    END AS _websiteengagement,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry
                        )
                        ORDER BY
                            CASE
                                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                                ELSE PARSE_DATE('%F', _latestimpression)
                            END DESC
                    ) AS _rownum,
                    CONCAT(
                        _6sensecompanyname,
                        _6sensecountry
                    ) AS _country_account
        FROM
            `syniti_mysql.syniti_db_campaign_reached_accounts`
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
                    CONCAT(_6sensecompanyname, _6sensecountry) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry) AS _country_account

        FROM 
            `syniti_mysql.syniti_db_6qa_accounts_list`
    
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
            `syniti.db_6sense_buying_stages_movement`

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