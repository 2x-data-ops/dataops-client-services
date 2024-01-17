CREATE
OR
REPLACE
TABLE
    `syniti.db_6sense_buying_stages_movement` AS
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
            ) AS _account_key,
            '6sense' AS _data_source,
            _buyingstagestart AS _previous_stage,
            _buyingstageend AS _current_stage
        FROM
            `syniti_mysql.syniti_db_accounts_buying_stage`
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT main.*
        EXCEPT (_rownum), prev._order AS _previous_stage_order, curr._order AS _current_stage_order, CASE
            WHEN curr._order > prev._order THEN '+ve'
            WHEN prev._order > curr._order THEN '-ve'
            ELSE 'Stagnant'
        END AS _movement
        FROM
            sixsense_buying_stage_data AS main
            LEFT JOIN sixsense_stage_order AS prev ON main._previous_stage = prev._buying_stage
            LEFT JOIN sixsense_stage_order AS curr ON main._current_stage = curr._buying_stage
        WHERE main._rownum = 1
    )
SELECT *
FROM
    latest_sixsense_buying_stage_with_order_and_movement;

---6sense Account Current State
CREATE
OR
REPLACE
TABLE
    'syniti.db_6sense_account_current_state' AS
WITH target_accounts AS (
        SELECT *
        EXCEPT (_rownum)
        FROM (
                SELECT
                    DISTINCT _6sensecompanyname,
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
                    CONCAT(
                        _6sensecompanyname,
                        _6sensecountry,
                        _6sensedomain
                    ) AS _account_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY _6sensecompanyname,
                        _6sensecountry,
                        _6sensedomain
                        ORDER BY
                            CASE
                                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                                ELSE PARSE_DATE('%F', _extractdate)
                            END
                    ) AS _rownum
                FROM
                    `syniti_mysql.db_target_account`
            )
        WHERE
            _rownum = 1
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
                    ) AS _account_key
        FROM
            `syniti_mysql.db_campaign_reached_account`
        )
    WHERE _rownum = 1
),

-- Get date when account first become 6QA
-- later

-- Get buying stage info for each account

buying_stage_related_info AS (
    SELECT DISTINCT
        _previous_stage,
        _current_stage,
        _current_stage_rder,
        _movement,
        _activities_on SA _movement_date,
        _account_key
    FROM `syniti.db_6sense_buying_stages_movement`
    WHERE
        _data_source = '6sense'
),

-- Attach all other data parts to target accounts
