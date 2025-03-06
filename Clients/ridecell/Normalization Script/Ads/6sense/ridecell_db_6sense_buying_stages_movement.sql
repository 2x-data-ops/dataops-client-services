TRUNCATE TABLE `x-marketing.ridecell.db_6sense_buying_stages_movement`;

INSERT INTO `x-marketing.ridecell.db_6sense_buying_stages_movement` (
    _activities_on,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _country_account,
    _data_source,
    _previous_stage,
    _current_stage,
    _previous_stage_order,
    _current_stage_order,
    _movement
)

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
        SELECT DISTINCT
            CASE
                WHEN buying_stage._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', buying_stage._extractdate)
                ELSE PARSE_DATE('%F', buying_stage._extractdate)
            END AS _activities_on,
            master_list._sfdcaccountname AS _6sensecompanyname,
            master_list._sfdcbillingcountry AS _6sensecountry, 
            -- sf_account_18_id AS sf_account_id,
            master_list._sfdcwebsite AS _6sensedomain,
            CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account,
            '6sense' AS _data_source,
            buying_stage._buyingstagestart AS _previous_stage,
            buying_stage._buyingstageend AS _current_stage
        FROM `x-marketing.ridecell_mysql.ridecell_db_buying_stage` buying_stage
        JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
            ON buying_stage._6sensecompanyname = master_list._6sensename
            AND buying_stage._6sensecountry = master_list._6sensecountry
            AND buying_stage._6sensedomain = master_list._6sensedomain
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite
            ORDER BY CASE
                WHEN buying_stage._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', buying_stage._extractdate)
                ELSE PARSE_DATE('%F', buying_stage._extractdate) 
            END DESC) = 1
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT
            main.*,
            prev._order AS _previous_stage_order,
            curr._order AS _current_stage_order,
            CASE
                WHEN curr._order > prev._order THEN '+ve'
                WHEN prev._order > curr._order THEN '-ve'
                ELSE 'Stagnant'
            END AS _movement
        FROM sixsense_buying_stage_data AS main
        LEFT JOIN sixsense_stage_order AS prev
            ON main._previous_stage = prev._buying_stage
        LEFT JOIN sixsense_stage_order AS curr
            ON main._current_stage = curr._buying_stage
    )
SELECT *
FROM latest_sixsense_buying_stage_with_order_and_movement;