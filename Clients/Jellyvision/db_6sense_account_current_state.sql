-- 6sense Account Current State

CREATE OR REPLACE TABLE `jellyvision.db_6sense_account_current_state` AS
WITH target_accounts AS (

    SELECT DISTINCT 
        main.*
    FROM (
    
        SELECT DISTINCT 

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            SPLIT(_6sensedomain, '.')[SAFE_OFFSET(0)] AS _domain,
            _industrylegacy AS _6senseindustry,
            _6senseemployeerange,
            _6senserevenuerange,

            CASE
                WHEN _extractdate LIKE '0%' 
                THEN PARSE_DATE('%m/%d/%y', _extractdate)
                ELSE PARSE_DATE('%m/%d/%Y', _extractdate) 
            END 
            AS _added_on,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
           `x-marketing.jellyvision_mysql.jellyvision_db_segment_target_account`

        WHERE
            LENGTH(_extractdate) > 0


    ) main

    -- Get the earliest date of appearance of each account
    JOIN (

        SELECT DISTINCT 

            MIN(
                CASE
                    WHEN _extractdate LIKE '0%' 
                    THEN PARSE_DATE('%m/%d/%y', _extractdate)
                    ELSE PARSE_DATE('%m/%d/%Y', _extractdate) 
                END 
            ) 
            AS _added_on,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
        FROM
           `x-marketing.jellyvision_mysql.jellyvision_db_segment_target_account`

        WHERE 
            LENGTH(_extractdate) > 0

        GROUP BY 
            2
        ORDER BY 
            1 DESC

    ) scenario 

    ON 
        main._country_account = scenario._country_account 
    AND 
        main._added_on = scenario._added_on
    
)
-- Get date when account had first impression
, reached_related_info AS (

    SELECT
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            MIN(
                CASE
                    WHEN _latestimpression LIKE '0%' 
                    THEN PARSE_DATE('%m/%d/%y', _latestimpression)
                    ELSE PARSE_DATE('%m/%d/%Y', _latestimpression) 
                END 
            )
            OVER(
                PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname) 
            )
            AS _first_impressions,

            CASE
                WHEN _websiteengagement = '-'
                THEN CAST(NULL AS STRING)
                ELSE _websiteengagement
            END 
            AS _website_engagement,

            ROW_NUMBER() OVER(
                PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname) 
                ORDER BY (
                    CASE
                        WHEN _latestimpression LIKE '0%' 
                        THEN PARSE_DATE('%m/%d/%y', _latestimpression)
                        ELSE PARSE_DATE('%m/%d/%Y', _latestimpression) 
                    END 
                ) DESC
            )
            AS rownum,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `x-marketing.jellyvision_mysql.jellyvision_db_campaign_reached_accounts`

        WHERE 

            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid 
                FROM 
                  `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_6sense`
                WHERE 
                    _campaignid != ''

            )

    )
    WHERE 
        rownum = 1

)

-- Get the date when account first became a 6QA
, six_qa_related_info AS (

    SELECT DISTINCT 
        
        MIN(side._6qa_date) AS _6qa_date,
        side._is_6qa,
        _6sensescore,
        main._country_account

    FROM 
        target_accounts AS main
    
    -- This gets all possible 6QA dates for each account
    JOIN (

        SELECT DISTINCT

            DATE(account6qastartdate6sense__c) AS _6qa_date,
            account6qa6sense__c AS _is_6qa,
            accountprofilescore6sense__c AS _6sensescore,
            act.name AS _account_name,
            web_domain_name__c  AS _domain,
            --COALESCE(act.shippingcountry, act.billingcountry) AS _country

        FROM 
            `jellyvision_salesforce.Account` act
        WHERE isdeleted IS FALSE

        
    ) side

    -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
   ON 
         (  side._domain =  SPLIT(main._6sensedomain, '.')[SAFE_OFFSET(0)] 
        AND
            (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL) 
        -- AND
        --     (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL)
        -- AND
        --     main._6sensecompanyname = side._account_name
        -- AND
        --     main._6sensecountry = side._country
         )        
--    OR (
--             side._domain NOT LIKE CONCAT('%', SPLIT(main._6sensedomain, '.')[SAFE_OFFSET(0)], '%')
--         AND 
--             main._6sensecompanyname = side._account_name

--     ) 


    GROUP BY 
        2, 3, 4

)
-- Get the buying stage info each account
, buying_stage_related_info AS (

    SELECT DISTINCT 
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            _prev_stage,
            _prev_order,
            _current_stage,
            _curr_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC 
            ) 
            AS rownum

        FROM
            `jellyvision.db_6sense_buying_stages_movement`

    )
    WHERE 
        rownum = 1

)
-- Attach all other data parts to target accounts

, combined_data AS (

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

, account_lookup AS (
     SELECT 
    _crmaccountid, 
    _crmdomain, 
    _crmaccount, 
    _6sensedomain, 
    _6senseaccount 
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_6sense_lookup_table` 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _crmaccountid,_6sensedomain ORDER BY _crmaccountid) = 1
)

SELECT combined_data.* ,
account_lookup.* EXCEPT(_6sensedomain, _6senseaccount )
FROM combined_data
LEFT JOIN account_lookup ON CONCAT(LOWER(combined_data._6sensecompanyname),combined_data._6sensedomain) = CONCAT(LOWER(_6senseaccount),account_lookup._6sensedomain);