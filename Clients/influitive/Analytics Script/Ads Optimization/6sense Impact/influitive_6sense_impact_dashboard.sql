
-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `influitive.db_6sense_buying_stages_movement` AS

-- Set buying stages and their order
WITH stage_order AS (

    SELECT 'Target' AS _buying_stage, 1 AS _order 
    UNION ALL
    SELECT 'Awareness' AS _buying_stage, 2 
    UNION ALL
    SELECT 'Consideration' AS _buying_stage, 3 
    UNION ALL
    SELECT 'Decision' AS _buying_stage, 4 
    UNION ALL
    SELECT 'Purchase' AS _buying_stage, 5

),

-- Get buying stage data
buying_stage_data AS (

    SELECT DISTINCT

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _buyingstageend AS _buying_stage,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _activities_on
        
    FROM 
        `influitive_mysql.influitive_db_accounts_initial_buying_stage`

),

-- Get first ever buying stage for each account
first_ever_buying_stage AS (

    SELECT DISTINCT 

        _activities_on,
        _6sensecountry,
        _6sensedomain, 
        _6sensecompanyname,
        _buying_stage,
        'Initial' AS _buying_stage_source,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

    FROM
        buying_stage_data

    JOIN (

        SELECT DISTINCT 

            _6sensecountry, 
            _6sensecompanyname, 
            MIN(_activities_on) AS _activities_on 

        FROM 
            buying_stage_data 
        GROUP BY 
            1, 2

    ) 
    USING(
        _6sensecountry, 
        _6sensecompanyname, 
        _activities_on
    )

    ORDER BY
        1 DESC

),

-- Get every other buying stage for each account
every_other_buying_stage AS (

    SELECT 
        * 
    FROM (

        SELECT DISTINCT 

            _activities_on,
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage,
            'Non Initial' AS _buying_stage_source,
            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM
            buying_stage_data
            
    )

    -- Exclude those that are first ever stages
    WHERE 
        CONCAT(_country_account, _activities_on) NOT IN (

            SELECT DISTINCT 
                CONCAT(_country_account, MIN(_activities_on)) 
            FROM 
                first_ever_buying_stage 
            GROUP BY 
                _country_account
        
        )

),

-- Combine both first ever data and every other data
historical_buying_stage AS (

    SELECT * FROM first_ever_buying_stage 

    UNION DISTINCT

    SELECT * FROM every_other_buying_stage

),

-- Get the current stage and previous stage for each historical record of an account
set_buying_stage_order AS (

    SELECT DISTINCT 

        main.* EXCEPT(_current_stage, _prev_stage),
        main._current_stage,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                )
            ) 
            AND _prev_stage IS NULL, 
            _current_stage, 
            _prev_stage  
        ) 
        AS _prev_stage,

        curr._order AS _curr_order,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                ) 
            )
            AND _prev_stage IS NULL, 
            curr._order, 
            prev._order  
        ) 
        AS _prev_order

    FROM (

        SELECT DISTINCT 

            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage AS _current_stage,
            _activities_on,

            LAG(_buying_stage) OVER(
                PARTITION BY _6sensedomain 
                ORDER BY _activities_on ASC
            ) AS _prev_stage,

            _buying_stage_source,
            _country_account

        FROM 
            historical_buying_stage

    ) main

    LEFT JOIN
        stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage

    LEFT JOIN
        stage_order AS prev 
    ON 
        main._prev_stage = prev._buying_stage

),

-- Set movement of each historical record an account
set_movement AS (

    SELECT 
        * EXCEPT(_order) 
    FROM (

        SELECT DISTINCT 
            *,

            IF(
                _curr_order > _prev_order, 
                "+ve", 
                IF(
                    _curr_order < _prev_order, 
                    "-ve", 
                    "Stagnant"
                )
            ) 
            AS _movement,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC
            ) 
            AS _order

        FROM
            set_buying_stage_order
        ORDER BY 
            _activities_on DESC

    )
    WHERE
        _order = 1
    ORDER BY
        _country_account

)

SELECT * FROM set_movement;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


-- 6sense Account Current State

CREATE OR REPLACE TABLE `influitive.db_6sense_account_current_state` AS

-- Get all target accounts and their segments
WITH target_accounts AS (

    SELECT DISTINCT 
        main.*
    FROM (
    
        SELECT DISTINCT 

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            _industrylegacy AS _6senseindustry,
            _6senseemployeerange,
            _6senserevenuerange,


            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _added_on,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `influitive_mysql.influitive_db_target_accounts`

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

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
            
        FROM
            `influitive_mysql.influitive_db_target_accounts`
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

-- Get date when account had first impression
reached_related_info AS (

    SELECT
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            MIN(
                CASE 
                    WHEN _extractdate LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                    ELSE PARSE_DATE('%F', _latestimpression)
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
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                        ELSE PARSE_DATE('%F', _latestimpression)
                    END
                ) DESC
            )
            AS rownum,

            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account

        FROM 
            `influitive_mysql.influitive_db_campaign_reached_accounts`

        WHERE 
            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid 
                FROM 
                    `influitive_mysql.influitive_optimization_airtable_ads`
                WHERE 
                    _campaignid != ''

            )

    )
    WHERE 
        rownum = 1

),

-- Get the date when account first became a 6QA
six_qa_related_info AS (

    SELECT DISTINCT 
        
        MIN(side._6qa_date) AS _6qa_date,
        side._is_6qa,
        main._country_account

    FROM 
        target_accounts AS main

    JOIN (

        SELECT DISTINCT

            DATE(account6qastartdate6sense__c) AS _6qa_date,
            true AS _is_6qa,
            name AS _account_name,
            website AS _domain,
            COALESCE(shippingcountry, billingcountry) AS _country

        FROM 
            `influitive_salesforce.Account`
        WHERE 
            isdeleted = false
        AND 
            accountupdatedate6sense__c IS NOT NULL

    ) side

    -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
    ON (
            side._domain LIKE CONCAT('%', main._6sensedomain, '%')
        AND
            (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL)
        AND
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    )
        
    OR (
            side._domain NOT LIKE CONCAT('%', main._6sensedomain, '%')
        AND 
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    ) 

    GROUP BY 
        2, 3

),

-- Get the buying stage info each account
buying_stage_related_info AS (

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
            `influitive.db_6sense_buying_stages_movement`

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


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- 6sense Engagement Log

CREATE OR REPLACE TABLE `influitive.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (

    SELECT * FROM `influitive.db_6sense_account_current_state`

),

-- Prep the reached account data for use later
reached_accounts_data AS (

    SELECT DISTINCT

        CAST(main._clicks AS INTEGER) AS _clicks,
        CAST(main._influencedformfills AS INTEGER) AS _influencedformfills,

        CASE 
            WHEN main._latestimpression LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._latestimpression)
            ELSE PARSE_DATE('%F', main._latestimpression)
        END 
        AS _latestimpression, 

        CASE 
            WHEN main._extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
            ELSE PARSE_DATE('%F', main._extractdate)
        END 
        AS _activities_on, 

        main._campaignid AS _campaign_id,

        -- Need label to distingush 6sense and Linkedin campaigns
        side._campaigntype AS _campaign_type,
        side._campaignname AS _campaign_name,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    
    FROM 
        `influitive_mysql.influitive_db_campaign_reached_accounts` main
    
    JOIN (

        SELECT DISTINCT 

            _campaignid, 
            _campaignname,  
            IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
            
        FROM
            `influitive_mysql.influitive_optimization_airtable_ads`
        WHERE 
            _campaignid != ''

    ) side

    USING(_campaignid)

),


-- Get campaign reached engagements
campaign_reached AS (

    SELECT DISTINCT 
 
        _country_account, 
        CAST(NULL AS STRING) AS _email,

        MIN(_latestimpression) OVER(
            PARTITION BY 
                _country_account, 
                _campaign_name
            ORDER BY 
                _latestimpression
        ) 
        AS _timestamp,

        CONCAT(_campaign_type, ' ', 'Campaign Reached') AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaign_name AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data

),

-- Get ad clicks engagements
ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account,
            CAST(NULL AS STRING) AS _email,  
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' ', 'Ad Clicks') AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaign_name AS _description,  
            _clicks AS _notes,

            -- Get last period's clicks to compare
            LAG(_clicks) OVER(
                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on
            )
            AS _old_notes

        FROM
            reached_accounts_data 
        WHERE
            _clicks >= 1

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get form fills engagements
influenced_form_fills AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            _country_account, 
            CAST(NULL AS STRING) AS _email, 
            _activities_on AS _timestamp,
            CONCAT(_campaign_type, ' ', 'Influenced Form Filled') AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaign_name AS _description,  
            _influencedformfills AS _notes,

            -- Get last period's clicks to compare
            LAG(_influencedformfills) OVER(
                PARTITION BY 
                    _country_account, 
                    _campaign_name
                ORDER BY 
                    _activities_on
            )
            AS _old_notes

        FROM
            reached_accounts_data 
        WHERE
            _influencedformfills >= 1

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Prep the sales intelligence data for use later
sales_intelligence_data AS (

    SELECT 

        side._activitytype,
        side._activitytarget,
        side._activitymetainfo,
        side._contactname,
        side._email,

        CASE 
            WHEN _activitydate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _activitydate)
            ELSE PARSE_DATE('%F', _activitydate)
        END  
        AS _date,

        main._country_account,
        COUNT(*) AS _count

    FROM 
        target_accounts AS main

    JOIN (

        SELECT

            act.name AS _account_name,
            act.website AS _domain,
            COALESCE(act.billingcountry, act.shippingcountry) AS _country,
            sales.* EXCEPT(_accountname)
        
        FROM 
            `influitive_mysql.influitive_db_sales_intelligence_activities` sales
        
        LEFT JOIN 
            `influitive_salesforce.Account` act 
        ON 
            REPLACE(sales._crmaccountid, 'CMA', '') = act.id
        
        WHERE 
            act.accountupdatedate6sense__c IS NOT NULL
        
        AND 
            sales._sdc_deleted_at IS NULL
        AND
            act.isdeleted = false
        
    ) side

    -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
    ON (
            side._domain LIKE CONCAT('%', main._6sensedomain, '%')
        AND
            (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL)
        AND
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    )
        
    OR (
            side._domain NOT LIKE CONCAT('%', main._6sensedomain, '%')
        AND 
            main._6sensecompanyname = side._account_name
        AND
            main._6sensecountry = side._country
    ) 
        
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7

),

-- Get all the different types of engagements
sales_intelligence_engagements AS (

    SELECT DISTINCT 

        _country_account,
        _email, 
        _date AS _timestamp,
        
        CASE 
            WHEN 
                _activitytype LIKE '%Web Visit%'
            THEN 
                '6sense Web Visits'
            WHEN 
                _activitytype = 'KW Research'
            THEN 
                '6sense Searched Keywords'
            WHEN 
                _activitytype LIKE '%Page Click%'
            THEN 
                'Page Clicked'
            WHEN 
                _activitytype IN (
                    'Form Fill'/* ,
                    'Email Open',
                    'Email Click' */
                )
            THEN 
                CONCAT(_activitytype, 'ed')
            ELSE 
                _activitytype

        END 
        AS _engagement, 

        'Sales Intelligence' AS _engagement_data_source,
        
        CASE 
            WHEN 
                _activitytype IN (
                    'Web Visit (Anonymous)',
                    'KW Research',
                    'Bombora',
                    'Form Fill',
                    'Registered for Event',
                    'Web Visit (Known)',
                    'Page Click (Anonymous)',
                    'Attended Webinar',
                    'Attended Meeting',
                    'Attended Event'
                )
            THEN 
                _activitytarget
            WHEN 
                _activitytype IN (
                    -- 'Email Open', 
                    'TrustRadius',
                    'Unsubscribed'/* , 
                    'Email Click' */
                )  
            THEN 
                _activitymetainfo
        END 
        AS _description,
        
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype != ''
    
    -- Exclude email events from sales intelligence
    AND 
        _activitytype NOT LIKE '%Email%'

),

-- Get email engagements from Hubspot
hubspot_email_engagements AS (

    SELECT  

        CONCAT(_country, _company) AS _country_account,
        _email,
        DATE(_timestamp) AS _timestamp,
        CONCAT('Email ', _engagement) AS _engagement,
        'Hubspot' AS _engagement_data_source,
        _utmcampaign AS _description,
        1 AS _notes

    FROM 
        `influitive.db_email_engagements_log` 

    WHERE 
        _engagement IN('Opened', 'Clicked')

),

-- Only activities involving target accounts are considered
combined_data AS (

    SELECT DISTINCT 

        target_accounts.*,
        activities.* EXCEPT(_country_account)
        
    FROM (

        SELECT * FROM campaign_reached 
        UNION DISTINCT
        SELECT * FROM ad_clicks 
        UNION DISTINCT
        SELECT * FROM influenced_form_fills
        UNION DISTINCT
        SELECT * FROM sales_intelligence_engagements
        UNION DISTINCT
        SELECT * FROM hubspot_email_engagements
        
    ) activities

    JOIN
        target_accounts

    USING (_country_account)

)

SELECT * FROM combined_data;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


-- Opportunity Influenced + Accelerated

CREATE OR REPLACE TABLE `influitive.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (

    SELECT DISTINCT 

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        _6qa_date,
        _engagement, 
        ROW_NUMBER() OVER() AS _eng_id,
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,

        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END 
        AS _channel

    FROM 
        `influitive.db_6sense_engagement_log` 
    
    -- Get engagements after start of campaign
   -- WHERE
        --_timestamp >= '2023-10-19'

),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (

    SELECT DISTINCT 

        opp.accountid AS _account_id, 
        act.name AS _account_name,
        act.website AS _domain,
        
        COALESCE(
            act.shippingcountry, 
            act.billingcountry
        )
        AS _country,
        
        opp.id AS _opp_id,
        opp.name AS _opp_name,
        own.name AS _opp_owner_name,
        opp.type AS _opp_type,
        DATE(opp.createddate) AS _created_date,
        DATE(opp.closedate) AS _closed_date,
        opp.amount AS _amount,

        -- For filling up those opps with missing first stage in the opp history
        opp.stagename AS _current_stage,
        DATE(opp.laststagechangedate) AS _stage_change_date

    FROM 
        `influitive_salesforce.Opportunity` opp
    
    LEFT JOIN
        `influitive_salesforce.Account` act
    ON 
        opp.accountid = act.id 
    
    LEFT JOIN
        `influitive_salesforce.User` own
    ON 
        opp.ownerid = own.id 
    
    WHERE 
        opp.isdeleted = false
    AND 
        EXTRACT(YEAR FROM opp.createddate) >= 2023 

),

-- Get all historical stages of opp
-- Perform necessary cleaning of the data
opps_historical_stage AS (

    SELECT

        main.*,
        side._previous_stage_prob,
        side._next_stage_prob
    
    FROM (

        SELECT DISTINCT 
            
            opportunityid AS _opp_id,
            createddate AS _historical_stage_change_timestamp,
            DATE(createddate) AS _historical_stage_change_date,
            oldvalue AS _previous_stage,
            newvalue AS _next_stage

        FROM
            `influitive_salesforce.OpportunityFieldHistory` 
        WHERE
            field = 'StageName'
        AND 
            isdeleted = false

    ) main

    JOIN (

        SELECT DISTINCT 

            opportunityid AS _opp_id,
            createddate AS _historical_stage_change_timestamp,
            oldvalue__fl AS _previous_stage_prob,
            newvalue__fl AS _next_stage_prob,

        FROM
            `influitive_salesforce.OpportunityFieldHistory`
        WHERE
            field = 'Probability'
        AND 
            isdeleted = false

    ) side

    USING (
        _opp_id,
        _historical_stage_change_timestamp
    )

),

-- There are several stages that can occur on the same day
-- Get unique stage on each day 
unique_opps_historical_stage AS (

    SELECT
        * EXCEPT(_rownum),

        -- Setting the rank of the historical stage based on stage change date
        ROW_NUMBER() OVER(

            PARTITION BY  
                _opp_id
            ORDER BY 
                _historical_stage_change_date DESC

        )
        AS _stage_rank

    FROM (

        SELECT
            *,
            
            -- Those on same day are differentiated by timestamp
            ROW_NUMBER() OVER(

                PARTITION BY  
                    _opp_id,
                    _historical_stage_change_date
                ORDER BY 
                    _historical_stage_change_timestamp DESC

            )
            AS _rownum

        FROM 
            opps_historical_stage

    )
    WHERE
        _rownum = 1

),

-- Generate a log to store stage history from latest to earliest
get_aggregated_stage_history_text AS (

    SELECT
        *,

        STRING_AGG( 
            CONCAT(
                '[ ', _historical_stage_change_date, ' ]',
                ' : ', _next_stage
            ),
            '; '
        ) 
        OVER(
            
            PARTITION BY 
                _opp_id
            ORDER BY 
                _stage_rank

        )
        AS _stage_history

    FROM 
        unique_opps_historical_stage

),

-- Obtain the current stage and the stage date in this CTE 
get_current_stage_and_date AS (

    SELECT
        *,

        CASE 
            WHEN _stage_rank = 1 THEN _historical_stage_change_date
        END  
        AS _stage_change_date,

        CASE 
            WHEN _stage_rank = 1 THEN _next_stage
        END  
        AS _current_stage

    FROM 
        get_aggregated_stage_history_text

),

-- Add the stage related fields to the opps data
opps_history AS (

    SELECT

        -- Remove the current stage from the opp created CTE
        main.* EXCEPT(_current_stage, _stage_change_date),
        
        -- Fill the current stage and date for an opp
        -- Will be the same in each row of an opp
        -- If no stage and date, get the stage and date from the opp created CTE
        COALESCE(
            MAX(side._stage_change_date) OVER (PARTITION BY side._opp_id),
            main._stage_change_date,
            main._created_date
        )
        AS _stage_change_date,
        
        COALESCE(
            MAX(side._current_stage) OVER (PARTITION BY side._opp_id),
            main._current_stage
        )
        AS _current_stage,

        -- Set the stage history to aid crosscheck
        MAX(side._stage_history) OVER (PARTITION BY side._opp_id) AS _stage_history,

        -- The stage and date fields here represent those of each historical stage
        -- Will be different in each row of an opp
        side._historical_stage_change_date,
        side._next_stage AS _historical_stage,

        -- Set the stage movement 
        CASE
            WHEN side._previous_stage_prob > side._next_stage_prob
            THEN 'Downward' 
            ELSE 'Upward'
        END 
        AS _stage_movement

    FROM
        opps_created AS main

    LEFT JOIN 
        get_current_stage_and_date AS side

    ON 
        main._opp_id = side._opp_id

),

-- Tie opportunities with stage history and account engagements
combined_data AS (

    SELECT

        opp.*,
        act.*,

        CASE
            WHEN act._engagement IS NOT NULL
            THEN true 
        END 
        AS _is_matched_opp

    FROM 
        opps_history AS opp

    LEFT JOIN 
        target_account_engagements AS act
        
    ON (
            opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
        AND 
            LENGTH(opp._domain) > 1
        AND 
            LENGTH(act._6sensedomain) > 1
    )
        
    OR (
            opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
        AND    
            LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
        AND 
            LENGTH(opp._account_name) > 1
        AND 
            LENGTH(act._6sensecompanyname) > 1
    )
        
),

-- Label the activty that influenced the opportunity
set_influencing_activity AS (

    SELECT

        *,

        CASE 
            WHEN 
                DATE(_eng_timestamp) 
                    BETWEEN 
                        DATE_SUB(_created_date, INTERVAL 90 DAY) 
                    AND 
                        DATE(_created_date)
                AND 
                    REGEXP_CONTAINS(
                        _engagement, 
                        '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
                    )                     
            THEN true 
        END 
        AS _is_influencing_activity

    FROM 
        combined_data

),

-- Mark every other rows of the opportunity as influenced 
-- If there is at least one influencing activity
label_influenced_opportunity AS (
    
    SELECT

        *,

        MAX(_is_influencing_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_influenced_opp

    FROM 
        set_influencing_activity

),

-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (

    SELECT 

        *,
        
        CASE 
            WHEN 
                _is_influenced_opp IS NULL
            AND 
                _eng_timestamp > _created_date 
            AND 
                _eng_timestamp <= _historical_stage_change_date
            AND 
                _stage_movement = 'Upward'
            AND 
                REGEXP_CONTAINS(
                    _engagement, 
                    '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
                )

            THEN true
        END 
        AS _is_accelerating_activity

    FROM
        label_influenced_opportunity

),

-- Mark every other rows of the opportunity as accelerated 
-- If there is at least one accelerating activity
label_accelerated_opportunity AS (
    
    SELECT
    
        *,

        MAX(_is_accelerating_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_accelerated_opp

    FROM 
        set_accelerating_activity

),

-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (

    SELECT 

        *,
        
        CASE 
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _eng_timestamp > _created_date 
            AND 
                _eng_timestamp <= _historical_stage_change_date
            AND 
                _stage_movement = 'Upward'
            AND 
                REGEXP_CONTAINS(
                    _engagement, 
                    '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
                )
            THEN true
        END 
        AS _is_later_accelerating_activity

    FROM
        label_accelerated_opportunity

),

-- Mark every other rows of the opportunity as infuenced cum accelerated 
-- If there is at least one accelerating activity for the incluenced opp
label_influenced_opportunity_that_continue_to_accelerate AS (
    
    SELECT
    
        *,

        MAX(_is_later_accelerating_activity) OVER(
            PARTITION BY _opp_id
        )
        AS _is_later_accelerated_opp

    FROM 
        set_accelerating_activity_for_influenced_opportunity

),

-- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
label_stagnant_opportunity AS (

    SELECT
        *,

        CASE
            WHEN 
                _is_matched_opp = true 
            AND 
                _is_influenced_opp IS NULL 
            AND 
                _is_accelerated_opp IS NULL 
            AND 
                _is_later_accelerated_opp IS NULL
            THEN
                true 
        END 
        AS _is_stagnant_opp

    FROM 
        label_influenced_opportunity_that_continue_to_accelerate

),


-- Get the latest stage of each opportunity 
-- While carrying forward all its boolean fields' value caused by its historical changes 
latest_stage_opportunity_only AS (

    SELECT
        * EXCEPT(_rownum)
    FROM (

        SELECT DISTINCT

            -- Remove fields that are unique for each historical stage of opp
            * EXCEPT(
                _historical_stage_change_date,
                _historical_stage,
                _stage_movement
            ),

            -- For removing those with values in the activity boolean fields
            -- Different historical stages may have caused the influencing or accelerating
            -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
            ROW_NUMBER() OVER(
                PARTITION BY 
                    _opp_id,
                    _eng_id
                ORDER BY 
                    _is_influencing_activity DESC,
                    _is_accelerating_activity DESC,
                    _is_later_accelerating_activity DESC
            )
            AS _rownum

        FROM 
            label_stagnant_opportunity
    
    )
    WHERE _rownum = 1

)

SELECT * FROM latest_stage_opportunity_only;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `influitive.opportunity_summarized` AS

-- Opportunity information are duplicated by channel field which has ties to engagement
-- The influencing and accelerating boolean fields together with the channel are unique
-- Remove the duplicate channels and prioritize the channels with boolean values
SELECT DISTINCT
    
    _account_id,
    _account_name,
    _country,
    _domain,
    _6qa_date,
    _opp_id,
    _opp_name,
    _opp_owner_name,
    _opp_type,
    _created_date,
    _closed_date,
    _amount,
    _stage_change_date,
    _current_stage,
    _stage_history,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _is_matched_opp,
    _is_influenced_opp,

    MAX(_is_influencing_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_influencing_activity,

    _is_accelerated_opp,

    MAX(_is_accelerating_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_accelerating_activity,

    _is_later_accelerated_opp,

    MAX(_is_later_accelerating_activity) OVER(
        PARTITION BY 
            _opp_id,
            _channel
    )
    AS _is_later_accelerating_activity,

    _is_stagnant_opp,
    _channel

FROM 
    `influitive.opportunity_influenced_accelerated`;