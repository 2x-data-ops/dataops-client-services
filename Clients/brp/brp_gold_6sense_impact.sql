-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `brp.db_6sense_buying_stages_movement` AS

-- Set 6sense buying stages and their order
WITH sixsense_stage_order AS (

    SELECT 'Target' AS _buying_stage, 1 AS _order 
    UNION ALL
    SELECT 'Awareness' AS _buying_stage, 2 AS _order 
    UNION ALL
    SELECT 'Consideration' AS _buying_stage, 3 AS _order 
    UNION ALL
    SELECT 'Decision' AS _buying_stage, 4 AS _order 
    UNION ALL
    SELECT 'Purchase' AS _buying_stage, 5 AS _order 

),

-- Get 6sense buying stage data
sixsense_buying_stage_data AS (

    SELECT DISTINCT

        ROW_NUMBER() OVER(

            PARTITION BY 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain
            ORDER BY 
                CASE 
                    WHEN main._extractdate LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
                    ELSE PARSE_DATE('%F', main._extractdate)
                END 
            DESC 

        )
        AS _rownum,

        CASE 
            WHEN main._extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
            ELSE PARSE_DATE('%F', main._extractdate)
        END 
        AS _activities_on,

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        CONCAT(main._6sensecompanyname, main._6sensecountry, main._6sensedomain) AS _account_key,
        '6sense' AS _data_source,
        _buyingstagestart AS _previous_stage,
        _buyingstageend AS _current_stage,
        _segmentname AS _segment

    FROM 
        `x-marketing.brp_mysql.db_account_buying_stages` main
    LEFT JOIN `x-marketing.brp_mysql.db_segment_target_accounts` target ON CONCAT(main._6sensecompanyname, main._6sensecountry, main._6sensedomain) = CONCAT(target._6sensecompanyname, target._6sensecountry, target._6sensedomain)

),

-- Get latest week data for each account
-- Add 6sense buying stage order into the mix
-- Evaluate the movement of the stage
latest_sixsense_buying_stage_with_order_and_movement AS (

    SELECT

        main.* EXCEPT(_rownum),
        prev._order AS _previous_stage_order,
        curr._order AS _current_stage_order,

        CASE
            WHEN curr._order > prev._order 
            THEN '+ve'
            WHEN curr._order < prev._order 
            THEN '-ve'
            ELSE 'Stagnant'
        END 
        AS _movement

    FROM 
        sixsense_buying_stage_data AS main 
    
    LEFT JOIN 
        sixsense_stage_order AS prev 
    ON 
        main._previous_stage = prev._buying_stage
    
    LEFT JOIN 
        sixsense_stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage
    
    WHERE 
        main._rownum = 1

)
SELECT * FROM latest_sixsense_buying_stage_with_order_and_movement;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- 6sense Account Current State

CREATE OR REPLACE TABLE `brp.db_6sense_account_current_state` AS

-- Get all target accounts and their segments
WITH target_accounts AS (

    SELECT         

        * EXCEPT(_rownum)
    
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

            '6sense' AS _data_source,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _account_key,
            _segmentname AS _segment,
            
            -- Get the LATEST date of appearance of each account
            ROW_NUMBER() OVER(

                PARTITION BY 
                    _6sensecompanyname,
                    _6sensecountry,
                    _6sensedomain
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
            `x-marketing.brp_mysql.db_segment_target_accounts`
        WHERE _sdc_deleted_at IS NULL

    ) 

    WHERE 
        _rownum = 1
    
),

-- Get date when account had first impression
reached_related_info AS (

    SELECT

        * EXCEPT(_rownum)

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

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
            
            )
            AS _first_impressions,

            CASE
                WHEN _websiteengagement = '-'
                THEN CAST(NULL AS STRING)
                ELSE _websiteengagement
            END 
            AS _website_engagement,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                        ELSE PARSE_DATE('%F', _latestimpression)
                    END
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _account_key

        FROM 
            `x-marketing.brp_mysql.db_campaign_reached_accounts`

        WHERE 
            _campaignid IN (

                SELECT DISTINCT _campaignid
                FROM `x-marketing.brp_mysql.db_daily_campaign_performance`
                WHERE _datatype = 'Campaign'

            )
        AND _sdc_deleted_at IS NULL

    )

    WHERE 
        _rownum = 1

),

-- Get the date when account first became a 6QA
/*six_qa_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            CASE 
                WHEN _6qadate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _6qadate)
                ELSE PARSE_DATE('%F', _6qadate)
            END 
            AS _6qa_date,

            true _is_6qa,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) 
                ORDER BY 
                    CASE 
                        WHEN _6qadate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _6qadate)
                        ELSE PARSE_DATE('%F', _6qadate)
                    END 
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _account_key

        FROM 
            `abbyy_mysql.db_6qa_status`
    
    )

    WHERE 
        _rownum = 1

),*/

-- Get the buying stage info each account
buying_stage_related_info AS (

    SELECT DISTINCT

        _previous_stage,
        _previous_stage_order,
        _current_stage,
        _current_stage_order,
        _movement,
        _activities_on AS _movement_date,
        _account_key

    FROM
        `brp.db_6sense_buying_stages_movement`
    
    WHERE 
        _data_source = '6sense'

),

-- Attach all other data parts to target accounts
combined_data AS (

    SELECT DISTINCT 

        target.*, 
        reached.* EXCEPT(_account_key),
        --six_qa.* EXCEPT(_account_key),
        stage.* EXCEPT(_account_key)   

    FROM
        target_accounts AS target

    LEFT JOIN
        reached_related_info AS reached 

    USING (_account_key)

    --LEFT JOIN
        --six_qa_related_info AS six_qa 
    
    --USING (_account_key) 

    LEFT JOIN
        buying_stage_related_info AS stage
    
    USING (_account_key) 

)

SELECT * FROM combined_data;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- 6sense Engagement Log

CREATE OR REPLACE TABLE `brp.db_6sense_engagement_log` AS

-- Get all 6sense target accounts and their unique info
WITH target_accounts AS (

    SELECT * EXCEPT(_account_key), _account_key AS _country_account FROM `brp.db_6sense_account_current_state`

    WHERE _data_source = '6sense'

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
        --CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
        CONCAT(main._6sensecompanyname,main._6sensecountry,main._6sensedomain) AS _country_account
    
    FROM 
        `x-marketing.brp_mysql.db_campaign_reached_accounts` main
    
    JOIN (

        SELECT DISTINCT 

            _campaignid, 
            _name AS _campaignname,  
            IF(_campaigntype = '6Sense', '6sense', _campaigntype) AS _campaigntype
            
        FROM
            `x-marketing.brp_mysql.db_daily_campaign_performance`
        WHERE 
            _campaignid != '' AND _datatype = 'Campaign'

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

intent_engagements AS (

    SELECT

        CONCAT(
            _companyname, 
            SPLIT(_companyinfo, ' - ')[ORDINAL(1)], 
            TRIM(SPLIT(_companyinfo, ' -')[ORDINAL(2)])    
        ) 
        AS _country_account,
        
        CAST(NULL AS STRING) AS _email,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _timestamp,

        CASE
            WHEN _categoryname LIKE '%Keyword%'
            THEN '6sense Searched Keywords'
            WHEN _categoryname LIKE '%Website%'
            THEN '6sense Website Visited'
            WHEN _categoryname LIKE '%Topic%'
            THEN '6sense Bombora Topics'
        END 
        AS _engagement,

        '6sense' AS _engagement_data_source,
        _categoryvalue AS _description,
        1 AS _notes

    FROM 
        `brp_mysql.db_account_activity_summary`

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
        SELECT * FROM intent_engagements
        
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

CREATE OR REPLACE TABLE `brp.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (

    SELECT DISTINCT 

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        --_6qa_date,
        _engagement, 
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,
        _segment AS _segment,
        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END 
        AS _channel

    FROM 
        `brp.db_6sense_engagement_log` 
    

),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (

    SELECT DISTINCT 

        _accountid AS _account_id, 
        _accountname AS _account_name,
        _website AS _domain,
        _shippingcountry AS _country,
        _opportunityid AS _opp_id,
        _opportunityname AS _opp_name,
        _opportunityowner AS _opp_owner_name,
        _type AS _opp_type,

        CASE 
            WHEN _createddate = '' 
            THEN CAST(NULL AS DATE)
            WHEN _createddate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _createddate)
            ELSE PARSE_DATE('%F', _createddate)
        END 
        AS _created_date,
        
        CASE 
            WHEN _closedate = '' 
            THEN CAST(NULL AS DATE)
            WHEN _closedate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _closedate)
            ELSE PARSE_DATE('%F', _closedate)
        END 
        AS _closed_date,
        
        CASE WHEN _amount = '' THEN NULL 
        WHEN _amount LIKE '%$%' THEN CAST(REPLACE(REPLACE(_amount, '$', ''), ',', '') AS FLOAT64)
        ELSE CAST(_amount AS FLOAT64) END
        AS _arr

    FROM 
        `x-marketing.brp_mysql.db_stratus_opportunity` 
    WHERE _sdc_deleted_at IS NULL

),

-- Get all historical stages of opp
-- Perform necessary cleaning of the data
opps_historical_stage AS (

    SELECT DISTINCT

        _opportunityid AS _opp_id,

        CASE 
            WHEN _editdate NOT LIKE '%M%'
            THEN PARSE_TIMESTAMP('%m/%d/%Y %R', _editdate)
            ELSE PARSE_TIMESTAMP('%m/%d/%Y, %I:%M %p', _editdate)
        END 
        AS _historical_stage_change_timestamp, 

        CASE 
            WHEN _editdate LIKE '%/%'
            THEN DATE(PARSE_TIMESTAMP('%m/%d/%Y, %I:%M %p',_editdate))
            ELSE PARSE_DATE('%F', SPLIT(_editdate, ' ')[ORDINAL(1)])
        END 
        AS _historical_stage_change_date,

        _newvalue AS _next_stage,
        CASE WHEN _newvalue = 'First Meeting' THEN 1
        WHEN _newvalue = 'Pending Approval' THEN 1
        WHEN _newvalue LIKE 'Discovery%' THEN 2
        WHEN _newvalue = 'Gain Agreement' THEN 3
        WHEN _newvalue = 'Present Proposal' THEN 4
        WHEN _newvalue LIKE 'Secure BOR%' THEN 5
        WHEN _newvalue = 'Approved' THEN 6
        WHEN _newvalue = 'Closed' THEN 7
        WHEN _newvalue = 'Closed Won' THEN 7
        WHEN _newvalue = 'Closed Lost' THEN 7
        ELSE NULL END AS _next_stage_prob,
        _oldvalue AS _previous_stage,
        CASE WHEN _oldvalue = 'First Meeting' THEN 1
        WHEN _oldvalue = 'Pending Approval' THEN 1
        WHEN _oldvalue LIKE 'Discovery%' THEN 2
        WHEN _oldvalue = 'Gain Agreement' THEN 3
        WHEN _oldvalue = 'Present Proposal' THEN 4
        WHEN _oldvalue LIKE 'Secure BOR%' THEN 5
        WHEN _oldvalue = 'Approved' THEN 6
        WHEN _oldvalue = 'Closed' THEN 7
        WHEN _oldvalue = 'Closed Won' THEN 7
        WHEN _oldvalue = 'Closed Lost' THEN 7
        ELSE NULL END AS _previous_stage_prob   
    FROM 
        `x-marketing.brp_mysql.db_stratus_opportunity_history` 
    WHERE _fieldevent = 'Stage' AND _sdc_deleted_at IS NULL

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

        main.*,
        
        -- Fill the current stage and date for an opp
        -- Will be the same in each row of an opp
        MAX(side._stage_change_date) OVER (PARTITION BY side._opp_id) AS _stage_change_date,
        MAX(side._current_stage) OVER (PARTITION BY side._opp_id) AS _current_stage,

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

    JOIN 
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
            opp._domain = act._6sensedomain
        AND 
            LENGTH(opp._domain) > 1
        AND 
            LENGTH(act._6sensedomain) > 1
    )
        
    OR (
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
            THEN true 
        END 
        AS _is_influencing_activity

    FROM 
        combined_data

) ,

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
/*
-- Label accounts that became 6QA before the influenced opportunity was created
label_6qa_before_influenced_opportunity AS (

    SELECT

        *,

        CASE
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _6qa_date < _created_date
            -- AND 
            --     DATE(_6qa_date) 
            --         BETWEEN 
            --             DATE(_created_date)
            --         AND 
            --             DATE_ADD(_created_date, INTERVAL 90 DAY)                       
            THEN true 
        END 
        AS _is_6qa_before_influenced_opp

    FROM 
        label_influenced_opportunity

),

-- Label accounts that became 6QA after the influenced opportunity was created
label_6qa_after_influenced_opportunity AS (

    SELECT

        *,

        CASE
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _6qa_date > _created_date
            -- AND 
            --     DATE(_6qa_date) 
            --         BETWEEN 
            --             DATE(_created_date)
            --         AND 
            --             DATE_ADD(_created_date, INTERVAL 90 DAY)                       
            THEN true 
        END 
        AS _is_6qa_after_influenced_opp

    FROM 
        label_6qa_before_influenced_opportunity

),
*/
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
            --AND 
               -- _stage_movement = 'Upward'

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
            --AND 
                --_stage_movement = 'Upward'

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
                _historical_stage
                --_stage_movement
            ),

            -- For removing those with values in the activity boolean fields
            -- Different historical stages may have caused the influencing or accelerating
            -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
            ROW_NUMBER() OVER(
                PARTITION BY 
                    _opp_id,
                    _eng_timestamp,
                    _engagement,
                    _eng_description
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

CREATE OR REPLACE TABLE `brp.opportunity_summarized` AS

-- Opportunity information are duplicated by channel field which has ties to engagement
-- The influencing and accelerating boolean fields together with the channel are unique
-- Remove the duplicate channels and prioritize the channels with boolean values
SELECT DISTINCT
    
    _account_id,
    _account_name,
    _country,
    _domain,
    --_6qa_date,
    _opp_id,
    _opp_name,
    _opp_owner_name,
    _opp_type,
    _created_date,
    _closed_date,
    _arr,
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

    --_is_6qa_before_influenced_opp,
    --_is_6qa_after_influenced_opp,
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
    _channel,
    _segment

FROM 
    `brp.opportunity_influenced_accelerated`;


-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


-- 6sense Ad Performance

CREATE OR REPLACE TABLE `brp.db_6sense_ad_performance` AS

-- Get ads data
WITH ads AS (
    SELECT *
    EXCEPT (_rownum)
    FROM (
        SELECT DISTINCT
            _campaignid AS _campaign_id,
            _name AS _advariation,
            _6senseid AS _adid,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
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
        FROM `x-marketing.brp_mysql.db_daily_campaign_performance`
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
            _linkedincampaignid,
            _segment,
            CASE
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _extractdate)
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
                        THEN PARSE_DATE('%m/%d/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC
            ) 
            AS _rownum

        FROM 
            `x-marketing.brp_mysql.db_daily_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),
ads_campaign_combined AS (
    SELECT ads.*,
        campaign_fields._linkedincampaignid,
        -- campaign_fields._campaign_id,
        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts,
        campaign_fields._segment
    FROM ads
    JOIN campaign_fields ON ads._campaign_id = campaign_fields._campaign_id
)
,
airtable_fields AS (

    SELECT DISTINCT 
        _campaignid AS _campaign_id, 
        _adid AS _ad_id,
        _adname,
        _adgroup AS _ad_group,
        _screenshot
    FROM
        -- `x-marketing.brp_mysql.optimization_airtable_ads_linkedin`
        `x-marketing.brp_mysql.optimization_airtable_ads_6sense`
    WHERE 
        _campaignid != ''

),
combined_data AS (

    SELECT
        ads_campaign_combined.*,
        airtable_fields._ad_group,
        airtable_fields._screenshot
    FROM 
        ads_campaign_combined
    LEFT JOIN
        airtable_fields 
    ON (
            ads_campaign_combined._adid = airtable_fields._ad_id
        AND 
            ads_campaign_combined._campaign_id = airtable_fields._campaign_id
    )
    OR (
            airtable_fields._ad_id IS NULL
        AND 
            ads_campaign_combined._campaign_id = airtable_fields._campaign_id
    )
    LEFT JOIN 
        campaign_fields
    ON 
        ads_campaign_combined._campaign_id = campaign_fields._campaign_id

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
                `x-marketing.brp_mysql.db_segment_target_accounts` main
            
            JOIN `x-marketing.brp_mysql.optimization_airtable_ads_6sense` side

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
                `x-marketing.brp_mysql.db_segment_target_accounts` main
            
            JOIN `x-marketing.brp_mysql.optimization_airtable_ads_6sense` side             
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `x-marketing.brp_mysql.db_campaign_reached_accounts` extra

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

 /*   -- Get accounts that are 6QA
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
                `x-marketing.brp_mysql.db_segment_target_accounts` main
            
            JOIN 
                `x-marketing.brp_mysql.optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `brp.db_6sense_account_current_state` extra
            
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

    USING(_campaign_id)
*/
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
        --_6qa_accounts / _occurrence AS _reduced_6qa_accounts

    FROM 
        total_ad_occurrence_per_campaign

)

SELECT * FROM reduced_campaign_numbers;


-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------

-- 6sense Account Performance

CREATE OR REPLACE TABLE `brp.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,            
        side._segment,
        side._campaignid AS _campaignid,
        side._campaignname AS _campaignname
    FROM 
        `x-marketing.brp_mysql.db_segment_target_accounts` main
    
    JOIN (
        SELECT li_id.* EXCEPT(_campaignid),
            sixsense_id._campaignid
        FROM `x-marketing.brp_mysql.optimization_airtable_ads_linkedin` li_id
        JOIN `x-marketing.brp_mysql.db_daily_campaign_performance` sixsense_id
        ON li_id._campaignid = sixsense_id._linkedincampaignid
    ) side
    
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
        AS _has_impressions,

        CAST(REGEXP_EXTRACT(side._spend, r'[\d.]+') AS FLOAT64) AS _accountSpend,
        CAST(REPLACE(side._impressions, ',', '') AS INT64) AS _impressions,
        CAST(REPLACE(side._clicks, ',', '') AS INT64) AS _clicks,
        side._batchid

    FROM 
        target_accounts AS main

    LEFT JOIN 
        `x-marketing.brp_mysql.db_campaign_reached_accounts` side 

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts
WHERE _batchid = (SELECT MAX(_batchid) FROM `x-marketing.brp_mysql.db_campaign_reached_accounts`)
;