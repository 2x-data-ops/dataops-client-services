-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `exiger.db_6sense_buying_stages_movement` AS

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
)
-- Get buying stage data
, buying_stage_data AS (
    SELECT 
      DISTINCT
      _6sensecompanyname,
      _6sensecountry,
      _6sensedomain,
      _buyingstageend AS _buying_stage,
      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _account_key,
      CASE WHEN _extractdate LIKE '0%'  THEN PARSE_DATE('%m/%d/%y', _extractdate)
      ELSE PARSE_DATE('%m/%d/%Y', _extractdate) END AS _activities_on        
    FROM`exiger_mysql.db_account_buying_stages`
    WHERE LENGTH(_extractdate) > 0
)
, buying_stages AS (
    SELECT 
      DISTINCT 
      _6sensecountry, 
      _6sensecompanyname, 
      MIN(_activities_on) AS _activities_on 
    FROM buying_stage_data 
    GROUP BY 1, 2
)
-- Get first ever buying stage for each account
, first_ever_buying_stage AS (
    SELECT 
      DISTINCT 
      _activities_on,
      _6sensecountry,
      _6sensedomain, 
      _6sensecompanyname,
      _buying_stage,
      'Initial' AS _source,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account,
      _account_key
    FROM buying_stage_data
    JOIN buying_stages USING(_6sensecountry, _6sensecompanyname, _activities_on)
) 
, buying_stage_datas AS (
    SELECT 
      DISTINCT
      _activities_on,
      _6sensecountry,
      _6sensedomain, 
      _6sensecompanyname,
      _buying_stage,
      'Non Initial' AS _source,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account,
      _account_key
    FROM buying_stage_data
)
-- Get every other buying stage for each account
, every_other_buying_stage AS (
    SELECT 
        * 
    FROM buying_stage_datas
    -- Exclude those that are first ever stages
    WHERE CONCAT(_country_account, _activities_on) NOT IN (
            SELECT DISTINCT 
                CONCAT(_country_account, MIN(_activities_on)) 
            FROM 
                first_ever_buying_stage 
            GROUP BY 
                _country_account
              )
)
-- Combine both first ever data and every other data
, historical_buying_stage AS (
    SELECT * FROM first_ever_buying_stage   
    UNION DISTINCT
    SELECT * FROM every_other_buying_stage
) , historical_buying_stages AS (
  SELECT 
    DISTINCT 
    _6sensecountry,
    _6sensedomain, 
    _6sensecompanyname,
    _buying_stage AS _current_stage,
    _activities_on,
    LAG(_buying_stage) OVER( PARTITION BY _6sensedomain ORDER BY _activities_on ASC ) AS _prev_stage,
    _source,
    _country_account,
    _account_key
    FROM historical_buying_stage
),
-- Get the current stage and previous stage for each historical record of an account
 set_buying_stage_order AS (
    SELECT 
      DISTINCT 
        main.* EXCEPT(_current_stage, _prev_stage),
        main._current_stage,
        IF(_activities_on = (MIN(_activities_on) OVER( PARTITION BY _6sensedomain, _6sensecountry ORDER BY _activities_on)) AND _prev_stage IS NULL, _current_stage, _prev_stage  ) AS _prev_stage,
        curr._order AS _curr_order,
        IF(_activities_on = (MIN(_activities_on) OVER( PARTITION BY _6sensedomain, _6sensecountry ORDER BY _activities_on) )AND _prev_stage IS NULL, curr._order, prev._order) AS _prev_order
    FROM historical_buying_stages main
    LEFT JOIN stage_order AS curr ON main._current_stage = curr._buying_stage
    LEFT JOIN stage_order AS prev ON main._prev_stage = prev._buying_stage
)
 , set_buying_stages_order AS (
    SELECT 
      DISTINCT 
      *,
      IF( _curr_order > _prev_order,  "+ve", IF(_curr_order < _prev_order, "-ve", "Stagnant")) AS _movement
    FROM set_buying_stage_order
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _country_account ORDER BY _activities_on DESC) = 1

)
SELECT * FROM set_buying_stages_order;



CREATE OR REPLACE TABLE `exiger.db_6sense_account_current_state` AS
WITH target_accounts AS (

    SELECT 

        * EXCEPT(_rownum)
    
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
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _added_on,

            '6sense' AS _data_source,
            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account,
            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _account_key,
            
            -- Get the earliest date of appearance of each account
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

            )
            AS _rownum

        FROM 
            `exiger_mysql.db_target_account`

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
            `exiger_mysql.db_campaign_reached_account`

        -- WHERE 
        --     _campaignid IN (

        --         SELECT DISTINCT 
        --             _campaignid 
        --         FROM 
        --             `exiger_mysql.db_campaign_segment`

        --     )

    )

    WHERE 
        _rownum = 1

),

-- Get the date when account first became a 6QA
six_qa_related_info AS (

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
            `exiger_mysql.db_6qa_status`
    
    )

    WHERE 
        _rownum = 1

),

-- Get the buying stage info each account
buying_stage_related_info AS (

    SELECT DISTINCT

        _prev_stage ,
        _prev_order ,
        _current_stage,
        _curr_order,
        _movement,
        _activities_on AS _movement_date,
        _account_key

    FROM
        `exiger.db_6sense_buying_stages_movement`
    
    -- WHERE 
    --     _data_source = '6sense'

),

-- Attach all other data parts to target accounts
combined_data AS (

    SELECT DISTINCT 

        target.*, 
        reached.* EXCEPT(_account_key),
        six_qa.* EXCEPT(_account_key),
        stage.* EXCEPT(_account_key)   

    FROM
        target_accounts AS target

    LEFT JOIN
        reached_related_info AS reached 

    USING (_account_key)

    LEFT JOIN
        six_qa_related_info AS six_qa 
    
    USING (_account_key) 

    LEFT JOIN
        buying_stage_related_info AS stage
    
    USING (_account_key) 

)

SELECT * FROM combined_data;



-- 6sense Engagement Log
CREATE OR REPLACE TABLE `exiger.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (
    SELECT 
        _6sensecompanyname AS _6sense_company_name,
        _6sensecountry AS _6sense_country,
        _6sensedomain AS _6sense_domain, 
        _domain,
        _6senseindustry AS _6sense_industry,
        _6senseemployeerange AS _6sense_employee_range,
        _6senserevenuerange AS _6sense_revenue_range,
        _added_on,
        _country_account,
        _first_impressions,
        _website_engagement,
        _6qa_date,
        _is_6qa,

        _prev_stage,
        _prev_order,
        _current_stage,
        _curr_order,
        _movement,
        _movement_date,
        CONCAT(_6sensecompanyname, _6sensecountry,_6sensedomain) AS _account_key,
    FROM `exiger.db_6sense_account_current_state`
),
-- Prep the reached account data for use later
campaign_airtable_ads_6sense AS (
     SELECT DISTINCT 
      _campaignid, 
      _name AS _campaign_name,  
      IF(_campaigntype = '6Sense', '6sense', _campaigntype) AS _campaigntype
    FROM `x-marketing.exiger_mysql.db_daily_campaign_performance`
    WHERE _campaignid != '' AND _datatype = 'Campaign'
),
reached_accounts_data AS (
    SELECT DISTINCT
      CAST(main._clicks AS INTEGER) AS _clicks,
      CAST(main._influencedformfills AS INTEGER) AS _influenced_form_fills,
      CASE WHEN main._latestimpression LIKE '%/%'THEN PARSE_DATE('%m/%e/%Y', main._latestimpression)
      ELSE PARSE_DATE('%F', main._latestimpression) END AS _latest_impression, 
      CASE WHEN main._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
      ELSE PARSE_DATE('%F', main._extractdate) END AS _activities_on, 
      main._campaignid AS _campaign_id,
      -- Need label to distingush 6sense and Linkedin campaigns
      side._campaigntype AS _campaign_type,
      side._campaign_name AS _campaign_name,
      CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account,
      CONCAT(_6sensecompanyname, _6sensecountry,_6sensedomain) AS _account_key,
    FROM `x-marketing.exiger_mysql.db_campaign_reached_account` main
    JOIN campaign_airtable_ads_6sense side
    USING(_campaignID)
),
-- Get campaign reached engagements
campaign_reached AS (
    SELECT DISTINCT 
    _country_account,
        _account_key, 
        CAST(NULL AS STRING) AS _email,
        MIN(_latest_impression) OVER(
            PARTITION BY _country_account,_account_key, _campaign_name
            ORDER BY _latest_impression
        ) AS _timestamp,
        CONCAT(_campaign_type, ' ', 'Campaign Reached') AS _engagement,
        --'6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaign_name AS _description, 
        1 AS _notes
    FROM reached_accounts_data
),
-- Get ad clicks engagements
ad_clicks AS (
    SELECT DISTINCT 
    _country_account,
       _account_key,
        CAST(NULL AS STRING) AS _email,  
        _activities_on AS _timestamp,
        CONCAT(_campaign_type, ' ', 'Ad Clicks') AS _engagement,
        --'6sense Ad Clicks' AS _engagement,  
        '6sense' AS _engagement_data_source,
        _campaign_name AS _description,  
        _clicks AS _notes
    FROM reached_accounts_data 
    WHERE _clicks >= 1
    -- Get those who have increased in numbers from the last period
    QUALIFY (_clicks - COALESCE(LAG(_clicks) OVER(
        PARTITION BY _country_account, _account_key,_campaign_name
        ORDER BY _activities_on
    ), 0)) >= 1
),
-- Get form fills engagements
influenced_form_fills AS (
    SELECT DISTINCT 
    _country_account,
      _account_key,
      CAST(NULL AS STRING) AS _email, 
      _activities_on AS _timestamp,
      --CONCAT(_campaign_type, ' ', 'Influenced Form Filled') AS _engagement, 
      '6sense Influenced Form Filled' AS _engagement,
      '6sense' AS _engagement_data_source,
      _campaign_name AS _description,  
      _influenced_form_fills AS _notes
    FROM reached_accounts_data 
    WHERE _influenced_form_fills >= 1
    QUALIFY (_influenced_form_fills - COALESCE(LAG(_influenced_form_fills) OVER(
        PARTITION BY _country_account,_account_key, _campaign_name
        ORDER BY _activities_on
    ), 0)) >= 1
),

intent_engagements AS (
    SELECT
   CONCAT(_companyname, SPLIT(_companyinfo, ' - ')[ORDINAL(1)]) AS _country_account,
     CONCAT(_companyname, SPLIT(_companyinfo, ' - ')[ORDINAL(1)], TRIM(SPLIT(_companyinfo, ' -')[SAFE_ORDINAL(2)])) AS _account_key,
      CAST(NULL AS STRING) AS _email,
      CASE 
        WHEN _extractdate LIKE '%/%'THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE PARSE_DATE('%F', _extractdate)
      END AS _timestamp,
      CASE 
        WHEN _categoryname LIKE '%Keyword%' THEN '6sense Searched Keywords'
        WHEN _categoryname LIKE '%Website%' THEN '6sense Website Visited'
        WHEN _categoryname LIKE '%Topic%' THEN '6sense Bombora Topics'
        END AS _engagement,
      '6sense' AS _engagement_data_source,
        _categoryvalue AS _description,
      1 AS _notes
    FROM `exiger_mysql.db_account_activity_summary`

),
-- Only activities involving target accounts are considered
combined_data AS (
    SELECT DISTINCT 
        target_accounts._6sense_company_name,
        target_accounts._6sense_country,
        target_accounts._6sense_domain,
        target_accounts._domain,
        target_accounts._6sense_industry,
        target_accounts._6sense_employee_range,
        target_accounts._6sense_revenue_range,
        target_accounts._added_on,
        target_accounts._country_account,
        target_accounts._first_impressions,
        target_accounts._website_engagement,
        target_accounts._6qa_date,
        target_accounts._is_6qa,

        target_accounts._prev_stage,
        target_accounts._prev_order,
        target_accounts._current_stage,
        target_accounts._curr_order,
        target_accounts._movement,
        target_accounts._movement_date,

        activities._email,
        activities._timestamp,
        activities._engagement,
        activities._engagement_data_source,
        activities._description,
        activities._notes 
    FROM (
        SELECT * FROM campaign_reached 
        UNION DISTINCT
        SELECT * FROM ad_clicks 
        UNION DISTINCT
        SELECT * FROM influenced_form_fills
        UNION DISTINCT
        SELECT * FROM intent_engagements
    ) activities
    JOIN target_accounts
    USING (_account_key)
),
-- Get accumulated values for each engagement
accumulated_engagement_values AS (
    SELECT
        *,
         -- The aggregated values
        SUM(CASE WHEN _engagement = '6sense Advertising Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Advertising Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = '6sense Influenced Form Filled' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_influence_form_filled,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_opens,
                SUM(CASE WHEN _engagement = '6sense Website Visited' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_website_visit,
        SUM(CASE WHEN _engagement = '6sense Bombora Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora,
    FROM combined_data
)
SELECT * FROM accumulated_engagement_values;



-- Opportunity Influenced + Accelerated

-- Opportunity Influenced + Accelerated

CREATE OR REPLACE TABLE `exiger.opportunity_influenced_accelerated` AS
WITH 
account_engagements AS (
    SELECT DISTINCT 
         _6sense_company_name,
        _6sense_country,
        _6sense_domain, 
        _6qa_date,
        _engagement,
        --_account_id, 
        ROW_NUMBER() OVER() AS _eng_id,
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,
        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END AS _channel
    FROM `exiger.db_6sense_engagement_log`
),
-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (
    SELECT DISTINCT 
        opp.accountid AS _account_id, 
        act.name AS _account_name,
        REGEXP_EXTRACT(website, r"(?:https?://)?(?:www\.)?([^/]+)")  AS _domain,  
        country__c AS _country,
        opp.id AS _opp_id,
        opp.name AS _opp_name,
        own.name AS _opp_owner_name,
        opp.type AS _opp_type,
        DATE(opp.createddate) AS _created_date,
        DATE(opp.closedate) AS _closed_date,
        opp.amount AS _amount,
        opp.product_type__c AS _product_type
    FROM `exiger_salesforce.Opportunity` opp
    LEFT JOIN `exiger_salesforce.Account` act
        ON opp.accountid = act.id 
    LEFT JOIN `exiger_salesforce.User` own
        ON opp.ownerid = own.id 
    WHERE opp.isdeleted = FALSE
        AND EXTRACT(YEAR FROM opp.createddate) >= 2023 
),
-- Get all historical stages of opp
-- Perform necessary cleaning of the data
opp_field_history AS (
    SELECT DISTINCT 
        opportunityid AS _opp_id,
        createddate AS _historical_stage_change_timestamp,
        DATE(createddate) AS _historical_stage_change_date,
        oldvalue AS _previous_stage,
        newvalue AS _next_stage
    FROM `exiger_salesforce.OpportunityFieldHistory` 
    WHERE field = 'StageName'
        AND isdeleted = FALSE
),
opp_history AS (
    SELECT DISTINCT 
        opportunityid AS _opp_id,
        TIMESTAMP(CONCAT(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(createddate)), ':00 UTC')) AS _historical_stage_change_timestamp,
        DATE(createddate) AS _historical_stage_change_date,
        LAG(probability) OVER( PARTITION BY  opportunityid ORDER BY createddate) AS _previous_stage_prob,
        probability AS _next_stage_prob,
        LAG(stagename) OVER( PARTITION BY  opportunityid ORDER BY createddate) AS _previous_stage,
        stagename
    FROM `x-marketing.exiger_salesforce.OpportunityHistory`
    WHERE isdeleted = FALSE 
),
opps_historical_stage AS (
    SELECT
        main._opp_id,
        main._historical_stage_change_timestamp,
        main._historical_stage_change_date,
        main._previous_stage,
        main._next_stage,
        side._previous_stage_prob,
        side._next_stage_prob
    FROM opp_field_history main
    JOIN opp_history side
    USING (
        _opp_id,
        _historical_stage_change_date,
        _previous_stage
    )
),
-- There are several stages that can occur on the same day
-- Get unique stage on each day 
unique_opps_historical_stage AS (
    SELECT
        _opp_id,
        _historical_stage_change_timestamp,
        _historical_stage_change_date,
        _previous_stage,
        _next_stage,
        _previous_stage_prob,
        _next_stage_prob,
        -- Setting the rank of the historical stage based on stage change date
        ROW_NUMBER() OVER(
            PARTITION BY _opp_id
            ORDER BY _historical_stage_change_date DESC
        ) AS _stage_rank
    FROM opps_historical_stage
    -- Those on same day are differentiated by timestamp
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY _opp_id, _historical_stage_change_date
        ORDER BY _historical_stage_change_timestamp DESC 
    ) = 1
),
-- Generate a log to store stage history from latest to earliest
get_aggregated_stage_history_text AS (
    SELECT
        *,
        STRING_AGG(CONCAT('[ ', _historical_stage_change_date, ' ]',' : ', _next_stage),'; ') 
        OVER( PARTITION BY _opp_id ORDER BY _stage_rank) AS _stage_history
    FROM unique_opps_historical_stage
),
-- Obtain the current stage and the stage date in this CTE 
get_current_stage_and_date AS (
    SELECT
        *,
        CASE 
            WHEN _stage_rank = 1 
            THEN _historical_stage_change_date
        END AS _stage_change_date,
        CASE 
            WHEN _stage_rank = 1 
            THEN _next_stage
        END AS _current_stage
    FROM get_aggregated_stage_history_text
),
-- Add the stage related fields to the opps data
opps_history AS (
    SELECT
        main._account_id,
        main._account_name,
        main._domain,
        main._country,
        main._opp_id,
        main._opp_name,
        main._opp_owner_name,
        main._opp_type,
        main._created_date,
        main._closed_date,
        main._amount,
        main._product_type,
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
        END AS _stage_movement
    FROM opps_created AS main
    JOIN get_current_stage_and_date AS side
        ON main._opp_id = side._opp_id
)
,
-- Tie opportunities with stage history and account engagements
combined_data AS (
    SELECT
        opp.*,
        act.* ,
        CASE
            WHEN act._engagement IS NOT NULL
            THEN TRUE 
        END AS _is_matched_opp
    FROM opps_history AS opp
   JOIN account_engagements AS act
        ON (
            opp._domain = act._6sense_domain
        AND 
            LENGTH(opp._domain) > 1
        AND 
            LENGTH(act._6sense_domain) > 1
    )
        
    OR (
            LOWER(opp._account_name) = LOWER(act. _6sense_company_name)
        AND 
            LENGTH(opp._account_name) > 1
        AND 
            LENGTH(act. _6sense_company_name) > 1
    )
),
-- Label the activty that influenced the opportunity
set_influencing_activity AS (
    SELECT
        *,
        CASE 
            WHEN DATE(_eng_timestamp) 
                BETWEEN DATE_SUB(_created_date, INTERVAL 90 DAY) 
                AND DATE(_created_date)                     
            THEN TRUE 
        END AS _is_influencing_activity
    FROM combined_data
),
-- Mark every other rows of the opportunity as influenced 
-- If there is at least one influencing activity
label_influenced_opportunity AS (  
    SELECT
        *,
        MAX(_is_influencing_activity) OVER(
            PARTITION BY _opp_id
        ) AS _is_influenced_opp
    FROM set_influencing_activity
),
-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NULL
            AND _eng_timestamp > _created_date 
            AND _eng_timestamp <= _historical_stage_change_date
            AND _stage_movement = 'Upward'
            THEN TRUE
        END AS _is_accelerating_activity
    FROM label_influenced_opportunity
),
-- Mark every other rows of the opportunity as accelerated 
-- If there is at least one accelerating activity
label_accelerated_opportunity AS (    
    SELECT
        *,
        MAX(_is_accelerating_activity) OVER(
            PARTITION BY _opp_id
        ) AS _is_accelerated_opp
    FROM set_accelerating_activity
),
-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NOT NULL
            AND _eng_timestamp > _created_date 
            AND _eng_timestamp <= _historical_stage_change_date
            AND _stage_movement = 'Upward'
            THEN TRUE
        END AS _is_later_accelerating_activity
    FROM label_accelerated_opportunity
),
-- Mark every other rows of the opportunity as infuenced cum accelerated 
-- If there is at least one accelerating activity for the incluenced opp
label_influenced_opportunity_that_continue_to_accelerate AS (    
    SELECT
        *,
        MAX(_is_later_accelerating_activity) OVER(
            PARTITION BY _opp_id
        ) AS _is_later_accelerated_opp
    FROM set_accelerating_activity_for_influenced_opportunity
),
-- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
label_stagnant_opportunity AS (
    SELECT
        *,
        CASE
            WHEN _is_matched_opp = TRUE 
            AND _is_influenced_opp IS NULL 
            AND _is_accelerated_opp IS NULL 
            AND _is_later_accelerated_opp IS NULL
            THEN TRUE 
        END AS _is_stagnant_opp
    FROM label_influenced_opportunity_that_continue_to_accelerate
),
-- Get the latest stage of each opportunity 
-- While carrying forward all its boolean fields' value caused by its historical changes 
latest_stage_opportunity_only AS (
    SELECT DISTINCT
        -- Remove fields that are unique for each historical stage of opp
        * EXCEPT(
            _historical_stage_change_date,
            _historical_stage,
            _stage_movement
        )
    FROM label_stagnant_opportunity
    -- For removing those with values in the activity boolean fields
    -- Different historical stages may have caused the influencing or accelerating
    -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY _opp_id, _eng_id
        ORDER BY _is_influencing_activity DESC,
                 _is_accelerating_activity DESC,
                 _is_later_accelerating_activity DESC
    ) = 1
)

SELECT * FROM latest_stage_opportunity_only;


---- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `exiger.opportunity_summarized` AS

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
    _product_type,
    _created_date,
    _closed_date,
    _amount,
    _stage_change_date,
    _current_stage,
    _stage_history,
    _6sense_company_name,
    _6sense_country,
    _6sense_domain,
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
    `exiger.opportunity_influenced_accelerated`;
