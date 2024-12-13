TRUNCATE TABLE`ridecell.opportunity_influenced_accelerated`;

INSERT INTO `ridecell.opportunity_influenced_accelerated` (
    _account_id,
    _account_name,
    _domain,
    _country,
    _opp_id,
    _opp_name,
    _opp_owner_name,
    _opp_type,
    _created_date,
    _closed_date,
    _amount,
    isclosed,
    _stage_change_date,
    _current_stage,
    _stage_history,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _6qa_date,
    _engagement,
    _eng_id,
    _eng_timestamp,
    _eng_description,
    _eng_notes,
    _account_tier,
    _channel,
    _is_matched_opp,
    _is_influencing_activity,
    _is_influenced_opp,
    _is_accelerating_activity,
    _is_accelerated_opp,
    _is_later_accelerating_activity,
    _is_later_accelerated_opp,
    _is_stagnant_opp
)
-- Get account engagements of target account 
WITH target_account_engagements AS (
    SELECT DISTINCT 
        -- prev table pull the salesforce account name instead of 6sense
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _6qa_date, 
        _engagement, 
        ROW_NUMBER() OVER() AS _eng_id,
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,
        _account_tier,
        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END AS _channel
    FROM `ridecell.db_6sense_engagement_log` engagement_log
),
-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
closedConversionRate AS (
    SELECT DISTINCT
        opp.id,
        opp.closedate,
        opp.amount AS amount
    FROM `x-marketing.ridecell_salesforce.Opportunity` opp
    WHERE opp.isclosed = TRUE
),
openConversionRate AS (
    SELECT DISTINCT
        opp.id,
        opp.closedate
    FROM `x-marketing.ridecell_salesforce.Opportunity` opp
    WHERE opp.isclosed = FALSE
),
opps_main AS (
    SELECT DISTINCT
        opp.accountid AS _account_id, 
        act.name AS _account_name,
        REGEXP_REPLACE(act.website, r'^(https?://)?www\.(.*?)(?:/|$)', r'\2') AS _domain,
        COALESCE(act.shippingcountry, act.billingcountry) AS _country,
        opp.id AS _opp_id,
        opp.name AS _opp_name,
        own.name AS _opp_owner_name,
        opp.type AS _opp_type,
        DATE(opp.createddate) AS _created_date,
        DATE(opp.closedate) AS _closed_date,
        opp.amount AS _amount,
        opp.isclosed,
        -- For filling up those opps with missing first stage in the opp history
        opp.stagename AS _current_stage,
        DATE(opp.laststagechangedate) AS _stage_change_date
    FROM `ridecell_salesforce.Opportunity` opp
    LEFT JOIN `ridecell_salesforce.Account` act
        ON opp.accountid = act.id 
    LEFT JOIN`ridecell_salesforce.User` own
        ON opp.ownerid = own.id 
    WHERE opp.isdeleted = FALSE
),
opps_created AS (
    SELECT DISTINCT
        opps_main.*
    FROM opps_main
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
            oldvalue__st AS _previous_stage,
            newvalue__st AS _next_stage
        FROM `ridecell_salesforce.OpportunityFieldHistory` 
        WHERE field = 'StageName'
            AND isdeleted = FALSE
    ) main
    JOIN (
        SELECT DISTINCT 
            opportunityid AS _opp_id,
            createddate AS _historical_stage_change_timestamp,
            oldvalue AS _previous_stage_prob,
            newvalue AS _next_stage_prob,
        FROM `ridecell_salesforce.OpportunityFieldHistory`
        WHERE field = 'Probability_to_Close__c'
        AND isdeleted = FALSE
    ) side
    USING (_opp_id, _historical_stage_change_timestamp)
),
-- There are several stages that can occur on the same day
-- Get unique stage on each day 
unique_opps_historical_stage AS (
    SELECT
        *,
        -- Setting the rank of the historical stage based on stage change date
        ROW_NUMBER() OVER(PARTITION BY _opp_id ORDER BY _historical_stage_change_date DESC) AS _stage_rank
    FROM opps_historical_stage
    -- Those on same day are differentiated by timestamp
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _opp_id, _historical_stage_change_date ORDER BY _historical_stage_change_timestamp DESC) = 1
),
-- Generate a log to store stage history from latest to earliest
get_aggregated_stage_history_text AS (
    SELECT
        *,
        STRING_AGG( 
            CONCAT(
                '[ ', _historical_stage_change_date, ' ]',
                ' : ', _next_stage),'; ') 
        OVER (PARTITION BY _opp_id ORDER BY _stage_rank) AS _stage_history
    FROM unique_opps_historical_stage
),
-- Obtain the current stage and the stage date in this CTE 
get_current_stage_and_date AS (
    SELECT
        *,
        CASE 
            WHEN _stage_rank = 1 THEN _historical_stage_change_date
        END AS _stage_change_date,
        CASE 
            WHEN _stage_rank = 1 THEN _next_stage
        END AS _current_stage
    FROM get_aggregated_stage_history_text
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
            main._stage_change_date,
            MAX(side._stage_change_date) OVER (PARTITION BY side._opp_id),
            main._created_date) AS _stage_change_date,
        COALESCE(
            MAX(side._current_stage) OVER (PARTITION BY side._opp_id),
            main._current_stage) AS _current_stage,

        -- Set the stage history to aid crosscheck
        MAX(side._stage_history) OVER (PARTITION BY side._opp_id) AS _stage_history,

        -- The stage and date fields here represent those of each historical stage
        -- Will be different in each row of an opp
        side._historical_stage_change_date,
        side._next_stage AS _historical_stage,

        -- Set the stage movement 
        CASE
            WHEN side._previous_stage_prob > side._next_stage_prob THEN 'Downward' 
            ELSE 'Upward'
        END AS _stage_movement
    FROM opps_created main
    LEFT JOIN get_current_stage_and_date side
        ON main._opp_id = side._opp_id
),
-- Tie opportunities with stage history and account engagements
combined_data AS (
SELECT
    opp.*,
    act.*,
    CASE
        WHEN act._engagement IS NOT NULL THEN true 
    END AS _is_matched_opp
FROM opps_history opp
LEFT JOIN target_account_engagements act     
    ON (opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
        -- opp._domain = act._6sensedomain
    AND LENGTH(opp._domain) > 1
    AND LENGTH(act._6sensedomain) > 1)

    OR (opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
            -- opp._domain = act._6sensedomain
        AND LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
        AND LENGTH(opp._account_name) > 1
        AND LENGTH(act._6sensecompanyname) > 1)

    OR (LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
        AND LENGTH(opp._account_name) > 1
        AND LENGTH(act._6sensecompanyname) > 1)
),
-- Label the activty that influenced the opportunity
set_influencing_activity AS (
    SELECT
        *,
        CASE 
            WHEN DATE(_eng_timestamp) BETWEEN DATE_SUB(_created_date, INTERVAL 90 DAY) AND DATE(_created_date) THEN true 
        END AS _is_influencing_activity
    FROM combined_data
),
-- Mark every other rows of the opportunity as influenced 
-- If there is at least one influencing activity
label_influenced_opportunity AS (
    SELECT
        *,
        MAX(_is_influencing_activity) OVER(PARTITION BY _opp_id)AS _is_influenced_opp
    FROM set_influencing_activity
),
-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NULL AND _eng_timestamp > _created_date AND _eng_timestamp <= _historical_stage_change_date AND _stage_movement = 'Upward'
            -- AND 
            --     REGEXP_CONTAINS(
            --         _engagement, 
            --         '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
            --     )
            THEN true
        END AS _is_accelerating_activity
    FROM label_influenced_opportunity
),
-- Mark every other rows of the opportunity as accelerated 
-- If there is at least one accelerating activity
label_accelerated_opportunity AS (
    SELECT
        *,
        MAX(_is_accelerating_activity) OVER (PARTITION BY _opp_id) AS _is_accelerated_opp
    FROM set_accelerating_activity
),
-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NOT NULL AND _eng_timestamp > _created_date AND _eng_timestamp <= _historical_stage_change_date AND _stage_movement = 'Upward'
            -- AND 
            --     REGEXP_CONTAINS(
            --         _engagement, 
            --         '6sense Campaign|6sense Ad|6sense Form|LinkedIn Campaign|LinkedIn Ad'
            --     )
            THEN true
        END AS _is_later_accelerating_activity
    FROM label_accelerated_opportunity
),
-- Mark every other rows of the opportunity as infuenced cum accelerated 
-- If there is at least one accelerating activity for the incluenced opp
label_influenced_opportunity_that_continue_to_accelerate AS ( 
    SELECT
        *,
        MAX(_is_later_accelerating_activity) OVER(PARTITION BY _opp_id) AS _is_later_accelerated_opp
    FROM set_accelerating_activity_for_influenced_opportunity
),
-- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
label_stagnant_opportunity AS (
    SELECT
        *,
        CASE
            WHEN _is_matched_opp = true 
            AND _is_influenced_opp IS NULL 
            AND _is_accelerated_opp IS NULL 
            AND _is_later_accelerated_opp IS NULL
            THEN true 
        END AS _is_stagnant_opp
    FROM label_influenced_opportunity_that_continue_to_accelerate
),
-- Get the latest stage of each opportunity 
-- While carrying forward all its boolean fields' value caused by its historical changes 
latest_stage_opportunity_only AS (
    SELECT DISTINCT
        -- Remove fields that are unique for each historical stage of opp
        * EXCEPT(_historical_stage_change_date, _historical_stage, _stage_movement),
        -- For removing those with values in the activity boolean fields
        -- Different historical stages may have caused the influencing or accelerating
        -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
    FROM label_stagnant_opportunity
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _opp_id, _eng_id ORDER BY _is_influencing_activity DESC, _is_accelerating_activity DESC, _is_later_accelerating_activity DESC) = 1 
)
SELECT
  *
FROM latest_stage_opportunity_only;