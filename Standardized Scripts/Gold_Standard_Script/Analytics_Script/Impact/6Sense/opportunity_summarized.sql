-- CREATE OR REPLACE TABLE `jellyvision_v2.opportunity_summarized` AS
TRUNCATE TABLE `jellyvision_v2.opportunity_summarized`;
INSERT INTO `jellyvision_v2.opportunity_summarized`(
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
    _is_influencing_activity,
    _is_accelerated_opp,
    _is_accelerating_activity,
    _is_later_accelerated_opp,
    _is_later_accelerating_activity,
    _is_stagnant_opp,
    _channel
)

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
        PARTITION BY _opp_id, _channel
    ) AS _is_influencing_activity,
    _is_accelerated_opp,
    MAX(_is_accelerating_activity) OVER(
        PARTITION BY  _opp_id, _channel
    ) AS _is_accelerating_activity,
    _is_later_accelerated_opp,
    MAX(_is_later_accelerating_activity) OVER(
        PARTITION BY _opp_id, _channel
    ) AS _is_later_accelerating_activity,
    _is_stagnant_opp,
    _channel
FROM `jellyvision_v2.opportunity_influenced_accelerated`;
