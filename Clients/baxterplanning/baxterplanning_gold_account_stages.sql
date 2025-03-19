TRUNCATE TABLE `x-marketing.baxterplanning.baxterplanning_gold_account_stages`;
INSERT INTO `x-marketing.baxterplanning.baxterplanning_gold_account_stages`
(
  _acc_id,
  _acc_market_segment,
  _acc_type,
  _som_prospect,
  _pre_pipeline,
  _qualified_pipeline,
  _bookings_won,
  _reached,
  _engaged,
  _meeting_completed
)
-- CREATE OR REPLACE TABLE `x-marketing.baxterplanning.baxterplanning_gold_account_stages` AS
WITH base_data AS (
  SELECT
    _acc_id,
    _acc_created_date,
    _opp_stagename,
    _opp_create_date,
    _opp_close_date,
    closed_won_date,
    _1_development_date,
    _2_validation_date,
    _3_proposal_date,
    _4_negotiation_date,
    _acc_market_segment,
    _acc_type
  FROM `x-marketing.baxterplanning.acc_contact_opps`
  WHERE _acc_market_segment = "SOM" 
    AND (_acc_type = 'Prospect' OR _acc_type = 'Customer')
),
acc_ops AS (
  SELECT
    _acc_id,
    _acc_created_date,
    _acc_market_segment,
    _acc_type,
    MIN (
      CASE
        WHEN _opp_stagename IN ('0 - Pre-Pipeline','1 - Development','2 - Validation','3 - Proposal','4 - Negotiation','Closed Won') 
        AND DATE(_opp_close_date) >= DATE('2024-01-01')
        THEN _opp_create_date
        ELSE NULL
      END
    ) AS _min_pre_pipeline_date,
    MIN (
      CASE 
        WHEN _opp_stagename IN ('1 - Development', '2 - Validation', '3 - Proposal', '4 - Negotiation', 'Closed Won') 
          AND DATE(_opp_close_date) >= DATE('2024-01-01') 
        THEN LEAST(_1_development_date,_2_validation_date,_3_proposal_date,_4_negotiation_date) 
        ELSE NULL 
      END 
    ) AS _min_qualified_pipeline_date,
    MIN (
      CASE
        WHEN _opp_stagename = 'Closed Won' 
        AND DATE(closed_won_date) > DATE('2024-01-01')
        THEN closed_won_date
        ELSE NULL 
      END
    ) AS _min_closed_won_date
  FROM base_data
  GROUP BY _acc_id,_acc_created_date,_acc_market_segment,_acc_type
),
acc_engagement AS (
  SELECT
    _acc_id,
    _acc_market_segment,
    _acc_type,
    MIN (
      CASE
        WHEN 
        (
          LOWER(_engagement_type) LIKE '%click%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%attend%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%participant%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%met with%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%meeting with%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%meeting at%'
          OR
          LOWER(_engagement_type) LIKE '%webinar%' AND LOWER(_engagement_activity) LIKE '%attend%'
          OR
          LOWER(_engagement_type) LIKE '%event%' AND LOWER(_engagement_activity) LIKE '%participant%'
        )
        THEN _engagement_date
        ELSE NULL 
      END
    ) AS _min_engaged_date,
    MIN (
      CASE
        WHEN 
        (   
          (LOWER(_engagement_source) LIKE '%6sense%' AND LOWER(_engagement_type) LIKE '%reached%')
          OR
          (LOWER(_engagement_source) LIKE '%linkedin%' AND LOWER(_engagement_type) LIKE '%reached%')
          OR
          (LOWER(_engagement_source) LIKE '%email%' AND LOWER(_engagement_type) LIKE '%opened%') 
        )
        THEN _engagement_date
        ELSE NULL 
      END
    ) AS _min_reached_date,
    MIN (
      CASE
        WHEN _engagement_type = 'Meeting Conducted' 
        THEN _engagement_date
        ELSE NULL
      END 
    ) AS _min_meeting_completed
  FROM `x-marketing.baxterplanning.acc_contact_activity_consolidation`
  WHERE DATE(_engagement_date) > DATE('2024-01-01') 
    AND _acc_market_segment = "SOM" 
    AND (_acc_type = 'Prospect' OR _acc_type = 'Customer')
  GROUP BY _acc_id,_acc_market_segment,_acc_type
),
stage_date AS (
  SELECT
    acc_ops._acc_id,
    acc_ops._acc_created_date,
    acc_ops._min_pre_pipeline_date,
    acc_ops._min_qualified_pipeline_date,
    acc_ops._min_closed_won_date,
    acc_engagement._min_reached_date,
    acc_engagement._min_engaged_date,
    acc_engagement._min_meeting_completed,
    COALESCE(acc_ops._acc_market_segment,acc_engagement._acc_market_segment) AS _acc_market_segment,
    COALESCE(acc_ops._acc_type,acc_engagement._acc_type) AS _acc_type
  FROM acc_ops
  LEFT JOIN acc_engagement
    ON acc_ops._acc_id = acc_engagement._acc_id
),
unpivoted_data AS (
  SELECT 
    _acc_id, 
    _acc_market_segment,
    _acc_type,
    _stage, 
    _date_entered_stage
  FROM stage_date
  UNPIVOT (_date_entered_stage FOR _stage IN (
    _acc_created_date AS 'SOM Prospect',
    _min_pre_pipeline_date AS 'Pre Pipeline',
    _min_qualified_pipeline_date AS 'Qualified Pipeline',
    _min_closed_won_date AS 'Bookings (Won)',
    _min_reached_date AS 'Reached',
    _min_engaged_date AS 'Engaged',
    _min_meeting_completed AS 'Meeting Completed'
  ))
  WHERE _date_entered_stage IS NOT NULL
)
SELECT 
  _acc_id, 
  _acc_market_segment,
  _acc_type,
  MAX(CASE WHEN _stage = 'SOM Prospect' THEN _date_entered_stage END) AS _som_prospect,
  MAX(CASE WHEN _stage = 'Pre Pipeline' THEN _date_entered_stage END) AS _pre_pipeline,
  MAX(CASE WHEN _stage = 'Qualified Pipeline' THEN _date_entered_stage END) AS _qualified_pipeline,
  MAX(CASE WHEN _stage = 'Bookings (Won)' THEN _date_entered_stage END) AS _bookings_won,
  MAX(CASE WHEN _stage = 'Reached' THEN _date_entered_stage END) AS _reached,
  MAX(CASE WHEN _stage = 'Engaged' THEN _date_entered_stage END) AS _engaged,
  MAX(CASE WHEN _stage = 'Meeting Completed' THEN _date_entered_stage END) AS _meeting_completed
FROM unpivoted_data
GROUP BY _acc_id,_acc_market_segment,_acc_type
ORDER BY _acc_id;