CREATE OR REPLACE TABLE `x-marketing.carenet_health.opportunity_influenced_accelerated` AS
WITH account AS (
  SELECT
    act.account_id_18_digit__c AS _account_id,
    act.name AS _account_name,
    LOWER(REGEXP_EXTRACT(
      REGEXP_REPLACE(LOWER(act.website), r'[https|http]*.\/\/|www.',''),
      r'[\w\-]*\.[\w\-]{2,3}'
    )) AS _domain,
    act.website,
    COALESCE(act.shippingcountry, act.billingcountry) AS _country,
  FROM `x-marketing.carenet_health_salesforce.Account` act
  WHERE 
  REGEXP_EXTRACT(
    REGEXP_REPLACE(LOWER(act.website), r'[https|http]*.\/\/|www.',''),
    r'[\w\-]*\.[\w\-]{2,3}'
  ) NOT IN (
    'facebook.com','google.com','amazon.com','army.mil','navy.mil','gmail.com'
  )
),
opportunity AS (
  SELECT
    opp.account_name_text__c AS _account_name,
    opp.id AS _opp_id,
    opp.accountid AS _account_id,
    opp.name AS _opp_name,
    opp.stagename,
    opp.amount AS _amount,
    DATE(opp.createddate) AS _created_date,
    DATE(opp.closedate) AS _closed_date,
    opp.type AS _opp_type,
    opp.ownerid AS _owner_id,
    days_in_stage__c,
    DATE(opp.laststagechangedate) AS _stage_change_date
  FROM `x-marketing.carenet_health_salesforce.Opportunity` opp
),
form_fill_data AS (
  SELECT 
    form._form_title AS _form_title,
    DATE(form._timestamp) AS _date,
    form.company_name AS _company_name,
    form.accociated_company_name AS _associated_company_name,
    LOWER(form._domain) AS _domain,
    form.salesforceaccountid AS _account_id,
    form._engagement
  FROM `x-marketing.carenet_health.db_form_fill_log` form
),
SEM AS (
  SELECT
    PARSE_DATE('%m/%d/%Y', SEM._date) AS _date,
    SEM._campaign,
    SEM._6sensecompanyname,
    SEM._6sensecountry,
    LOWER(SEM._6sensedomain) AS _domain,
  FROM `x-marketing.carenet_health_mysql.carenet_db_sem_engagement_tracking` SEM
),
campaign_tracking AS (
  SELECT
    PARSE_DATE('%m/%d/%Y', TRC._date) AS _date,
    TRC._campaign,
    TRC._account,
    LOWER(TRC._6sensedomain) AS _domain,
    "6sense Campaign Reached" AS _engagement,
    CONCAT(_campaign," ",_impressions) AS _description
  FROM `x-marketing.carenet_health_mysql.carenet_db_6sense_campaign_tracking` TRC
  WHERE CAST(REPLACE(TRC._impressions, ',', '') AS INT64) > 0
  UNION ALL
  SELECT
    PARSE_DATE('%m/%d/%Y', TRC._date) AS _date,
    TRC._campaign,
    TRC._account,
    LOWER(TRC._6sensedomain) AS _domain,
    "6sense Ad Clicked" AS _engagement,
    CONCAT(_campaign," ",_clicks) AS _description
  FROM `x-marketing.carenet_health_mysql.carenet_db_6sense_campaign_tracking` TRC
  WHERE CAST(REPLACE(TRC._clicks, ',', '') AS INT64) > 0  
),
engagement_data AS (
  SELECT DISTINCT
    _account_id,
    _domain,
    _company_name AS _sf_account_name,
    CAST(NULL AS STRING) AS _6sensecompanyname,
    _associated_company_name,
    _date AS _eng_timestamp , 
    _form_title AS _description,
    _engagement,
    CAST(NULL AS STRING) AS _country
  FROM form_fill_data
  UNION ALL
  SELECT DISTINCT
    CAST(NULL AS STRING) AS _account_id,
    _domain,
    CAST(NULL AS STRING) AS _sf_account_name,
    _6sensecompanyName AS _6sensecompanyname,
    CAST(NULL AS STRING) AS _associated_company_name,
    _date AS _eng_timestamp ,
    _campaign AS _description,
    'SEM' AS _engagement,
    _6sensecountry AS _country
  FROM SEM
  UNION ALL
  SELECT DISTINCT
    CAST(NULL AS STRING) AS _account_id,
    _domain,
    CAST(NULL AS STRING) AS _sf_account_name,
    _account AS _6sensecompanyname,
    CAST(NULL AS STRING) AS _associated_company_name,
    _date AS _eng_timestamp ,
    _description AS _description,
    _engagement AS _engagement,
    CAST(NULL AS STRING) AS _country
  FROM campaign_tracking
),
target_account_engagements AS (
  SELECT
    engagement_data._eng_timestamp ,
    engagement_data._engagement,
    engagement_data._description,
    COALESCE(engagement_data._sf_account_name,account._account_name) AS _sf_account_name,
    engagement_data._6sensecompanyname,
    engagement_data._associated_company_name,
    engagement_data._country AS _country_account,
    engagement_data._domain,
    COALESCE(engagement_data._account_id,account._account_id) AS _account_id,
    -- account._account_name,
    account.website AS _sf_Website,
    ROW_NUMBER() OVER() AS _eng_id,
  FROM engagement_data
  LEFT JOIN account ON (
    engagement_data._account_id = account._account_id
    OR
    engagement_data._domain = account._domain
  )
),
opps_created AS (
  SELECT DISTINCT
    opp._account_id,
    COALESCE(act._account_name,opp._account_name) AS _account_name,
    act._domain,
    act._country,
    opp._opp_id,
    opp._opp_name,
    own.name AS _opp_owner_name,
    opp._opp_type,
    opp._created_date,
    opp._closed_date,
    opp._amount,
    opp.stagename,
    days_in_stage__c,
    _stage_change_date
  FROM opportunity opp
  LEFT JOIN account act USING (_account_id)
  LEFT JOIN `carenet_health_salesforce.User` own ON opp._owner_id = own.id 
),
opps_history AS (
  SELECT
    stage._previousstage,
    stage._currentstage,
    stage._historical_stage_change_timestamp,
    stage._historical_stage_change_date,
    opps_created._account_id,
    opps_created._account_name,
    opps_created._domain,
    opps_created._country,
    opps_created._opp_id,
    opps_created._opp_name,
    opps_created._opp_owner_name,
    opps_created._opp_type,
    opps_created._created_date,
    opps_created._closed_date,
    opps_created._amount,
    opps_created.stagename,
    opps_created.days_in_stage__c,
    opps_created._stage_change_date,
    ROW_NUMBER() OVER(PARTITION BY _opp_id ORDER BY _historical_stage_change_timestamp DESC) AS _order
  FROM (
    SELECT
      DISTINCT opportunityid AS _opp_id,
      -- createddate AS _oppLastChangeinStage,
      oldvalue AS _previousstage,
      --probability,
      newvalue AS _currentstage,
      createddate AS _historical_stage_change_timestamp,
      DATE(createddate) AS _historical_stage_change_date,
    FROM
      `x-marketing.carenet_health_salesforce.OpportunityFieldHistory`
    WHERE 
      field = 'StageName' 
      AND isdeleted IS FALSE
  ) stage
  RIGHT JOIN  opps_created USING(_opp_id)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _opp_id ORDER BY _historical_stage_change_timestamp DESC) = 1
),
combined_data AS (
  SELECT
    opp.*,
    eng.* EXCEPT (_account_id,_domain, _sf_account_name),
    COALESCE(eng._sf_account_name,_account_name) AS _sf_account_name,
    IF(eng._engagement IS NOT NULL, TRUE, FALSE) AS _is_matched_opp
  FROM opps_history opp 
  LEFT JOIN 
    target_account_engagements AS eng USING (_account_id)
),
-- Label the activty that influenced the opportunity
set_influencing_activity AS (
  SELECT
    *,
    IF(
      DATE(_eng_timestamp) 
      BETWEEN DATE_SUB(_created_date, INTERVAL 9 MONTH) 
      AND DATE(_created_date),
      true,
      false
    ) AS _is_influencing_activity
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
    IF(
      _is_influenced_opp = false 
      AND DATE(_eng_timestamp) > DATE(_created_date)
      AND DATE(_stage_change_date) > DATE(_eng_timestamp),
      true,
      false
    ) AS _is_accelerating_activity
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
    ) AS _is_accelerated_opp
  FROM 
    set_accelerating_activity
),
-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (
  SELECT 
    *,
    IF(
      _is_influenced_opp = true
      AND CAST(_eng_timestamp AS DATE) > _created_date
      AND _eng_timestamp <= CAST(_historical_stage_change_timestamp AS DATE),
      TRUE,
      FALSE
    ) AS _is_later_accelerating_activity
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
    ) AS _is_later_accelerated_opp
  FROM 
    set_accelerating_activity_for_influenced_opportunity
),
-- Mark opportunities that were matched but werent influenced or accelerated or influenced cum accelerated as stagnant 
label_stagnant_opportunity AS (
  SELECT
    *,
    IF(
      _is_matched_opp = true
      AND _is_influenced_opp = false
      AND _is_accelerated_opp = false
      AND _is_later_accelerated_opp = false,
      TRUE,
      FALSE
    ) AS _is_stagnant_opp
  FROM 
    label_influenced_opportunity_that_continue_to_accelerate
),
-- Get the latest stage of each opportunity 
-- While carrying forward all its boolean fields' value caused by its historical changes 
latest_stage_opportunity_only AS (
  SELECT DISTINCT
    -- Remove fields that are unique for each historical stage of opp
    * ,
    -- For removing those with values in the activity boolean fields
    -- Different historical stages may have caused the influencing or accelerating
    -- This is unlike the opportunity boolean that is uniform among the all historical stage of opp 
  FROM 
    label_stagnant_opportunity
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY 
      _opp_id,
      _eng_id
    ORDER BY 
      _is_influencing_activity DESC,
      _is_accelerating_activity DESC,
      _is_later_accelerating_activity DESC
  ) = 1
)

SELECT * FROM latest_stage_opportunity_only;

----------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.carenet_health.opportunity_summarized` AS 
SELECT DISTINCT
  _account_id,
  _account_name,
  _country,
  _domain,
  --_is_targeted_act,
  --_6qa_date,
  _opp_id,
  _opp_name,
  _opp_owner_name,
  _opp_type,
  _created_date,
  _closed_date,
  _amount,
  _stage_change_date  AS _stage_change_date,
  -- _current_stage,
  --_stage_history,
  _previousstage,
  stagename AS _current_stage,
  -- _6sensecountry,
  -- _6sensedomain,
  _is_matched_opp,
  _is_influenced_opp,
  MAX(_is_influencing_activity) OVER(
    PARTITION BY 
      _opp_id
  )
  AS _is_influencing_activity,
  _is_accelerated_opp,
  MAX(_is_accelerating_activity) OVER(
    PARTITION BY 
      _opp_id
  )
  AS _is_accelerating_activity,
  _is_later_accelerated_opp,
  MAX(_is_later_accelerating_activity) OVER(
    PARTITION BY 
      _opp_id
  )
  AS _is_later_accelerating_activity,
  _is_stagnant_opp
FROM 
  `x-marketing.carenet_health.opportunity_influenced_accelerated`;