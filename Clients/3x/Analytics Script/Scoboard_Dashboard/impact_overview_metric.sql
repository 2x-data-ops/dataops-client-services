
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------- Pipeline Metrics Overview ------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `x-marketing.3x.overview_engagement_opportunity` AS
WITH 
  account_info AS (

    SELECT * EXCEPT( _rownum) 
    FROM (
      SELECT 
          CAST(vid AS STRING) AS _id,
          property_email.value AS _email,
          COALESCE(
              RIGHT(associated_company.properties.domain.value, LENGTH(associated_company.properties.domain.value) - STRPOS(associated_company.properties.domain.value, "www.")),
              RIGHT(property_hs_email_domain.value, LENGTH(property_hs_email_domain.value) - STRPOS(property_hs_email_domain.value, "@"))
          ) AS _domain,
          property_salesforceaccountid.value AS _accountid,
          property_company.value AS _company,
          ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value) ORDER BY vid DESC) AS _rownum
        FROM 
          `x-marketing.x3x_hubspot.contacts` k
        WHERE 
          LENGTH(property_email.value) > 2
          AND property_email.value NOT LIKE '%2x.marketing%'
        ) 
    WHERE _rownum = 1

  ),
  account_score AS (

    SELECT 
      * EXCEPT(rownum, _extract_date)
    FROM (  
      SELECT 
        *,
        ROW_NUMBER() OVER(
          PARTITION BY _domain, _year, _week
          ORDER BY _extract_date DESC
        ) rownum
      FROM (
        SELECT
          DISTINCT _domain,
          _extract_date,
          EXTRACT(WEEK FROM _extract_date) AS _week,
          EXTRACT(YEAR FROM _extract_date) AS _year,
            (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_paid_ads_score , 0)+ COALESCE(_quarterly_web_score, 0) + COALESCE(_quarterly_6sense_score, 0) + COALESCE(_quarterly_event_score,0) + COALESCE(_quarterly_webinar_score,0) ) AS  _t90_days_score
    FROM 
      x-marketing.3x.account_90days_score_new
        --   (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_ads_score , 0)+ COALESCE(_quarterly_web_score, 0)) AS _t90_days_score
        -- FROM
        --   `3x.account_90days_score`  
        ORDER BY
          _domain DESC, _extract_date DESC
      )
    )
    WHERE 
      rownum = 1
    ORDER 
      BY _domain, _year, _week

  ),
  account_engagement AS (

    SELECT 
      DISTINCT eng._domain, 
      _sfdcaccountid AS accountid,
      _company,
      _engagement, 
      _email, 
      _timestamp, 
      _jobtitle, 
      _seniority, 
      _contentTitle,
      _description,
      MD5(CONCAT(eng._domain, _engagement, _timestamp, _contentTitle, _email)) AS _engagementID
    FROM 
      `3x.db_consolidated_engagements_log` eng
    WHERE
      _engagement NOT IN ('Opportunity Created', 'Opportunity Stage Change') 
      AND _engagement IS NOT NULL
    ORDER BY
      _timestamp DESC

  )
  ,
  opps_created AS (

    SELECT
      DISTINCT _opportunity_id, 
      _account_id,
      _account_name,
      _opportunity_name, 
      current_stage  AS _current_stage,
      _createdate,
      _close_date,
      _amount,
      _acv,
      COALESCE(main._domain, account_info._domain) AS _domain,
      _type,
      _leadsource,
      _lost_reason,
      _current_stage_change_date	 AS _last_stage_change_date,
      _previousStage   AS _previous_stage,
      _days_current_stage,
      _total_one_time,
      _max_amount,
      _orderstage_previous,
      _orderstage_current_from_previousstage
    FROM
      `x-marketing.3x.db_opportunity_log` main
    --WHERE _opportunity_id = '0064P000010Y8V1QAK'
    LEFT JOIN
    account_info ON main._account_id = account_info._accountid
    --WHERE current
  )
  ,
  combined_data AS (
    
    SELECT
      *
    FROM (
      SELECT
        DISTINCT opps_created._opportunity_id,
        opps_created._account_id,
        account_engagement._domain,
        opps_created._opportunity_name,
        _leadsource,
        account_engagement._engagementID,
        DATE(opps_created._createdate) AS _opportunityCreated,
        IF(_current_stage = 'Closed Won', DATE(opps_created._close_date), NULL) AS _opportunityWon,
        IF(_current_stage = 'Closed Lost', DATE(opps_created._close_date), NULL) AS _opportunityLost,
        COALESCE(_acv, _amount) AS _opportunityValue,
        _total_one_time,
        _current_stage,
        _previous_stage,
        -- _daysCurrentStage,
        CASE
         WHEN  _orderstage_previous	 > _orderstage_current_from_previousstage THEN 'Downward' 
          ELSE 'Upward'
        END AS _stageMovement,
        _last_stage_change_date AS _oppLastChangeinStage,
        DATE(account_engagement._timestamp) AS _engagementDate,
        _email,
        _jobtitle,
        _account_name,
        _engagement,
        _contentTitle AS _engagement_activities,
        _description,
        _t90_days_score,
        -- _ytd_first_party_score,
        opps_created._type,
        _max_amount,

        -- Label for generated opps
        CASE
          WHEN (
            ( 
              _leadsource LIKE 
                '%Marketing:%' 
              AND 
              ( NOT REGEXP_CONTAINS(_current_stage, '0|1') OR _current_stage IS NULL )
            )
          )
          AND _createdate >= '2023-01-16'     
          THEN 1 
          ELSE 0 
        END  
        AS _isGenerate,

        -- Label for influenced opps
        CASE 
          WHEN 
            _t90_days_score >= 15
            AND 
            NOT REGEXP_CONTAINS(_leadsource, 'Marketing:')
            AND
            DATE(account_engagement._timestamp) <= DATE(opps_created._createdate) 
          THEN 1 
          ELSE 0 
        END
        AS _isInfluence,
        

      FROM 
        opps_created
      LEFT JOIN
        account_engagement USING(_domain)
      LEFT JOIN
      account_score 
        ON 
          (EXTRACT(WEEK FROM opps_created._createdate) = account_score._week AND EXTRACT(YEAR FROM opps_created._createdate) = account_score._year) 
          AND opps_created._domain = account_score._domain
      WHERE
        (LOWER(_account_name) NOT LIKE '%3x%' OR _account_name IS NULL)
    )
     ),
  get_accelerated AS (

    SELECT 
      *,

      -- Label for accelerated opps
      CASE 
        WHEN 
          _isInfluence = 0 
          AND
          _t90_days_score >= 15
          AND 
          NOT REGEXP_CONTAINS(_leadsource, 'Marketing:')
          AND 
          _current_stage NOT LIKE '%Nurture%' 
          AND 
          ( _engagementDate >= _opportunityCreated AND TIMESTAMP(_engagementDate) <= _oppLastChangeinStage )
          AND 
          _opportunityLost IS NULL 
          AND _stageMovement  = 'Upward'
          -- AND 
          -- _opportunityWon IS NULL 
        THEN 1 
        ELSE 0 
      END 
      AS _isAccelerate,

      MD5(CONCAT(CAST(_opportunity_id AS BYTES), _engagementID)) AS _uniqueID,
      ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY _engagementDate DESC) AS rownum
    FROM
        combined_data
    ORDER BY 
      _isAccelerate DESC, _opportunity_name

  ), 
  opp_generated AS (

    SELECT
      *
    FROM 
      get_accelerated
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
        FROM 
          get_accelerated 
        WHERE 
          (_isGenerate = 1 ) 
        GROUP BY 1, 2
      ) USING(_opportunity_id, _account_id, rownum)
    WHERE 
      (_isGenerate = 1)

  ),
  opp_influenced AS (

    SELECT
      *
    FROM 
      get_accelerated
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
        FROM 
          get_accelerated 
        WHERE 
          (_isInfluence = 1 ) 
        GROUP BY 1, 2
      ) USING(_opportunity_id, _account_id, rownum)
    WHERE 
      (_isInfluence = 1)

  ),
  opp_accelerated AS (

    SELECT
      *
    FROM 
      get_accelerated
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
        FROM 
          get_accelerated 
        WHERE 
          (_isAccelerate = 1 ) 
        GROUP BY 1, 2
      ) USING(_opportunity_id, _account_id, rownum)
    WHERE 
      (_isAccelerate = 1) 
      AND _opportunity_id NOT IN (SELECT DISTINCT _opportunity_id FROM opp_influenced)
      
  ),
  opp_others AS (

    SELECT
      DISTINCT 
      _opportunity_id,
      _account_id,
      rownum,
      _domain,
      _opportunity_name,
      _leadsource,
      _engagementID,
      _opportunityCreated,
      _opportunityWon,
      _opportunityLost,
      _opportunityValue,
      _total_one_time,
      _current_stage,
      _previous_stage,
      _stageMovement,
      _oppLastChangeinStage,
      _engagementDate,
      _email,
      _jobtitle,
      _account_name,
      _engagement,
      _engagement_activities,
      _description,
      _t90_days_score,
      _type,_max_amount,
      _isGenerate,
      _isInfluence,
      _isAccelerate,
      --_max_amount,
      _uniqueID,
    FROM 
      get_accelerated
    WHERE
      _isGenerate = 0
      AND _isInfluence = 0
      AND _isAccelerate = 0
      AND _opportunity_id NOT IN (
        SELECT DISTINCT _opportunity_id FROM opp_generated 
        UNION DISTINCT
        SELECT DISTINCT _opportunity_id FROM opp_influenced 
        UNION DISTINCT 
        SELECT DISTINCT _opportunity_id FROM opp_accelerated
      )
  )

  SELECT 
  * EXCEPT(rownum),
  ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY _engagementDate DESC) AS rownum
FROM
  (
    SELECT * FROM opp_generated
    UNION DISTINCT
    SELECT * FROM opp_influenced
    UNION DISTINCT
    SELECT * FROM opp_accelerated
    UNION DISTINCT
    SELECT * FROM opp_others
)
    ORDER BY 
      _isInfluence DESC


;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------- Account Influence Script ------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE `x-marketing.3x.overview_account_opportunity`;
INSERT INTO `x-marketing.3x.overview_account_opportunity`
-- CREATE OR REPLACE TABLE `x-marketing.3x.overview_account_opportunity` AS 
WITH 
account_info AS (
  SELECT * EXCEPT( _rownum) 
  FROM (
      SELECT 
        CAST(vid AS STRING) AS _id,
        property_email.value AS _email,
        COALESCE(
            RIGHT(property_hs_email_domain.value, LENGTH(property_hs_email_domain.value) - STRPOS(property_hs_email_domain.value, "@")),
            RIGHT(associated_company.properties.domain.value, LENGTH(associated_company.properties.domain.value) - STRPOS(associated_company.properties.domain.value, "www."))
        ) AS _domain,
        property_company.value AS _company,
        ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value) ORDER BY vid DESC) AS _rownum
      FROM 
        `x-marketing.x3x_hubspot.contacts` k
      WHERE 
        LENGTH(property_email.value) > 2
        AND property_email.value NOT LIKE '%2x.marketing%'
        AND property_email.value NOT LIKE '%3x%'
      ) 
  WHERE _rownum = 1
),
pivot_engagement AS (
  SELECT 
    DISTINCT score.*,
    IF(_engagement_score > 14, 'High', 
        IF(_engagement_score BETWEEN 1 AND 14, 'Low', 
          'No') ) AS _quarterly_engagement_cluster,
    /* IF(target_account__c = true, 1, 0) */ 0 AS _target_account,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _extract_date DESC) AS _order
  FROM
    ( 
      SELECT 
        DISTINCT *, 
      --   (COALESCE(_quarterly_ads_score, 0) + COALESCE(_quarterly_email_score , 0) + COALESCE(_quarterly_web_score , 0) + COALESCE(_quarterly_content_synd_score , 0) + COALESCE(_quarterly_organic_social_score , 0) + COALESCE(_quarterly_form_fill_score, 0) + COALESCE(_web_eng_trend_score, 0) + COALESCE(_6sense_click_score, 0) ) AS _engagement_score 
      -- FROM 
      --   `3x.account_90days_score` ) score
        (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score , 0)+ COALESCE(_quarterly_organic_social_score , 0)+ COALESCE(_quarterly_form_fill_score , 0)+ COALESCE(_quarterly_paid_ads_score , 0)+ COALESCE(_quarterly_web_score, 0) + COALESCE(_quarterly_6sense_score, 0) + COALESCE(_quarterly_event_score,0) + COALESCE(_quarterly_webinar_score,0) ) AS _engagement_score 
    FROM 
      x-marketing.3x.account_90days_score_new ) score
      
  LEFT JOIN
    account_info USING(_domain)
),
influenced_opps AS (
  SELECT
    DISTINCT 
    _opportunity_id AS id, 
    _opportunityCreated, 
    _isInfluence
  FROM
    `3x.overview_engagement_opportunity`
  WHERE
    _isInfluence = 1
),
opps_created AS (
  SELECT
    CAST(NULL AS STRING) /* main.id */ AS _opportunityID, 
    CAST(NULL AS STRING) /* accountid */ AS _accountid,
    CAST(NULL AS STRING) /* accountname */ AS _accountname,
    CAST(NULL AS STRING) /* main.name */ AS _opportunityName, 
    CAST(NULL AS STRING)  /* stagename */ AS _currentStage,
    CAST(NULL AS TIMESTAMP) /* main.createddate */ AS _createTS,
    CAST(NULL AS TIMESTAMP)  /* closedate */ AS _closeTS,
    CAST(NULL AS INT64) /* amount */ AS _amount,
    CAST(NULL AS INTEGER) AS _acv,
    CAST(NULL AS STRING) AS _domain,
    CAST(NULL AS STRING) /* main.type */ AS _type,
    CAST(NULL AS STRING)  /* win_loss_reasons__c */ AS _reason,
    CAST(NULL AS STRING)  /* laststagechangedate */ AS _oppLastChangeinStage,
    /* previous_stage__c AS _previousStage,
    days_in_stage__c AS _daysCurrentStage */
    CAST(NULL AS INT64) AS _isinfluence
  /* FROM
    `3x_salesforce.Opportunity` main
  JOIN
    account_info USING(accountid)
  LEFT JOIN
    influenced_opps USING(id)
  WHERE
    main.isdeleted = False
    AND main.type !='Renewal'
    AND LOWER(accountname) NOT LIKE '%3x%'
    AND EXTRACT(YEAR FROM main.createddate ) IN (2022, 2023) */
),
pivoted_opps AS (
  SELECT
    DISTINCT _domain,
    _accountid,
    COUNT(DISTINCT _opportunityID) AS _opps_created,
    SUM(_amount) AS _total_opps_value,
    SUM(IF(_currentStage NOT LIKE '%Closed%', _amount, 0)) AS _total_active_opps,
    SUM(IF(_currentStage NOT LIKE '%Closed%' AND _isInfluence = 1, _amount, 0)) AS _total_active_influenced_opps,
    SUM(IF(_currentStage LIKE '%Closed%', _amount, 0)) AS _total_closed_opps,
    SUM(IF(_currentStage LIKE '%Closed%' AND _isInfluence = 1, _amount, 0)) AS _total_closed_influenced_opps,
  FROM
    opps_created
  GROUP BY
    1, 2
),
combined_data AS (
  SELECT
    *
  FROM 
    pivot_engagement
  LEFT JOIN
    pivoted_opps USING(_domain)
  WHERE
    LOWER(_domain) NOT LIKE '%3x%'
    AND _order = 1
)
SELECT 
  DISTINCT * EXCEPT(_order)
FROM 
  combined_data
ORDER BY
  _extract_date DESC
;