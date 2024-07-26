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

    -- SELECT 
    --   * EXCEPT(rownum, _extract_date)
    -- FROM (  
    --   SELECT 
    --     *,
    --     ROW_NUMBER() OVER(
    --       PARTITION BY _domain, _year, _week
    --       ORDER BY _extract_date DESC
    --     ) rownum
    --   FROM (
        SELECT
           _domain,
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
      -- )
    -- )
    -- WHERE 
    --   rownum = 1
    -- ORDER 
    --   BY _domain, _year, _week

  ),
  account_engagement AS (
  SELECT eng.* ,
  _t90_days_score
  FROM 
  (

     SELECT eng._domain, 
      _sfdcaccountid AS accountid,
      _company,
      _engagement, 
      _email, 
      _timestamp, 
      _jobtitle, 
      _seniority, 
      _contentTitle,
      _description,
      EXTRACT(WEEK FROM _timestamp) AS _week,
          EXTRACT(YEAR FROM _timestamp) AS _year,
      MD5(CONCAT(eng._domain, _engagement, _timestamp)) AS _engagementID,
      --_latest_account_score AS _t90_days_score 
    FROM 
      `3x.db_consolidated_engagements_log` eng
    WHERE
      _engagement NOT IN ('Opportunity Created', 'Opportunity Stage Change','Bombora Report') 
    
      AND _engagement IS NOT NULL
      AND _description <> '0 (Cumulative)' 
      ---AND _domain = 'samba.tv'
    ORDER BY
      _timestamp DESC
      ) eng
      LEFT JOIN 
     account_score ON ( eng._domain = account_score._domain AND DATE(eng._timestamp) = account_score._extract_date )

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
    
    LEFT JOIN
    account_info ON main._account_id = account_info._accountid
   WHERE 
  (_previousStage IS NULL  ) OR (_previousStage <>current_stage ) 
  ---AND  
   --_opportunity_id = '006UW0000019f51YAA'
  )
  ,
  combined_data AS (
    
    SELECT
      *,
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
      ---MD5(CONCAT(CAST(_opportunity_id AS BYTES), _engagementID)) AS _uniqueID,
      ROW_NUMBER()  OVER(PARTITION BY _opportunity_id ORDER BY  _engagementDate DESC) AS rownum, 
      DATE_DIFF(DATE(_opportunityCreated) ,DATE(_engagementDate),DAY) AS _date_diff
    FROM 
    ( SELECT *, 
     CASE
          WHEN (
            ( 
              _leadsource LIKE 
                '%Marketing:%' 
              AND 
              ( NOT REGEXP_CONTAINS(_current_stage, '0|1') OR _current_stage IS NULL )
            )
          )
          AND _opportunityCreated >= '2023-01-16'     
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
            DATE(_engagementDate) <= DATE(_opportunityCreated) 
            AND DATE_DIFF(DATE(_opportunityCreated) ,DATE(_engagementDate),DAY)  <= 90
            AND _domain <> "gmail.com"
            AND 
            _leadsource <> 'Existing Client'
            AND _engagement <> "Bombora Report"
        --  CASE 
        --   WHEN 
        --     _t90_days_score >= 15
        --     AND 
        --     NOT REGEXP_CONTAINS(_leadsource, 'Marketing:')
        --     AND
        --     DATE(_engagementDate) <= DATE(_opportunityCreated) 
          THEN 1 
          ELSE 0 
        END
        AS _isInfluence, 
    FROM 
    (
      SELECT
        DISTINCT opps_created._opportunity_id,
        opps_created._account_id,
        COALESCE(account_engagement._domain,opps_created._domain) AS _domain,
        opps_created._opportunity_name,
        _leadsource,
        account_engagement._engagementID,
        opps_created._createdate AS _opportunitycreate,
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
        account_engagement._timestamp AS _engagementDates,
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
       
        

      FROM 
        opps_created
      LEFT JOIN
        account_engagement ON ((opps_created._domain = account_engagement._domain AND ((EXTRACT(WEEK FROM opps_created._last_stage_change_date) = account_engagement._week AND EXTRACT(YEAR FROM opps_created._last_stage_change_date) = account_engagement._year) )  ))
        OR 
        ((opps_created._domain = account_engagement._domain AND ((EXTRACT(WEEK FROM opps_created._createdate) = account_engagement._week AND EXTRACT(YEAR FROM opps_created._createdate) = account_engagement._year) )  ))
        OR 
        ((opps_created._domain = account_engagement._domain AND ( EXTRACT(YEAR FROM opps_created._createdate) = account_engagement._year) )  )
      -- LEFT JOIN
      -- account_score 
      --   ON 
      --      ((EXTRACT(WEEK FROM opps_created._last_stage_change_date) = account_score._week AND EXTRACT(YEAR FROM opps_created._last_stage_change_date) = account_score._year) 
      --     AND opps_created._domain = account_score._domain)
      --     OR 
      --     ((EXTRACT(WEEK FROM opps_created._createdate) = account_score._week AND EXTRACT(YEAR FROM opps_created._createdate) = account_score._year) 
      --     AND opps_created._domain = account_score._domain)
      --      OR 
      --   ((opps_created._domain = account_score._domain AND ( EXTRACT(YEAR FROM opps_created._createdate) = account_score._year) )  )
      WHERE
        (LOWER(_account_name) NOT LIKE '%3x%' OR _account_name IS NULL)
    )
    )
    --WHERE _opportunity_id = '0064P000010YMe6QAG'
  ) 
,
  opp_influenced AS (
  SELECT
      *
    FROM 
      combined_data
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
          FROM (  
  SELECT 
          *
          --_opportunity_id, 
          --_account_id, 
          -- MIN(rownum) AS rownum 
        FROM 
          combined_data
        WHERE 
        (_isInfluence = 1 ) 
        ) GROUP BY 1, 2
        ) USING(_opportunity_id, _account_id, rownum)
), 
  opp_generated AS (
  SELECT
      *
    FROM 
      combined_data
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
          FROM (  
  SELECT 
          *
          --_opportunity_id, 
          --_account_id, 
          -- MIN(rownum) AS rownum 
        FROM 
          combined_data
        WHERE 
        (_isGenerate = 1)
        ) GROUP BY 1, 2
        ) USING(_opportunity_id, _account_id, rownum)
),
  opp_accelerated AS (
SELECT
      *
    FROM 
      combined_data
    JOIN
      ( SELECT 
          DISTINCT _opportunity_id, 
          _account_id, 
          MIN(rownum) AS rownum 
          FROM (  
  SELECT 
          *
          --_opportunity_id, 
          --_account_id, 
          -- MIN(rownum) AS rownum 
        FROM 
          combined_data
        WHERE 
         (_isAccelerate = 1 ) 
        ) GROUP BY 1, 2
        ) USING(_opportunity_id, _account_id, rownum)
        WHERE 
      (_isAccelerate = 1) 
      AND _opportunity_id NOT IN (SELECT DISTINCT _opportunity_id FROM opp_influenced)
)
,opp_others AS (

    SELECT
      DISTINCT 
       _opportunity_id,
      _account_id,
      rownum,
      _domain,
      _opportunity_name,
      _leadsource,
      _engagementID,
      _opportunitycreate,
      _opportunityCreated,
      _opportunityWon,
      _opportunityLost,
      _opportunityValue,
      _total_one_time,
      _current_stage,
      _previous_stage,
      _stageMovement,
      _oppLastChangeinStage,
      _engagementDates,
      _engagementDate,
      _email,
      _jobtitle,
      _account_name,
      _engagement,
      _engagement_activities,
      _description,
      _t90_days_score,
      _type,
      _max_amount,
      _isGenerate,
      _isInfluence,
      _isAccelerate,
      --_max_amount,
      --_uniqueID,
       _date_diff
    FROM 
      combined_data
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
  ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY _engagementDate DESC, _isInfluence DESC) AS rownum
FROM
  (
    SELECT * FROM (
    SELECT * FROM opp_generated
   UNION DISTINCT
    SELECT * FROM opp_influenced
    UNION DISTINCT
     SELECT * FROM opp_accelerated
     UNION DISTINCT
     SELECT * FROM opp_others
    ) 

)
--WHERE _opportunity_id = '0064P000011R0q8QAC'

 
  