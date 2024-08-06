----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
-- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `processunity.db_6sense_buying_stages_movement` AS

-- Set buying stages and their order
WITH stage_order AS (
  
  SELECT 'Target' AS _buying_stage, 1 AS _order
  UNION ALL
  SELECT 'Awareness', 2
  UNION ALL
  SELECT 'Consideration', 3
  UNION ALL
  SELECT 'Decision', 4
  UNION ALL
  SELECT 'Purchase', 5

),

-- Get buying stage data
buying_stage_data AS (

  SELECT
    DISTINCT ROW_NUMBER() OVER (
      PARTITION BY _6sensecompanyname, _6sensecountry, _6sensedomain
      ORDER BY 
        CASE
          WHEN _extractdate LIKE '%/%' 
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
          ELSE
            PARSE_DATE('%F', _extractdate)
        END DESC
    ) AS _rownum,
    CASE
      WHEN _extractdate LIKE '%/%' 
        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
      ELSE 
        PARSE_DATE('%F', _extractdate)
    END AS _activities_on,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    CONCAT(_6sensecountry,_6sensecompanyname) AS _country_account,
    '6sense' AS _data_source,
    _buyingstagestart AS _previous_stage,
    _buyingstageend AS _current_stage
  FROM
    `processunity_mysql.processunity_db_6sense_initial_buying_stage`
),

-- Latest buying stage with movement
latest_buying_stage_with_order_and_movement AS (

  SELECT 
    main.* EXCEPT (_rownum),
    prev._order AS _previous_stage_order,
    curr._order AS _current_stage_order,
    CASE
      WHEN curr._order > prev._order THEN '+ve'
      WHEN prev._order > curr._order THEN '-ve'
      ELSE 'Stagnant'
    END AS _movement
  FROM
    buying_stage_data AS main
  LEFT JOIN
    stage_order AS prev
  ON main._previous_stage = prev._buying_stage
  LEFT JOIN
    stage_order AS curr
  ON main._current_stage = curr._buying_stage
  WHERE main._rownum = 1
)
SELECT * FROM latest_buying_stage_with_order_and_movement;

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
-- 6sense Account Current State
-- Account Only

CREATE OR REPLACE TABLE `processunity.db_6sense_account_current_state` AS

-- Get all target accounts and their segments
WITH target_accounts AS (
    SELECT DISTINCT 
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _industrylegacy AS _6senseindustry,
        _6senseemployeerange,
        _6senserevenuerange,
        'Target' AS _source,
        IF(
            _extractdate LIKE '%/%',
            PARSE_DATE('%m/%e/%Y', _extractdate),
            PARSE_DATE('%F', _extractdate)
        ) AS _added_on,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM 
        `processunity_mysql.processunity_db_6sense_target_account`
),

-- Get date when account had first impression
reached_related_info AS (
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
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM 
        `processunity_mysql.processunity_db_6sense_account_reached`
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname) 
        ORDER BY (
            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                ELSE PARSE_DATE('%F', _latestimpression)
            END
        ) DESC
    ) = 1
),

six_qa_related_info AS (
  SELECT
      DISTINCT
      PARSE_DATE('%F',_6qadate) AS _6qa_date,
      true _is_6qa,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM
      `processunity_mysql.processunity_db_6qa_account`
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
        ORDER BY
            CASE
            WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE
                PARSE_DATE('%F', _extractdate)
            END
        DESC
    ) = 1
),

-- Get buying stage info for each account
buying_stage_related_info AS (
  SELECT
      DISTINCT
      _previous_stage,
      _previous_stage_order,
      _current_stage,
      _current_stage_order,
      _movement,
      _activities_on AS _movement_date,
      _country_account,
    FROM
      `processunity.db_6sense_buying_stages_movement`
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY _country_account
        ORDER BY _activities_on
        DESC
    ) = 1
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
    USING(_country_account) 
    LEFT JOIN
        six_qa_related_info AS six_qa 
    USING(_country_account) 
    LEFT JOIN
        buying_stage_related_info AS stage 
    USING(_country_account) 

)

SELECT * FROM combined_data;

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- 6sense Engagement Log
-- Account Engagement

CREATE OR REPLACE TABLE `processunity.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (
    SELECT * FROM `processunity.db_6sense_account_current_state`
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
        main._campaignid,
        -- Need label to distingush 6sense and Linkedin campaigns
        side._campaigntype,
        side._campaignname,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    FROM 
        `processunity_mysql.processunity_db_6sense_account_reached` main
    JOIN (
        SELECT DISTINCT 
            _campaignid, 
            _campaignname,  
            _platform AS _campaigntype
        FROM
            `processunity_mysql.processunity_optimization_airtable_ads_6sense`
    ) side
    USING(_campaignid)
),

-- Get campaign reached engagement for 6sense
sixsense_campaign_reached AS (
    SELECT DISTINCT 
        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,
        '6Sense Campaign Reached' AS _engagement,
        '6sense' AS _channel, 
        _campaignname AS _description, 
        1 AS _notes
    FROM
        reached_accounts_data
    WHERE
        _campaigntype = '6sense'
),

-- Get ad clicks engagement for 6sense
sixsense_ad_clicks AS (
    SELECT
        * EXCEPT(_old_notes)
    FROM (
        SELECT DISTINCT 
            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6Sense Ad Clicks' AS _engagement, 
            '6sense' AS _channel,
            _campaignname AS _description,  
            _clicks AS _notes,
            -- Get last period's clicks to compare
            LAG(_clicks) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _clicks >= 1
        AND
            _campaigntype = '6sense'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1
),

-- Get form fills engagement for 6sense
sixsense_form_fills AS (
    SELECT
        * EXCEPT(_old_notes)
    FROM (
        SELECT DISTINCT 
            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6Sense Influenced Form Fill' AS _engagement, 
            '6sense' AS _channel,
            _campaignname AS _description,  
            _influencedformfills AS _notes,

            -- Get last period's clicks to compare
            LAG(_influencedformfills) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _influencedformfills >= 1
        AND
            _campaigntype = '6sense'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1
),

-- Get campaign reached engagement for Linkedin
linkedin_campaign_reached AS (
    SELECT DISTINCT 
        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,
        'LinkedIn Campaign Reached' AS _engagement,
        'LinkedIn' AS _channel, 
        _campaignname AS _description, 
        1 AS _notes
    FROM
        reached_accounts_data
    WHERE
        _campaigntype = 'LinkedIn'
),

-- Get ad clicks engagement for Linkedin
linkedin_ad_clicks AS (
    SELECT
        * EXCEPT(_old_notes)
    FROM (
        SELECT DISTINCT 
            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            'LinkedIn Ad Clicks' AS _engagement, 
            'LinkedIn' AS _channel,
            _campaignname AS _description,  
            _clicks AS _notes,

            -- Get last period's clicks to compare
            LAG(_clicks) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _clicks >= 1
        AND
            _campaigntype = 'LinkedIn'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

-- Get form fills engagement for Linkedin
linkedin_form_fills AS (
    SELECT
        * EXCEPT(_old_notes)
    FROM (
        SELECT DISTINCT 
            CAST(NULL AS STRING) AS _email, 
            _country_account, 
            CAST(NULL AS STRING) AS _city,
            CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            'LinkedIn Influenced Form Fill' AS _engagement, 
            'LinkedIn' AS _channel,
            _campaignname AS _description,  
            _influencedformfills AS _notes,

            -- Get last period's clicks to compare
            LAG(_influencedformfills) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            )
            AS _old_notes
        FROM
            reached_accounts_data 
        WHERE
            _influencedformfills >= 1
        AND
            _campaigntype = 'LinkedIn'
    )
    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1
),

intent_engagements AS (
    SELECT 
        CAST(NULL AS STRING) AS _email,
        CONCAT(_companycountry, _companyname) AS _country_account,
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,
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
        '6sense' AS _channel,
        _categoryvalue AS _description,
        1 AS _notes
    FROM `x-marketing.processunity_mysql.processunity_db_6sense_engagement_activities`
),

-- Only activities involving target accounts are considered
combined_data AS (
    SELECT DISTINCT 
        target_accounts.*,
        activities.* EXCEPT(_country_account)
    FROM (

        SELECT * FROM sixsense_campaign_reached 
        UNION DISTINCT
        SELECT * FROM sixsense_ad_clicks 
        UNION DISTINCT
        SELECT * FROM sixsense_form_fills
        UNION DISTINCT
        SELECT * FROM linkedin_campaign_reached
        UNION DISTINCT
        SELECT * FROM linkedin_ad_clicks
        UNION DISTINCT
        SELECT * FROM linkedin_form_fills
        UNION DISTINCT
        SELECT * FROM intent_engagements
    ) activities
    JOIN
        target_accounts
    USING (_country_account)
),

-- Get accumulated values for each engagement
accumulated_engagement_values AS (
    SELECT
        *,
        -- The aggregated values
        SUM(CASE WHEN _engagement = '6Sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6Sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        SUM(CASE WHEN _engagement = '6Sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
        SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills
    FROM 
        combined_data
)

SELECT * FROM accumulated_engagement_values;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- 6sense Ad Performance

CREATE OR REPLACE TABLE `processunity.db_6sense_ad_performance` AS

WITH ads AS (
  SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      _name AS _advariation,
      _6senseid AS _adid,
      CAST(
        REPLACE(
          REPLACE(_spend, '$', ''),
          ',', ''
        ) AS FLOAT64
      ) AS _spend,
      CAST(REPLACE(_clicks, '.0', '') AS INT64) AS _clicks,
      CAST(REPLACE(_impressions, ',', '') AS INT64) AS _impressions,
      CASE
        WHEN _date LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _date)
        WHEN _date LIKE '%-%'
          THEN PARSE_DATE('%F', _date)
      END AS _date
    FROM
      `x-marketing.processunity_mysql.processunity_db_6sense_daily_campaign_performance`
    WHERE _datatype = 'Ad'
    QUALIFY ROW_NUMBER () OVER (
        PARTITION BY _campaignid, _6senseid, _date
        ORDER BY
          _date
    ) = 1
),
-- Get campaign level fields
campaign_fields AS (
  SELECT
      _campaignid AS _campaign_id,
      CASE
        WHEN _extractdate LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE
          PARSE_DATE('%F', _extractdate)
      END AS _extractdate,
      CASE 
        WHEN _startDate LIKE '%/%'
          THEN PARSE_DATE('%m/%d/%Y', _startDate)
        WHEN _startDate LIKE '%-%'
          THEN PARSE_DATE('%d-%h-%y', _startDate)
      END AS _start_date,

      CASE 
        WHEN _endDate LIKE '%/%'
          THEN PARSE_DATE('%m/%d/%Y', _endDate)
        WHEN _endDate LIKE '%-%'
          THEN PARSE_DATE('%d-%h-%y', _endDate)
      END AS _end_date,
      _status AS _campaign_status,
      _name AS _campaign_name,
      _campaigntype AS _campaign_type,
      IF (
        _accountsnewlyengagedlifetime = '-',
        0,
        SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
      ) AS _newly_engaged_accounts,
      IF (
            _accountswithincreasedengagementlifetime = '-',
            0,
            SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64) 
        ) AS _increased_engagement_accounts
    FROM
      `processunity_mysql.processunity_db_6sense_daily_campaign_performance`
    WHERE _datatype = 'Campaign'
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY _campaignid
      ORDER BY _extractdate DESC
    ) = 1
),

airtable_fields AS (
  SELECT 
    DISTINCT 
    _campaignid AS _campaign_id, 
    _adid AS _ad_id,
    _adgroup AS _ad_group,
    _screenshot,
    _messagingAngle,
    _size
  FROM
    `processunity_mysql.processunity_optimization_airtable_ads_6sense`  
  WHERE _campaignid <> ''
),

-- Combined Ads, Campaign and Airtable into one table
combined_data AS (
  SELECT
    campaign_fields._campaign_name,
    campaign_fields._campaign_type,
    campaign_fields._campaign_status,
    campaign_fields._start_date,
    campaign_fields._end_date,
    campaign_fields._extractDate,
    campaign_fields._newly_engaged_accounts,
    campaign_fields._increased_engagement_accounts,
    ads._campaign_id,
    ads._advariation,
    ads._adid,
    ads._spend,
    ads._clicks,
    ads._impressions,
    ads._date,
    airtable_fields._ad_group,
    airtable_fields._screenshot,
    airtable_fields._messagingAngle,
    airtable_fields._size
  FROM
    ads
  LEFT JOIN
    airtable_fields
  ON (
    ads._adid = airtable_fields._ad_id
    AND 
    ads._campaign_id = airtable_fields._campaign_id
  )
  OR (
    airtable_fields._ad_id IS NULL
    AND 
    ads._campaign_id = airtable_fields._campaign_id
  )
  LEFT JOIN
    campaign_fields
  ON ads._campaign_id = campaign_fields._campaign_id
),

-- Add campaign numbers to each ad
campaign_numbers AS (
  SELECT 
    *
  FROM
    combined_data
  -- Get accounts that are being targeted
  LEFT JOIN (
    SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _target_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side   --- CHANGE TO PROCESSUNITY 
      ON main._segmentname = side._segment   --- WRONG SEGMENT NAME IN AIRTABLE 6SENSE
    )
    GROUP BY 1
  ) target_acc

  USING(_campaign_id)

  -- Get accounts that have been reached
  LEFT JOIN (
    SELECT 
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _reached_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM 
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side    --- CHANGE TO PROCESSUNITY
      ON main._segmentname = side._segment   --- WRONG SEGMENT NAME IN AIRTABLE
      JOIN 
        `processunity_mysql.processunity_db_6sense_account_reached` extra
      USING (
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
      )
    )
    GROUP BY 1
  ) reach

  USING (_campaign_id)

  -- Get accounts that are 6QA
  LEFT JOIN (
    SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _6qa_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side    
      ON main._segmentname = side._segment
      JOIN 
        `x-marketing.processunity_mysql.processunity_db_6qa_account` extra   
      USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain
      )
      WHERE extra._6qadate IS NOT NULL
    )
    GROUP BY 1
  )
  USING (_campaign_id)
),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (
  SELECT
    *,
    COUNT(*) OVER(
      PARTITION BY _campaign_id
    ) AS _occurrence
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
    _6qa_accounts / _occurrence AS _reduced_6qa_accounts
  FROM 
    total_ad_occurrence_per_campaign
)

SELECT * FROM reduced_campaign_numbers;
CREATE OR REPLACE TABLE `processunity.db_6sense_ad_performance` AS

WITH ads AS (
  SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      _name AS _advariation,
      _6senseid AS _adid,
      CAST(
        REPLACE(
          REPLACE(_spend, '$', ''),
          ',', ''
        ) AS FLOAT64
      ) AS _spend,
      CAST(REPLACE(_clicks, '.0', '') AS INT64) AS _clicks,
      CAST(REPLACE(_impressions, ',', '') AS INT64) AS _impressions,
      CASE
        WHEN _date LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _date)
        WHEN _date LIKE '%-%'
          THEN PARSE_DATE('%F', _date)
      END AS _date
    FROM
      `x-marketing.processunity_mysql.processunity_db_6sense_daily_campaign_performance`
    WHERE _datatype = 'Ad'
    QUALIFY ROW_NUMBER () OVER (
        PARTITION BY _campaignid, _6senseid, _date
        ORDER BY
          _date
    ) = 1
),
-- Get campaign level fields
campaign_fields AS (
  SELECT
      _campaignid AS _campaign_id,
      CASE
        WHEN _extractdate LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE
          PARSE_DATE('%F', _extractdate)
      END AS _extractdate,
      CASE 
        WHEN _startDate LIKE '%/%'
          THEN PARSE_DATE('%m/%d/%Y', _startDate)
        WHEN _startDate LIKE '%-%'
          THEN PARSE_DATE('%d-%h-%y', _startDate)
      END AS _start_date,

      CASE 
        WHEN _endDate LIKE '%/%'
          THEN PARSE_DATE('%m/%d/%Y', _endDate)
        WHEN _endDate LIKE '%-%'
          THEN PARSE_DATE('%d-%h-%y', _endDate)
      END AS _end_date,
      _status AS _campaign_status,
      _name AS _campaign_name,
      _campaigntype AS _campaign_type,
      IF (
        _accountsnewlyengagedlifetime = '-',
        0,
        SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
      ) AS _newly_engaged_accounts,
      IF (
            _accountswithincreasedengagementlifetime = '-',
            0,
            SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64) 
        ) AS _increased_engagement_accounts
    FROM
      `processunity_mysql.processunity_db_6sense_daily_campaign_performance`
    WHERE _datatype = 'Campaign'
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY _campaignid
      ORDER BY _extractdate DESC
    ) = 1
),

airtable_fields AS (
  SELECT 
    DISTINCT 
    _campaignid AS _campaign_id, 
    _adid AS _ad_id,
    _adgroup AS _ad_group,
    _screenshot,
    _messagingAngle,
    _size
  FROM
    `processunity_mysql.processunity_optimization_airtable_ads_6sense`  
  WHERE _campaignid <> ''
),

-- Combined Ads, Campaign and Airtable into one table
combined_data AS (
  SELECT
    campaign_fields._campaign_name,
    campaign_fields._campaign_type,
    campaign_fields._campaign_status,
    campaign_fields._start_date,
    campaign_fields._end_date,
    ads.*,
    airtable_fields._ad_group,
    airtable_fields._screenshot,
    airtable_fields._messagingAngle,
    airtable_fields._size,
    campaign_fields._newly_engaged_accounts,
    campaign_fields._increased_engagement_accounts
  FROM
    ads
  LEFT JOIN
    airtable_fields
  ON (
    ads._adid = airtable_fields._ad_id
    AND 
    ads._campaign_id = airtable_fields._campaign_id
  )
  OR (
    airtable_fields._ad_id IS NULL
    AND 
    ads._campaign_id = airtable_fields._campaign_id
  )
  LEFT JOIN
    campaign_fields
  ON ads._campaign_id = campaign_fields._campaign_id
),

-- Add campaign numbers to each ad
campaign_numbers AS (
  SELECT 
    *
  FROM
    combined_data
  -- Get accounts that are being targeted
  LEFT JOIN (
    SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _target_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side   --- CHANGE TO PROCESSUNITY 
      ON main._segmentname = side._segment   --- WRONG SEGMENT NAME IN AIRTABLE 6SENSE
    )
    GROUP BY 1
  ) target_acc

  USING(_campaign_id)

  -- Get accounts that have been reached
  LEFT JOIN (
    SELECT 
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _reached_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM 
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side    --- CHANGE TO PROCESSUNITY
      ON main._segmentname = side._segment   --- WRONG SEGMENT NAME IN AIRTABLE
      JOIN 
        `processunity_mysql.processunity_db_6sense_account_reached` extra
      USING (
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
      )
    )
    GROUP BY 1
  ) reach

  USING (_campaign_id)

  -- Get accounts that are 6QA
  LEFT JOIN (
    SELECT
      DISTINCT
      _campaignid AS _campaign_id,
      COUNT(*) AS _6qa_accounts
    FROM (
      SELECT
        DISTINCT
        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._segmentname,
        side._campaignid
      FROM
        `processunity_mysql.processunity_db_6sense_target_account` main
      JOIN 
        `processunity_mysql.processunity_optimization_airtable_ads_6sense` side    
      ON main._segmentname = side._segment
      JOIN 
        `x-marketing.processunity_mysql.processunity_db_6qa_account` extra   
      USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain
      )
      WHERE extra._6qadate IS NOT NULL
    )
    GROUP BY 1
  )
  USING (_campaign_id)
),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (
  SELECT
    *,
    COUNT(*) OVER(
      PARTITION BY _campaign_id
    ) AS _occurrence
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
    _6qa_accounts / _occurrence AS _reduced_6qa_accounts
  FROM 
    total_ad_occurrence_per_campaign
)

SELECT * FROM reduced_campaign_numbers;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- 6sense account performance

CREATE OR REPLACE TABLE `processunity.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (
  SELECT
    DISTINCT
    main._6sensecompanyname,
    main._6sensecountry,
    main._6sensedomain,
    main._segmentname,              
    side._segment,
    side._campaignid,
    side._campaignid AS _campaign_id,
    side._campaignname AS _campaign_name
  FROM
    `processunity_mysql.processunity_db_6sense_target_account` main
  JOIN
    `processunity_mysql.processunity_optimization_airtable_ads_6sense` side   --- CHANGE TO PROCESSUNITY
  ON main._segmentname = side._segment
),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (
  SELECT
    DISTINCT
    main.* EXCEPT(_campaignid),
    CASE
      WHEN side._campaignid IS NOT NULL
        THEN true
    END AS _is_reached,
    
    CASE
      WHEN CAST(REPLACE(side._clicks, ',', '') AS INT64) > 0
        THEN true
    END AS _has_clicks,

    CASE
      WHEN CAST(REPLACE(side._impressions, ',', '') AS INT64) > 0
        THEN true
    END AS _has_impressions

  FROM
    target_accounts AS main
  LEFT JOIN
    `processunity_mysql.processunity_db_6sense_account_reached` side 
  USING (
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _campaignid
  )
)

SELECT * FROM reached_accounts;

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- Opportunity Influenced + Accelerated
-- Opportunity Engagement

CREATE OR REPLACE TABLE `processunity.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (
    SELECT DISTINCT 
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain, 
        _6qa_date,
        _is_6qa,
        _engagement, 
        ROW_NUMBER() OVER() AS _eng_id,
        _timestamp AS _eng_timestamp,
        _description AS _eng_description,
        _notes AS _eng_notes,
        CASE
            WHEN LOWER(_engagement) LIKE '%6sense%' THEN '6sense'
            WHEN LOWER(_engagement) LIKE '%linkedin%' THEN 'LinkedIn'
        END 
        AS _channel,
        LOWER(CONCAT(_6sensecountry,_6sensedomain)) AS _country_domain
    FROM 
        `processunity.db_6sense_engagement_log` 
),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (

    SELECT DISTINCT 
        opp.accountid AS _account_id, 
        act.name AS _account_name,
        act.domain__c AS _domain, 
        act.billingcountry AS _country,
        opp.id AS _opp_id,
        opp.name AS _opp_name,
        own.name AS _opp_owner_name,
        opp.type AS _opp_type,
        DATE(opp.createddate) AS _created_date,
        DATE(opp.closedate) AS _closed_date,
        opp.amount AS _amount,
        LOWER(CONCAT(act.billingcountry,act.domain__c)) AS _country_domain
    FROM 
        `processunity_salesforce.Opportunity` opp
    LEFT JOIN
        `processunity_salesforce.Account` act
    ON 
        opp.accountid = act.id 
    LEFT JOIN
        `processunity_salesforce.User` own
    ON 
        opp.ownerid = own.id 
    WHERE 
        opp.isdeleted = false

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
            `processunity_salesforce.OpportunityFieldHistory` 
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
            `processunity_salesforce.OpportunityFieldHistory`
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
      opp.* EXCEPT(_country_domain),
      act.* EXCEPT(_country_domain),
      CASE
          WHEN act._engagement IS NOT NULL
          THEN true 
      END 
      AS _is_matched_opp
    FROM 
      opps_history AS opp
    LEFT JOIN 
      target_account_engagements AS act
    USING (_country_domain)
    -- ON (
    --         opp._domain = act._6sensedomain
    --     AND 
    --         LENGTH(opp._domain) > 1
    --     AND 
    --         LENGTH(act._6sensedomain) > 1
    -- )
    -- OR (
    --         LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
    --     AND 
    --         LENGTH(opp._account_name) > 1
    --     AND 
    --         LENGTH(act._6sensecompanyname) > 1
    -- )
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

-- Label accounts that became 6QA before the influenced opportunity was created
label_6qa_before_influenced_opportunity AS (
    SELECT
        *,
        CASE
            WHEN 
                _is_influenced_opp IS NOT NULL
            AND 
                _6qa_date < _created_date
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
            THEN true 
        END 
        AS _is_6qa_after_influenced_opp
    FROM 
        label_6qa_before_influenced_opportunity
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
-- Opportunity Only

CREATE OR REPLACE TABLE `processunity.opportunity_summarized` AS

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

    -- _is_6qa_before_influenced_opp,
    -- _is_6qa_after_influenced_opp,
    _is_6qa,
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
    `processunity.opportunity_influenced_accelerated`;
