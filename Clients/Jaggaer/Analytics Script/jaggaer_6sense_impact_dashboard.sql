---------------------------------------------- BUYING STAGE ----------------------------------------------
--- 6sense Buying Stage Movement

CREATE OR REPLACE TABLE `jaggaer.db_6sense_buying_stages_movement` AS

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
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
    '6sense' AS _data_source,
    _buyingstagestart AS _previous_stage,
    _buyingstageend AS _current_stage
  FROM
    `jaggaer_mysql.jaggaer_db_account_buying_stage`
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


---------------------------------------------- ACCOUNT CURRENT STATE ----------------------------------------------

CREATE OR REPLACE TABLE `jaggaer.db_6sense_account_current_state` AS

--- Get all target accounts and their segments
WITH target_accounts AS (
  
  SELECT 
    DISTINCT main.*
  FROM (
    SELECT
      DISTINCT
      _6sensecompanyname,
      _6sensecountry,
      _6sensedomain,
      _industrylegacy AS _6senseindustry,
      _6senseemployeerange,
      _6senserevenuerange,
      CASE
        WHEN _extractdate LIKE '%/%' 
          THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE 
          PARSE_DATE('%F', _extractdate)
      END AS _added_on,
      '6sense' AS _data_source,
      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
      FROM
        `jaggaer_mysql.jaggaer_db_segment_target_accounts`
  ) main

-- To get the earliest date of appearance of each account
  JOIN (
    SELECT
      DISTINCT
      MIN(
        CASE
          WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
          ELSE
            PARSE_DATE('%F', _extractdate)
        END
      )
      AS _added_on,

      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    FROM
      `jaggaer_mysql.jaggaer_db_segment_target_accounts`
    
    GROUP BY 2
    ORDER BY 1 DESC
  ) scenario

  ON main._country_account = scenario._country_account AND main._added_on = scenario._added_on
),

reached_related_info AS (
  SELECT 
    * EXCEPT(_rownum)
  FROM(
    SELECT
      DISTINCT
      MIN(
        CASE
          WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
          ELSE
            PARSE_DATE('%F', _latestimpression)
          END
      ) 
      OVER (
        PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
      ) AS _first_impressions,
      
      CASE
        WHEN _websiteengagement = '-' 
          THEN CAST(NULL AS STRING)
        ELSE
          _websiteengagement
      END AS _websiteengagement,

      ROW_NUMBER() OVER (
        PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
      ORDER BY
        CASE
          WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
          ELSE
            PARSE_DATE('%F', _latestimpression)
        END DESC
      ) AS _rownum,

      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    FROM
      `jaggaer_mysql.jaggaer_db_6sense_accounts_reached`
    WHERE
      _campaignid IN (
        SELECT
          DISTINCT _campaignid
        FROM
          `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
        WHERE _campaignid <> ''
      )
  )
  WHERE _rownum = 1
),

six_qa_related_info AS (
  WITH max_batchid AS (
  SELECT MAX(_batchid) AS max_batchid
  FROM `jaggaer_mysql.jaggaer_db_6qa_accounts_list`
)
SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      DISTINCT
      CASE
        WHEN _extractdate LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE
          PARSE_DATE('%F', _extractdate)
      END AS _6qa_date,
      
      true _is_6qa,

      ROW_NUMBER() OVER(
        PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
        ORDER BY
          CASE
            WHEN _extractdate LIKE '%/%'
              THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE
              PARSE_DATE('%F', _extractdate)
          END
        DESC
      ) AS _rownum,

      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    FROM
      `jaggaer_mysql.jaggaer_db_6qa_accounts_list`
    CROSS JOIN max_batchid
    WHERE _batchid = max_batchid.max_batchid
  )
  WHERE _rownum = 1

  
  /* SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      DISTINCT
      CASE
        WHEN _extractdate LIKE '%/%'
          THEN PARSE_DATE('%m/%e/%Y', _extractdate)
        ELSE
          PARSE_DATE('%F', _extractdate)
      END AS _6qa_date,
      
      true _is_6qa,

      ROW_NUMBER() OVER(
        PARTITION BY CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain)
        ORDER BY
          CASE
            WHEN _extractdate LIKE '%/%'
              THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE
              PARSE_DATE('%F', _extractdate)
          END
        DESC
      ) AS _rownum,

      CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    FROM
      `jaggaer_mysql.jaggaer_db_6qa_accounts_list`
  )
  WHERE _rownum = 1 */
),

-- Get buying stage info for each account
buying_stage_related_info AS (
  SELECT
    DISTINCT * EXCEPT(rownum)
  FROM (
    SELECT
      DISTINCT
      _previous_stage,
      _previous_stage_order,
      _current_stage,
      _current_stage_order,
      _movement,
      _activities_on AS _movement_date,
      _country_account,

      ROW_NUMBER() OVER(
        PARTITION BY _country_account
        ORDER BY _activities_on
        DESC
      ) AS rownum
    FROM
      `jaggaer.db_6sense_buying_stages_movement`
  )
  WHERE rownum = 1
),

-- Attach all other data parts to target accounts
combined_data AS (
  SELECT
    DISTINCT
    target_acc.*,
    reached.* EXCEPT(_country_account),
    six_qa.* EXCEPT(_country_account),
    stage.* EXCEPT(_country_account)
  FROM
    target_accounts AS target_acc
  LEFT JOIN
    reached_related_info AS reached
  USING 
    (_country_account)
  LEFT JOIN
    six_qa_related_info AS six_qa
  USING
    (_country_account)
  LEFT JOIN
    buying_stage_related_info AS stage
  USING
    (_country_account)
)
SELECT * FROM combined_data;


---------------------------------------------- 6SENSE ENGAGEMENT LOG ----------------------------------------------

CREATE OR REPLACE TABLE `jaggaer.db_6sense_engagement_log` AS

-- Get all target accounts and their unique info
WITH target_accounts AS (
  SELECT * FROM `jaggaer.db_6sense_account_current_state`
),

-- Prep the reached account data for use later
reached_accounts_data AS (
  SELECT 
    DISTINCT
    CAST(main._clicks AS INTEGER) AS _clicks,
    CAST(main._influencedformfills AS INTEGER) AS _influencedformfills,
    CASE 
      WHEN main._latestimpression LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', main._latestimpression)
      ELSE 
        PARSE_DATE('%F', main._latestimpression)
    END AS _latestimpression, 

    CASE 
      WHEN main._extractdate LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
      ELSE 
        PARSE_DATE('%F', main._extractdate)
    END AS _activities_on, 

    main._campaignid AS _campaign_id,

    -- Need label to distingush 6sense and Linkedin campaigns
    side._campaigntype AS _campaign_type,
    side._campaignname AS _campaign_name,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
  FROM 
    `jaggaer_mysql.jaggaer_db_6sense_accounts_reached` main
  JOIN (
    
    SELECT 
      DISTINCT 
      _campaignid, 
      _campaignname,  
      IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
    FROM
      `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
    WHERE 
      _campaignid != ''
        
    UNION ALL

    SELECT 
      DISTINCT 
      _6sensecampaignid AS _campaignid, 
      _campaignname,  
      IF(_platform = 'LinkedIn', 'LinkedIn', _platform) AS _campaigntype
    FROM
      `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
    WHERE 
      _campaignid != ''
    
    /* SELECT 
      DISTINCT 
      _campaignid, 
      _campaignname,  
      IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
    FROM
      `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
    WHERE 
      _campaignid != '' */
  ) side

  USING(_campaignid)

),
email_alerts_data AS (
    SELECT 
    DISTINCT 
        CASE 
          WHEN main._keywordcount IS NULL OR main._keywordcount = '' THEN 0
          ELSE CAST(main._keywordcount AS INTEGER) 
        END AS _keywordCount,
        CASE
          WHEN main._webvisitcount IS NULL OR main._webvisitcount = '' THEN 0
          ELSE CAST(main._webvisitcount AS INTEGER)
        END AS _webvisitCount,
        main._keywords,
        main._weburls,
        CASE 
          WHEN main._timeframe LIKE '%-%'
              THEN PARSE_DATE('%m-%d-%Y', FORMAT_DATE('%m-%d-%Y', PARSE_DATE('%d-%b-%y', main._timeframe)))
          ELSE 
              PARSE_DATE('%m/%d/%Y', FORMAT_DATE('%m/%d/%Y', PARSE_DATE('%b %d, %Y', main._timeframe)))
        END AS _latestimpression,
        CASE 
            WHEN main._extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
            ELSE 
            PARSE_DATE('%F', main._extractdate)
        END AS _activities_on, 
        side._campaignid AS _campaign_id,
        -- Need label to distingush 6sense and Linkedin campaigns
        side._campaigntype AS _campaign_type,
        side._campaignname AS _campaign_name,
        CONCAT(_accountname, _country, _domain) AS _country_account
    FROM `x-marketing.jaggaer_mysql.jaggaer_db_email_alerts` main
    JOIN (
      SELECT 
        DISTINCT 
        _campaignid, 
        _campaignname,  
        _segment AS _segmentname,
        IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
      FROM
        `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
      WHERE 
        _campaignid != ''

      UNION ALL

      SELECT 
        DISTINCT 
        _6sensecampaignid, 
        _campaignname,  
        _segment AS _segmentname,
        IF(_platform = 'LinkedIn', 'LinkedIn', _platform) AS _campaigntype
      FROM
        `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
      WHERE 
        _campaignid != ''
        
        /* SELECT 
        DISTINCT 
        _campaignid, 
        _campaignname,  
        _segment AS _segmentname,
        IF(_platform = '6Sense', '6sense', _platform) AS _campaigntype
        FROM
        `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
        WHERE 
        _campaignid != '' */
    ) side
    USING(_segmentname)
),
email_alerts AS (
    SELECT 
        DISTINCT 
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
        CONCAT(_campaign_type, ' ', 'Email Alerts') AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaign_name AS _description, 
        1 AS _notes
    FROM
        email_alerts_data
),
-- Get campaign reached engagements
campaign_reached AS (
  SELECT 
    DISTINCT 
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
  SELECT * EXCEPT(_old_notes)
  FROM (
    SELECT 
      DISTINCT 
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
    SELECT 
      DISTINCT 
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

_keywords AS (
    SELECT * EXCEPT (_old_notes)
    FROM (
      SELECT
        DISTINCT
        _country_account,
        CAST(NULL AS STRING) AS _email,  
        _latestimpression AS _timestamp,     -- _activities_on
        CONCAT(_campaign_type, ' ', 'Keywords') AS _engagement, 
        '6sense' AS _engagement_data_source,
        _keywords AS _description,  
        _keywordCount AS _notes,

        -- Get last period's clicks to compare
        LAG(_keywordCount) OVER(
          PARTITION BY 
            _country_account, 
            _campaign_name
          ORDER BY 
            _latestimpression     -- _activities_on
        )
        AS _old_notes
      FROM
        email_alerts_data
      WHERE
        _keywordCount >= 1
    )
    WHERE 
      (_notes - COALESCE(_old_notes, 0)) >= 1
),

_webVisits AS (
    SELECT * EXCEPT (_old_notes)
    FROM (
      SELECT
        DISTINCT
        _country_account,
        CAST(NULL AS STRING) AS _email,  
        _latestimpression AS _timestamp,     -- _activities_on
        CONCAT(_campaign_type, ' ', 'Web Visits') AS _engagement, 
        '6sense' AS _engagement_data_source,
        _weburls AS _description,  
        _webvisitCount AS _notes,

        -- Get last period's clicks to compare
        LAG(_webvisitCount) OVER(
          PARTITION BY 
            _country_account, 
            _campaign_name
          ORDER BY 
            _latestimpression     -- _activities_on
        )
        AS _old_notes
      FROM
        email_alerts_data 
      WHERE
        _webvisitCount >= 1
    )
    WHERE 
      (_notes - COALESCE(_old_notes, 0)) >= 1
),

/* Sales Intelligence Data should be here // No sales intelligence data for Jaggaer */

-- Only activities involving target accounts are considered
combined_data AS (
  SELECT 
    DISTINCT 
    target_accounts.*,
    activities.* EXCEPT(_country_account)
  FROM (
    SELECT * FROM campaign_reached 
    UNION DISTINCT
    SELECT * FROM email_alerts 
    UNION DISTINCT
    SELECT * FROM ad_clicks 
    UNION DISTINCT
    SELECT * FROM influenced_form_fills
    UNION DISTINCT
    SELECT * FROM _keywords
    UNION DISTINCT
    SELECT * FROM _webVisits
    --UNION DISTINCT
    --SELECT * FROM sales_intelligence_engagements
    -- UNION DISTINCT
    -- SELECT * FROM hubspot_email_engagements
  ) activities
  JOIN
    target_accounts
  USING (_country_account)
)

SELECT * FROM combined_data;

---------------------------------------------- 6SENSE AD PERFORMANCE ----------------------------------------------

CREATE OR REPLACE TABLE `jaggaer.db_6sense_ad_performance` AS

WITH ads AS (
  SELECT 
    * EXCEPT(_rownum)
  FROM (
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
      END AS _date,
      ROW_NUMBER () OVER (
        PARTITION BY _campaignid, _6senseid, _date
        ORDER BY
          CASE
            WHEN _date LIKE '%/%'
              THEN PARSE_DATE('%m/%e/%Y', _date)
            WHEN _date LIKE '%-%'
              THEN PARSE_DATE('%F', _date)
          END
      ) AS _rownum
    FROM
      `x-marketing.jaggaer_mysql.jaggaer_db_6sense_daily_campaign_performance`
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

      CASE 
        WHEN _accountsnewlyengagedlifetime = '-'
          THEN 0
        ELSE 
          SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
      END AS _newly_engaged_accounts,

      CASE 
        WHEN _accountswithincreasedengagementlifetime = '-'
          THEN 0
        ELSE 
          SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
      END AS _increased_engagement_accounts,

      ROW_NUMBER() OVER(
        PARTITION BY _campaignid
        ORDER BY
          CASE
            WHEN _extractdate LIKE '%/%'
              THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE 
              PARSE_DATE('%F', _extractdate)
          END 
        DESC
      ) AS _rownum
    FROM
      `jaggaer_mysql.jaggaer_db_6sense_daily_campaign_performance`
    WHERE _datatype = 'Campaign'
  )
  WHERE _rownum = 1
),

airtable_fields AS (
  WITH _6sense AS (
  SELECT 
    DISTINCT 
    _campaignid AS _campaign_id, 
    _adid AS _ad_id,
    _adgroup AS _ad_group,
    _screenshot    
  FROM
    `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
  WHERE _campaignid <> ''
),
_linkedin AS (
  SELECT 
    DISTINCT 
    _6sensecampaignid AS _campaign_id, 
    _6senseadid AS _ad_id,
    _adgroup AS _ad_group,
    _screenshot    
  FROM
    `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
  WHERE _campaignid <> ''
)
SELECT * FROM _6sense
UNION ALL
SELECT * FROM _linkedin
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
        `jaggaer_mysql.jaggaer_db_segment_target_accounts` main
      JOIN (
        SELECT _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
        UNION ALL
        SELECT _6sensecampaignid AS _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
      ) side 
      ON main._segmentname = side._segment
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
        `jaggaer_mysql.jaggaer_db_segment_target_accounts` main
      JOIN (
        SELECT _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
        UNION ALL
        SELECT _6sensecampaignid AS _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
      ) side
      ON main._segmentname = side._segment
      JOIN 
        `jaggaer_mysql.jaggaer_db_6sense_accounts_reached` extra
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
        `jaggaer_mysql.jaggaer_db_segment_target_accounts` main
      JOIN (
        SELECT _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense`
        UNION ALL
        SELECT _6sensecampaignid AS _campaignid,_segment FROM `jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
      ) side
      ON main._segmentname = side._segment
      JOIN 
        `jaggaer.db_6sense_account_current_state` extra
      USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain
      )
      WHERE extra._6qa_date IS NOT NULL
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


-- Insert LinkedIn campaign performance data into Ad Performance table
INSERT INTO `jaggaer.db_6sense_ad_performance` (
_adid, 
_date, 
_spend, 
_clicks, 
_impressions, 
_campaign_type, 
_campaign_id, 
_campaign_name, 
_campaign_status, 
_start_date, 
_end_date, 
_advariation, 
_ad_group, 
_newly_engaged_accounts, 
_increased_engagement_accounts, 
_target_accounts, 
_reached_accounts, 
_6qa_accounts, 
_occurrence, 
_reduced_newly_engaged_accounts, 
_reduced_increased_engagement_accounts, 
_reduced_target_accounts, 
_reduced_reached_accounts, 
_reduced_6qa_accounts, 
_screenshot
) 
WITH
linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS DATE) AS _date,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions,
    'LinkedIn' AS _campaign_type
  FROM
    `jaggaer_linkedin_ads.ad_analytics_by_creative`
  GROUP BY creative_id, start_at
),
creative AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    CAST(campaign_id AS STRING) AS _campaign_id
  FROM `jaggaer_linkedin_ads.creatives`
),
campaign AS (
  SELECT
    name AS _campaign_name,
    CAST(id AS STRING) AS id,
    '' AS _campaign_status,
    CAST(NULL AS DATE) AS _start_date,
    CAST(NULL AS DATE) AS _end_date,
    '' AS _advariation,
    '' AS _ad_group,
    CAST(NULL AS INT64) AS _newly_engaged_accounts,
    CAST(NULL AS INT64) AS _increased_engagement_accounts,
    CAST(NULL AS INT64) AS _target_accounts,
    CAST(NULL AS INT64) AS _reached_accounts,
    CAST(NULL AS INT64) AS _6qa_accounts,
    CAST(NULL AS INT64) AS _occurrence,
    CAST(NULL AS INT64) AS _reduced_newly_engaged_accounts,
    CAST(NULL AS INT64) AS _reduced_increased_engagement_accounts,
    CAST(NULL AS INT64) AS _reduced_target_accounts,
    CAST(NULL AS INT64) AS _reduced_reached_accounts,
    CAST(NULL AS INT64) AS _reduced_6qa_accounts
  FROM `jaggaer_linkedin_ads.campaigns`
),
linkedin_airtable AS (
  SELECT
    _adid,
    _adtitle AS _adname, 
    _campaignid,  
    _campaignname, 
    _screenshot

  FROM `x-marketing.jaggaer_mysql.jaggaer_optimization_airtable_ads_linkedin`
  
  WHERE _campaignid = '304100656'
)
SELECT
  linkedin_ads.*,
  creative.* EXCEPT(cID),
  campaign.* EXCEPT (id),
  linkedin_airtable._screenshot
FROM linkedin_ads
JOIN linkedin_airtable 
ON linkedin_ads._adid = CAST(linkedin_airtable._adid AS STRING)
LEFT JOIN creative
ON linkedin_ads._adid = creative.cID
LEFT JOIN campaign
ON campaign.id = creative._campaign_id;


---------------------------------------------- 6SENSE ACCOUNT PERFORMANCE ----------------------------------------------

CREATE OR REPLACE TABLE `jaggaer.db_6sense_account_performance` AS

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
    `jaggaer_mysql.jaggaer_db_segment_target_accounts` main
  JOIN
    `jaggaer_mysql.jaggaer_optimization_airtable_ads_6sense` side
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
    `jaggaer_mysql.jaggaer_db_6sense_accounts_reached` side 
  USING (
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _campaignid
  )
)

SELECT * FROM reached_accounts;


---------------------------------------------- Opportunity Influenced + Accelerated ----------------------------------------------

CREATE OR REPLACE TABLE `jaggaer.opportunity_influenced_accelerated` AS

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
        END AS _channel
    FROM `jaggaer.db_6sense_engagement_log` 

),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
opps_created AS (
  WITH closedConversionRate AS (
    SELECT DISTINCT
      opp.id,
      isocode,
      opp.closedate,
      rate.conversionrate,
      opp.amount / rate.conversionrate AS converted
    FROM `x-marketing.jaggaer_salesforce.DatedConversionRate` rate
    LEFT JOIN `x-marketing.jaggaer_salesforce.Opportunity` opp
    ON rate.isoCode = opp.currencyisocode
      AND opp.closedate >= rate.startDate
      AND opp.closedate < rate.nextStartDate
    WHERE 
      opp.isclosed = true
    -- ORDER BY rate.startDate DESC
  ),
  openConversionRate AS (
    SELECT 
      * EXCEPT(rownum)
    FROM (
      SELECT DISTINCT
        opp.id,
        isocode,
        rate.conversionrate,
        rate.lastmodifieddate,
        opp.closedate,
        -- opp.total_price__c,
        ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) AS rownum
      FROM `x-marketing.jaggaer_salesforce.DatedConversionRate` rate
      LEFT JOIN `x-marketing.jaggaer_salesforce.Opportunity` opp
      ON opp.currencyisocode = rate.isocode
      WHERE opp.isclosed = false
      AND opp.currencyisocode != 'USD'
    )
    WHERE rownum = 1
    ORDER BY isocode 
  ),
  opps_main AS (
      SELECT DISTINCT 
          opp.accountid AS _account_id, 
          act.name AS _account_name,
          REGEXP_REPLACE(act.website, r'^(https?://)?www\.(.*?)(?:/|$)', r'\2') AS _domain,
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
          opp.acv__c AS _acv,
          opp.currencyisocode,
          opp.isclosed,
          opp.region__c AS _region,

          -- For filling up those opps with missing first stage in the opp history
          opp.stagename AS _current_stage,
          DATE(opp.laststagechangedate) AS _stage_change_date

      FROM 
          `jaggaer_salesforce.Opportunity` opp
      
      LEFT JOIN
          `jaggaer_salesforce.Account` act
      ON 
          opp.accountid = act.id 
      
      LEFT JOIN
          `jaggaer_salesforce.User` own
      ON 
          opp.ownerid = own.id 
      
      WHERE 
          opp.isdeleted = false
      AND 
          opp.createddate >= '2023-01-01'

  )
  SELECT
    *
  FROM (
    SELECT DISTINCT
      opps_main.* EXCEPT(_amount),
      -- Opportunity.opportunityID,
      -- Opportunity.createddate,
      -- Opportunity.isclosed,
      -- Opportunity.currencyisocode,
      _amount AS original_amount,
      CASE 
        WHEN isclosed = true AND currencyisocode != 'USD'
        THEN (
          closedConversionRate.conversionRate
        )
        WHEN isclosed = false AND currencyisocode != 'USD'
        THEN (
          openConversionRate.conversionRate 
        )
      END AS conversionRate,
      CASE 
        WHEN isclosed = true AND currencyisocode != 'USD'
        THEN (

          closedConversionRate.converted
        )
        WHEN isclosed = false AND currencyisocode != 'USD'
        THEN (
          (_amount / openConversionRate.conversionrate) 
        )
        ELSE _amount
      END AS _amount_converted,
      -- sfdc_activity_casesafeid__c,
      -- application_specialist__c,
      -- Event_Status__c,
      -- Web_Location__c
    FROM opps_main
    LEFT JOIN closedConversionRate ON closedConversionRate.id = opps_main._opp_id
    LEFT JOIN openConversionRate ON openConversionRate.isocode = opps_main.currencyisocode
  )
  WHERE _created_date >= '2023-01-01'
  AND _country = 'United States'  -- to filter those account based outside United States
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
            `jaggaer_salesforce.OpportunityFieldHistory` 
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
            newvalue AS _next_stage_prob
            --newvalue__fl AS _next_stage_prob,

        FROM
            `jaggaer_salesforce.OpportunityFieldHistory`
        WHERE
            field = 'Probability' --'ForecastProbability__c'
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
            CAST(MAX(side._current_stage) OVER (PARTITION BY side._opp_id) AS STRING),
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
            -- opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
            opp._domain = act._6sensedomain
        AND 
            LENGTH(opp._domain) > 1
        AND 
            LENGTH(act._6sensedomain) > 1
    )
        
    OR (
            -- opp._domain LIKE CONCAT('%', act._6sensedomain, '%')
            opp._domain = act._6sensedomain
        AND    
            LOWER(opp._account_name) = LOWER(act._6sensecompanyname)
        AND 
            LENGTH(opp._account_name) > 1
        AND 
            LENGTH(act._6sensecompanyname) > 1
    )

        OR 
    (
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
                        DATE_SUB(_created_date, INTERVAL 120 DAY) 
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

-- Label the activty that accelerated the opportunity
set_accelerating_activity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NULL
            --AND _created_date BETWEEN DATE(_eng_timestamp) AND DATE_ADD(DATE(_eng_timestamp) , INTERVAL 120 DAY)
            AND DATE(_eng_timestamp) > _created_date
            AND DATE(_eng_timestamp) <= _stage_change_date --_historical_stage_change_date
            AND _stage_movement = 'Upward'
            THEN true
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
        )
        AS _is_accelerated_opp
    FROM set_accelerating_activity
),

-- Label the activty that accelerated an influenced opportunity
set_accelerating_activity_for_influenced_opportunity AS (
    SELECT 
        *,
        CASE 
            WHEN _is_influenced_opp IS NOT NULL
            --AND _created_date BETWEEN DATE(_eng_timestamp) AND DATE_ADD(DATE(_eng_timestamp) , INTERVAL 120 DAY)
            AND DATE(_eng_timestamp) > _created_date
            AND DATE(_eng_timestamp) <= _stage_change_date --_historical_stage_change_date
            AND _stage_movement = 'Upward'
            THEN true
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


---------------------------------------------- Opportunity Summarized ----------------------------------------------
-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `jaggaer.opportunity_summarized` AS

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
    _amount_converted,
    _acv,
    _region,
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
    `jaggaer.opportunity_influenced_accelerated`;