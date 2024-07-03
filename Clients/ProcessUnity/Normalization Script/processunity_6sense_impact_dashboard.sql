
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

---------------------------------------------- BUYING STAGE MOVEMENT ----------------------------------------------

CREATE OR REPLACE TABLE `processunity.db_6sense_buying_stages_movement` AS

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

),

-- Get buying stage data
buying_stage_data AS (

    SELECT DISTINCT
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _buyingstageend AS _buying_stage,

        CASE 
            WHEN _extractdate LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate)
        END 
        AS _activities_on
        
    FROM 
        `processunity_mysql.processunity_db_6sense_initial_buying_stage`

),

-- Get first ever buying stage for each account
first_ever_buying_stage AS (

    SELECT DISTINCT 
        _activities_on,
        _6sensecountry,
        _6sensedomain, 
        _6sensecompanyname,
        _buying_stage,
        'Initial' AS _source,
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM
        buying_stage_data
    JOIN (

        SELECT DISTINCT 
            _6sensecountry, 
            _6sensecompanyname, 
            MIN(_activities_on) AS _activities_on 
        FROM 
            buying_stage_data 
        GROUP BY 
            1, 2

    ) 
    USING(
        _6sensecountry, 
        _6sensecompanyname, 
        _activities_on
    )
    ORDER BY
        1 DESC

),

-- Get every other buying stage for each account
every_other_buying_stage AS (

    SELECT 
        * 
    FROM (

        SELECT DISTINCT 
            _activities_on,
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage,
            'Non Initial' AS _source,
            CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
        FROM
            buying_stage_data
            
    )
    -- Exclude those that are first ever stages
    WHERE 
        CONCAT(_country_account, _activities_on) NOT IN (

            SELECT DISTINCT 
                CONCAT(_country_account, MIN(_activities_on)) 
            FROM 
                first_ever_buying_stage 
            GROUP BY 
                _country_account
        
        )

),

-- Combine both first ever data and every other data
historical_buying_stage AS (

    SELECT * FROM first_ever_buying_stage 
    UNION DISTINCT
    SELECT * FROM every_other_buying_stage

),

-- Get the current stage and previous stage for each historical record of an account
set_buying_stage_order AS (

    SELECT DISTINCT 
        main.* EXCEPT(_current_stage, _prev_stage),
        main._current_stage,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                )
            ) 
            AND _prev_stage IS NULL, 
            _current_stage, 
            _prev_stage  
        ) 
        AS _prev_stage,

        curr._order AS _curr_order,

        IF(
            _activities_on = (
                MIN(_activities_on) OVER(
                    PARTITION BY _6sensedomain, _6sensecountry 
                    ORDER BY _activities_on
                ) 
            )
            AND _prev_stage IS NULL, 
            curr._order, 
            prev._order  
        ) 
        AS _prev_order

    FROM (

        SELECT DISTINCT 
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname,
            _buying_stage AS _current_stage,
            _activities_on,

            LAG(_buying_stage) OVER(
                PARTITION BY _6sensedomain 
                ORDER BY _activities_on ASC
            ) AS _prev_stage,

            _source,
            _country_account
        FROM 
            historical_buying_stage

    ) main
    LEFT JOIN
        stage_order AS curr 
    ON 
        main._current_stage = curr._buying_stage
    LEFT JOIN
        stage_order AS prev 
    ON 
        main._prev_stage = prev._buying_stage

),

-- Set movement of each historical record an account
set_movement AS (

    SELECT * EXCEPT(_order) 
    FROM (

        SELECT DISTINCT 
            *,

            IF(
                _curr_order > _prev_order, 
                "+ve", 
                IF(
                    _curr_order < _prev_order, 
                    "-ve", 
                    "Stagnant"
                )
            ) 
            AS _movement,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC
            ) 
            AS _order

        FROM
            set_buying_stage_order
        ORDER BY 
            _activities_on DESC

    )
    WHERE
        _order = 1
    ORDER BY
        _country_account

)

SELECT * FROM set_movement;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

---------------------------------------------- ACCOUNT CURRENT STATE ----------------------------------------------

CREATE OR REPLACE TABLE `processunity.db_6sense_account_current_state` AS

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
        `processunity_mysql.processunity_db_6sense_target_account`
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
      `processunity_mysql.processunity_db_6sense_target_account`
    
    GROUP BY 2
    ORDER BY 1 DESC
  ) scenario

  ON main._country_account = scenario._country_account
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
      `processunity_mysql.processunity_db_6sense_account_reached`
    /*
    WHERE
    _campaignid IN (
      SELECT
        DISTINCT _campaignid
      FROM
        `processunity_mysql.jaggaer_optimization_airtable_ads_6sense`
      WHERE _campaignid <> ''
    )
    */
  )
  WHERE _rownum = 1
),
/*
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
  )
  WHERE _rownum = 1
),
*/
/*
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
*/

-- Attach all other data parts to target accounts
combined_data AS (
  SELECT
    DISTINCT
    target_acc.*,
    reached.* EXCEPT(_country_account),
    /*
    six_qa.* EXCEPT(_country_account),
    stage.* EXCEPT(_country_account)
    */
  FROM
    target_accounts AS target_acc
  LEFT JOIN
    reached_related_info AS reached
  USING 
    (_country_account)
  /*
  LEFT JOIN
    six_qa_related_info AS six_qa
  USING
    (_country_account)
  LEFT JOIN
    buying_stage_related_info AS stage
  USING
    (_country_account)
  */
)
SELECT * FROM combined_data;


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

---------------------------------------------- Engagement Log ----------------------------------------------

-- 6sense Engagement Log

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
        -- side._campaigntype,
        CAST(NULL AS STRING) AS _campaigntype,
        -- side._campaignname,
        CAST(NULL AS STRING) AS _campaignname,
        CONCAT(main._6sensecountry, main._6sensecompanyname) AS _country_account
    
    FROM 
        `processunity_mysql.processunity_db_6sense_account_reached` main
    /*
    JOIN (

        SELECT DISTINCT 
            _campaignid, 
            _name AS _campaignname,  
            _campaigntype
        FROM
            `sandler_mysql.db_airtable_6sense_segment`
        WHERE 
            _sdc_deleted_at IS NULL

    ) side

    USING(_campaignid)
    */

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
        _campaigntype = '6sense Advertising'

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
            _campaigntype = '6sense Advertising'

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
            _campaigntype = '6sense Advertising'

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
/*
-- Get SEM engagement 
sem_engagements AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account, 
        CAST(NULL AS STRING) AS _city,
        CAST(NULL AS STRING) AS _state,

        CASE 
            WHEN _date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _date)
            ELSE PARSE_DATE('%F', _date)
        END 
        AS _timestamp,

        'SEM Engagement' AS _engagement,
        'SEM' AS _channel,
        _utmcampaign AS _description, 
        1 AS _notes

    FROM
        `sandler_mysql.db_sem_engagement_new` 
    WHERE
        _sdc_deleted_at IS NULL

),

-- Prep the sales intelligence data for use later
sales_intelligence_data AS (

    SELECT 

        _activitytype,
        _activitytarget,
        _contactemail,
        _accountname,
        _country,
        _city,
        _state,
        
        CASE 
            WHEN _date LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', _date)
            ELSE PARSE_DATE('%F', _date)
        END  
        AS _date,

        CONCAT(_country, _accountname) AS _country_account,
        COUNT(*) AS _count

    FROM 
        `sandler_mysql.db_sales_intelligence_activities`
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8

),

-- Get campaign reached engagements
sales_intelligence_campaign_reached AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        _activitytype AS _engagement, 
        'Sales Intelligence' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%Reached%'

),

-- Get web visits engagement
web_visits AS (

    SELECT DISTINCT

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        '6Sense Web Visits' AS _engagement, 
        '6sense' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%Web Visit%'

),

-- Get searched keywords engagements
searched_keywords AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        '6Sense Searched Keywords' AS _engagement, 
        '6sense' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        _activitytype LIKE '%KW Research%'

),

-- Get email opens and clicks engagements
email_engagements AS (

    SELECT DISTINCT 

        _contactemail AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        CONCAT(_activitytype, 'ed') AS _engagement, 
        'Hubspot' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        REGEXP_CONTAINS(_activitytype,'Email Open|Email Click')

),

-- Get all other new engagements from sales intelligence
other_engagements AS (

    SELECT DISTINCT 

        _contactemail AS _email, 
        _country_account, 
        _city,
        _state,
        _date AS _timestamp, 
        
        CASE 
            WHEN REGEXP_CONTAINS(_activitytype,'Bombora') THEN 'Bombora Topic Surged'
            WHEN REGEXP_CONTAINS(_activitytype,'Form Fill') THEN 'Form Filled'
            WHEN REGEXP_CONTAINS(_activitytype,'Email Reply') THEN 'Email Replied'
            WHEN REGEXP_CONTAINS(_activitytype,'Page Click') THEN 'Webpage Clicked'
            WHEN REGEXP_CONTAINS(_activitytype,'Submit') THEN 'Submitted'
            WHEN REGEXP_CONTAINS(_activitytype,'Video Play') THEN 'Video Played'
            WHEN REGEXP_CONTAINS(_activitytype,'Attend') THEN _activitytype
            WHEN REGEXP_CONTAINS(_activitytype,'Register') THEN _activitytype
            ELSE 'Unclassified Engagement'
        END 
        AS _engagement, 
        
        'Sales Intelligence' AS _channel,
        _activitytarget AS _description,
        _count AS _notes

    FROM
        sales_intelligence_data
    WHERE
        NOT REGEXP_CONTAINS(_activitytype,'Reached|Web Visit|KW Research|Email Open|Email Click')

),
*/

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
        /*
        UNION DISTINCT
        SELECT * FROM sem_engagements
        UNION DISTINCT
        SELECT * FROM sales_intelligence_campaign_reached
        UNION DISTINCT
        SELECT * FROM web_visits
        UNION DISTINCT
        SELECT * FROM searched_keywords
        UNION DISTINCT
        SELECT * FROM email_engagements
        UNION DISTINCT
        SELECT * FROM other_engagements
        */
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
        SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills,
        SUM(CASE WHEN _engagement = 'SEM Engagement' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_sem_engagements,
        SUM(CASE WHEN _engagement = '6Sense Web Visits' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_web_visits,
        SUM(CASE WHEN _engagement = '6Sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
        SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;
