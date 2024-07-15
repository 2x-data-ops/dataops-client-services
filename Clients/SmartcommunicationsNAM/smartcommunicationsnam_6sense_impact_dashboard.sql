CREATE OR REPLACE TABLE
  `smartcommnam.db_6sense_reached_account` AS
WITH reached AS (
  SELECT * EXCEPT (_spend, _impressions),
    SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
    SAFE_CAST(REGEXP_REPLACE(_spend, r'\$', '') AS FLOAT64) AS _spend,
  FROM
    `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY  _campaignid, _6sensecompanyname, _6sensecountry, _6sensedomain ORDER BY CASE
                      WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                      ELSE PARSE_DATE('%F', _extractdate) END DESC) = 1
),
airtable AS (
  SELECT
    DISTINCT _campaignid,
    _campaignname,
    '' AS _campaigntype
  FROM
    `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`
)
  SELECT
    reached.*,
    airtable.* EXCEPT (_campaignid)
  FROM reached
  LEFT JOIN airtable
    ON reached._campaignid = airtable._campaignid;



-- SCRIPT CONTEXT AND OVERVIEW
-- Analyst needs to build ads performance pages only for the 6sense impact dashboard
-- Currently only ads performance and account performance table is connected in the dashboard


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_buying_stages_movement` AS

WITH sixsense_stage_order AS (
        SELECT
            'Target' AS _buying_stage,
            1 AS _order
        UNION ALL
        SELECT
            'Awareness' AS _buying_stage,
            2 AS _order
        UNION ALL
        SELECT
            'Consideration' AS _buying_stage,
            3 AS _order
        UNION ALL
        SELECT
            'Decision' AS _buying_stage,
            4 AS _order
        UNION ALL
        SELECT
            'Purchase' AS _buying_stage,
            5 AS _order
    ),
    sixsense_buying_stage_data AS (
        SELECT
            DISTINCT ROW_NUMBER() OVER (
                PARTITION BY _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
                ORDER BY
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END DESC
            ) AS _rownum,
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _activities_on,
            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            CONCAT(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            ) AS _country_account,
            '6sense' AS _data_source,
            _buyingstagestart AS _previous_stage,
            _buyingstageend AS _current_stage
        FROM
            `smartcommnam_mysql.smartcommnam_db_6sense_buying_stages_nam`
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT main.*
            EXCEPT (_rownum),
            prev._order AS _previous_stage_order,
            curr._order AS _current_stage_order,
                CASE
                    WHEN curr._order > prev._order THEN '+ve'
                    WHEN prev._order > curr._order THEN '-ve'
                    ELSE 'Stagnant'
                END AS _movement
        FROM
            sixsense_buying_stage_data AS main
            LEFT JOIN sixsense_stage_order AS prev
                ON main._previous_stage = prev._buying_stage
            LEFT JOIN sixsense_stage_order AS curr
                ON main._current_stage = curr._buying_stage
        WHERE main._rownum = 1
    )
SELECT *
FROM
    latest_sixsense_buying_stage_with_order_and_movement;


--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------- ACCOUNT CURRENT STATE
--------------------------------------------------------------------------
--------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_account_current_state` AS

WITH target_accounts AS (
        SELECT DISTINCT main.*

        FROM (
                SELECT DISTINCT
                    _6sensecompanyname,
                    _6sensecountry,
                    _6sensedomain,
                    _industrylegacy AS _6senseindustry,
                    _6senseemployeerange,
                    _6senserevenuerange,
                    CASE
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END AS _added_on,
                    '6sense' AS _data_source,

                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                FROM
                    `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` 
            ) main

                            -- Get the earliest date of appearance of each account
                JOIN (

                    SELECT DISTINCT 

                        MIN(
                            CASE 
                                WHEN _extractdate LIKE '%/%'
                                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                                ELSE PARSE_DATE('%F', _extractdate)
                            END 
                        ) 
                        AS _added_on,

                        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
                        
                    FROM
                        `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam`
                    GROUP BY 
                        2
                    ORDER BY 
                        1 DESC

                ) scenario 

                ON 
                    main._country_account = scenario._country_account 
                AND 
                    main._added_on = scenario._added_on

    ),
    reached_related_info AS (
        SELECT *
        EXCEPT (_rownum)
        FROM (
                SELECT
                    DISTINCT MIN(
                        CASE
                            WHEN _extractdate LIKE '%/%' AND _latestimpression LIKE '%/%'
                            THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                            ELSE PARSE_DATE('%F', _latestimpression)
                        END
                    ) OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                    ) AS _first_impressions,
                    CASE
                        WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
                        ELSE _websiteengagement
                    END AS _websiteengagement,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONCAT(
                            _6sensecompanyname,
                            _6sensecountry,
                            _6sensedomain
                        )
                        ORDER BY
                            CASE
                                WHEN _extractdate LIKE '%/%' AND _latestimpression LIKE '%/%'
                                THEN PARSE_DATE('%m/%e/%Y', _latestimpression)
                                ELSE PARSE_DATE('%F', _latestimpression)
                            END DESC
                    ) AS _rownum,
                    CONCAT(
                        _6sensecompanyname,
                        _6sensecountry,
                        _6sensedomain
                    ) AS _country_account
        FROM
            `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam`
        WHERE 
            _campaignid IN (

                SELECT DISTINCT 
                    _campaignid
                FROM 
                    `smartcomm_mysql.smartcommunications_optimization_airtable_ads_6sense`
                WHERE 
                    _campaignid != ''

            )
        )
    WHERE _rownum = 1
),

six_qa_related_info AS (

    SELECT

        * EXCEPT(_rownum)

    FROM (

        SELECT DISTINCT

            CASE 
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END 
            AS _6qa_date,

            true _is_6qa,

            ROW_NUMBER() OVER(

                PARTITION BY 
                    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) 
                ORDER BY 
                    CASE 
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC

            )
            AS _rownum,

            CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account

        FROM 
            `smartcommnam_mysql.smartcommnam_db_6qa_account_nam`
    
    )

    WHERE 
        _rownum = 1

),

-- Get buying stage info for each account

buying_stage_related_info AS (

    SELECT DISTINCT 
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT

            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,

            ROW_NUMBER() OVER(
                PARTITION BY _country_account 
                ORDER BY _activities_on DESC 
            ) 
            AS rownum

        FROM
            `smartcommnam.db_6sense_buying_stages_movement`

    )
    WHERE 
        rownum = 1

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
    USING(
        _country_account
    )

    LEFT JOIN
        six_qa_related_info AS six_qa 
    USING(
        _country_account
    ) 

    LEFT JOIN
        buying_stage_related_info AS stage
    USING(
        _country_account
    ) 

)

SELECT * FROM combined_data;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ADS PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_ad_performance` AS

WITH ads AS (
SELECT *
    EXCEPT (_rownum)
    FROM (
        SELECT DISTINCT
            _campaignid,
            _name AS _advariation,
            _6senseid AS _adid,
            _accountvtr,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64
                ) AS _spend,
            CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
            -- SAFE_CAST(_impressions AS INTEGER) AS _impressions,
            SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
            -- CASE 
            --     WHEN _budget = '-' THEN NULL
            --     ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
            -- END AS _budget,
            CASE 
                WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
                WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
            END AS _date,
            ROW_NUMBER() OVER (
                    PARTITION BY _campaignid,
                    _6senseid,
                    _date
                    ORDER BY CASE 
                        WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate)
                    END
                ) AS _rownum
        FROM `smartcommnam_mysql.smartcommnam_db_6sense_campaign_performance_nam`
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

            _campaignid,
            _accountvtr,
            CASE 
                WHEN _budget = '-' THEN NULL
                ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
            END AS _budget,
            
            CASE
                WHEN _extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END
            AS _extractdate,

            CASE 
                WHEN _startDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _startDate)
                WHEN _startDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _startDate)
            END
            AS _start_date,

            CASE 
                WHEN _endDate LIKE '%/%'
                THEN PARSE_DATE('%m/%d/%Y', _endDate)
                WHEN _endDate LIKE '%-%'
                THEN PARSE_DATE('%d-%h-%y', _endDate)
            END
            AS _end_date,

            _status AS _campaign_status,
            _name AS _campaign_name,
            _campaigntype AS _campaign_type,
            
            CASE 
                WHEN _accountsnewlyengagedlifetime = '-'
                THEN 0
                ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
            END 
            AS _newly_engaged_accounts,

            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-'
                THEN 0
                ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
            END 
            AS _increased_engagement_accounts,

            ROW_NUMBER() OVER(
                PARTITION BY _campaignid
                ORDER BY 
                    CASE
                        WHEN _extractdate LIKE '%/%'
                        THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                        ELSE PARSE_DATE('%F', _extractdate)
                    END 
                DESC
            ) 
            AS _rownum

        FROM 
            `smartcommnam_mysql.smartcommnam_db_6sense_campaign_performance_nam`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE 
        _rownum = 1

),

airtable_fields AS (

    SELECT DISTINCT 

        _campaignid, 
        _adid,
        _adgroup,
        _screenshot
        
    FROM
        `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`
    WHERE 
        _campaignid != ''
),
combined_data AS (

    SELECT

        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        campaign_fields._budget,
        ads.*,
        airtable_fields._adgroup,
        airtable_fields._screenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts

    FROM 
        ads

    LEFT JOIN
        airtable_fields 
    ON (
            ads._adid = airtable_fields._adid
        AND 
            ads._campaignid = airtable_fields._campaignid
    )
    OR (
            airtable_fields._adid IS NULL
        AND 
            ads._campaignid = airtable_fields._campaignid
    )

    LEFT JOIN 
        campaign_fields
    ON 
        ads._campaignid = campaign_fields._campaignid

),

-- Add campaign numbers to each ad
campaign_numbers AS (

    SELECT
        *
    FROM
        combined_data 

    -- Get accounts that are being targeted
    LEFT JOIN (
        
        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _target_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
            
            JOIN 
                `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

        )
        GROUP BY 
            1

    ) target

    USING(_campaignid)

    -- Get accounts that have been reached
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _reached_accounts

        FROM (

            SELECT DISTINCT 

                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid

            FROM 
                `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
            
            JOIN 
                `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` extra

            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain,
                _campaignid
            )
            
        )
        GROUP BY 
            1

    ) reach

    USING(_campaignid)

    -- Get accounts that are 6QA
    LEFT JOIN (

        SELECT DISTINCT

            _campaignid,
            COUNT(*) AS _6qa_accounts
        
        FROM (
            
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,

            FROM 
                `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
            
            JOIN 
                `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
            
            ON 
                main._segmentname = side._segment

            JOIN 
                `smartcommnam.db_6sense_account_current_state` extra
            
            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            )

            WHERE 
                extra._6qa_date IS NOT NULL

        )
        GROUP BY 
            1

    )

    USING(_campaignid)

    -- Get actr for each campaign - click divide reach
    LEFT JOIN (
      WITH main AS (
        SELECT DISTINCT 
          main._6sensecompanyname,
          main._6sensecountry,
          main._6sensedomain,
          main._segmentname,
          side._campaignid,
          CASE WHEN extra._clicks != '0' THEN 1 ELSE 0 END AS _clicked_account,
          CASE WHEN extra._campaignid != '' THEN 1 ELSE 0 END AS _reached_account

        FROM 
          `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
        JOIN 
          `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
        ON 
          main._segmentname = side._segment
      JOIN 
          `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` extra
        USING(
          _6sensecompanyname,
          _6sensecountry,
          _6sensedomain,
          _campaignid
        )
      )
      SELECT
          _campaignid,
          SAFE_DIVIDE(SUM(_clicked_account), SUM(_reached_account)) AS _accountctr  
      FROM main
      GROUP BY _campaignid
    )

    USING(_campaignid)

),


-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (

    SELECT
    
        *,
        
        COUNT(*) OVER (
            PARTITION BY _campaignid
        ) 
        AS _occurrence

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


-- Insert Linkedin data into ad performance
INSERT INTO `smartcommnam.db_6sense_ad_performance` (
_adid,
_date,
_spend,
_clicks,
_impressions,
_campaign_type,
_campaignid,
_campaign_name,
_campaign_status,
_accountctr,
_accountvtr,
_start_date,
_end_date,
_budget,
_advariation,
_adgroup,
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
_reduced_6qa_accounts
)
WITH
linkedin_ads AS (
  SELECT
    CAST(creative_id AS STRING) AS _adid,
    CAST(start_at AS DATE) AS _date,
    SUM(cost_in_usd) AS _spend, 
    SUM(clicks) AS _clicks, 
    SUM(impressions) AS _impressions,
    'Linkedin' AS _campaign_type
  FROM
    `smartcomm_linkedin_ads.ad_analytics_by_creative`
  GROUP BY creative_id, start_at
),
creative AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    CAST(campaign_id AS STRING) AS _campaign_id
  FROM `smartcomm_linkedin_ads.creatives`
),
campaign AS (
  SELECT
    name AS _campaign_name,
    CAST(id AS STRING) AS id,
    '' AS _campaign_status,
    CAST(NULL AS FLOAT64) AS _accountctr,
    CAST(NULL AS STRING) AS _accountvtr,
    CAST(NULL AS DATE) AS _start_date,
    CAST(NULL AS DATE) AS _end_date,
    CAST(NULL AS FLOAT64) AS _budget,
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
  FROM `smartcomm_linkedin_ads.campaigns`
)
SELECT
  linkedin_ads.*,
  creative.* EXCEPT(cID),
  campaign.* EXCEPT (id)
FROM linkedin_ads
LEFT JOIN creative
ON linkedin_ads._adid = creative.cID
LEFT JOIN campaign
ON campaign.id = creative._campaign_id;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT+ENGAGEMENT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `smartcommnam.db_6sense_engagement_log` AS

WITH target_accounts AS (

    SELECT * FROM `smartcommnam.db_6sense_account_current_state`

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
        side._campaignname,
        CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account
    
    FROM 
        `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` main
    
    JOIN (

        SELECT DISTINCT 

            _campaignid, 
            _campaignname,  
            -- _campaigntype
            
        FROM
            `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense`

    ) side

    USING(_campaignid)

),

-- Get campaign reached engagement for 6sense
sixsense_campaign_reached AS (

    SELECT DISTINCT 

        -- CAST(NULL AS STRING) AS _email, 
        _country_account, 
        -- CAST(NULL AS STRING) AS _city,
        -- CAST(NULL AS STRING) AS _state,

        MIN(_latestimpression) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) 
        AS _timestamp,

        '6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaignname AS _description, 
        1 AS _notes

    FROM
        reached_accounts_data
    -- WHERE
    --     _campaigntype = '6sense Advertising'

),

-- Get ad clicks engagement for 6sense
sixsense_ad_clicks AS (

    SELECT
        * EXCEPT(_old_notes)
    FROM (

        SELECT DISTINCT 

            -- CAST(NULL AS STRING) AS _email, 
            _country_account, 
            -- CAST(NULL AS STRING) AS _city,
            -- CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6sense Ad Clicks' AS _engagement, 
            '6sense' AS _engagement_data_source,
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
        -- AND
        --     _campaigntype = '6sense Advertising'

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

            -- CAST(NULL AS STRING) AS _email, 
            _country_account, 
            -- CAST(NULL AS STRING) AS _city,
            -- CAST(NULL AS STRING) AS _state,
            _activities_on AS _timestamp,
            '6sense Influenced Form Fill' AS _engagement, 
            '6sense' AS _engagement_data_source,
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
        -- AND
        --     _campaigntype = '6sense Advertising'

    )

    -- Get those who have increased in numbers from the last period
    WHERE 
        (_notes - COALESCE(_old_notes, 0)) >= 1

),

account_activity_summary AS (
  SELECT
    _activitytype,
    _activitytarget,
    -- _contactname,
    -- _contactemail,
    _6sensecompanyname,
    _companyinfo,
    REGEXP_EXTRACT(_companyinfo, r'^(.*?) -') AS _6sensecountry,
    REGEXP_EXTRACT(_companyinfo, r'- (.*?)$') AS _6sensedomain,
    CASE 
      WHEN _activitydate LIKE '%/%'
      THEN PARSE_DATE('%m/%e/%Y', _activitydate)
      ELSE PARSE_DATE('%F', _activitydate)
    END  
    AS _activitydate,
    COUNT(*) AS _count

  FROM
    `smartcommnam_mysql.smartcommnam_db_6sense_activity_summary_nam`
  GROUP BY ALL
),
acccount_activity_summary_main AS (
  SELECT 
    _activitytype,
    _activitytarget,
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
    _activitydate,
    _count
  FROM account_activity_summary
),
acccount_activity_summary_keyword_researched AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'KW Research'
),
account_activity_summary_web_visited AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'Website Visit'
),
account_activity_summary_bombora_topics AS (
  SELECT DISTINCT
    _country_account,
    _activitydate AS _timestamp,
    _activitytype AS _engagement, 
    'Activity Summary Account' AS _engagement_data_source,
    _activitytarget AS _description,
    _count AS _notes
  FROM acccount_activity_summary_main
  WHERE _activitytype = 'Current Bombora Company Surge Topics'
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
        SELECT * FROM acccount_activity_summary_keyword_researched
        UNION DISTINCT
        SELECT * FROM account_activity_summary_web_visited
        UNION DISTINCT
        SELECT * FROM account_activity_summary_bombora_topics
        
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
        SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_campaign_reached,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_ad_clicks,
        -- SUM(CASE WHEN _engagement = 'LinkedIn Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_li_form_fills,
        -- SUM(CASE WHEN _engagement = 'SEM Engagement' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_sem_engagements,
        SUM(CASE WHEN _engagement = 'Website Visit' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_webpage_visits,
        SUM(CASE WHEN _engagement = 'KW Research' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = 'Current Bombora Company Surge Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics
        -- SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
        -- SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click

    FROM 
        combined_data
        
)

SELECT * FROM accumulated_engagement_values;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT PERFORMANCES
----------------------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE TABLE `smartcommnam.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        -- CASE
        --     WHEN main.segment_name = 'S4_HANA - NA' THEN 'S4/HANA - NA'
        --     WHEN main.segment_name = 'S4_HANA - EMEA' THEN 'S4/HANA - EMEA'
        --     WHEN main.segment_name = 'S4/HANA - APJ' THEN 'S4/HANA - APJ'
        --     ELSE main.segment_name
        -- END AS _segmentname,              
        side._segment,
        side._campaignid,
        side._campaignname
    FROM 
        `smartcommnam_mysql.smartcommnam_db_6sense_target_accounts_nam` main
    
    JOIN 
        `smartcommnam_mysql.smartcommnam_optimization_airtable_ads_6sense` side
    
    ON 
        main._segmentname = side._segment

),

-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (

    SELECT DISTINCT 

        main.* EXCEPT(_campaignid),

        CASE 
            WHEN side._campaignid IS NOT NULL 
            THEN true 
        END 
        AS _is_reached,

        CASE 
            WHEN SAFE_CAST(side._clicks AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_clicks,

        CASE 
            WHEN SAFE_CAST(side._impressions AS INTEGER) > 0 
            THEN true 
        END 
        AS _has_impressions

    FROM 
        target_accounts AS main

    LEFT JOIN
    
        `smartcommnam_mysql.smartcommnam_db_6sense_reached_accounts_nam` side

    USING(
        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _campaignid
    )

)

SELECT * FROM reached_accounts;

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- ACCOUNT ACTIVITY SUMMARY (SOURCE FROM MYSQL)
----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `smartcommnam.db_6sense_activity_summary`
CLUSTER BY _activitytype
AS
SELECT * FROM `smartcommnam_mysql.smartcommnam_db_6sense_activity_summary_nam`;