CREATE TEMP TABLE TEMP_ridecell_db_buying_stage AS
SELECT
  buying_stage.* EXCEPT (_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version),
  master_list. * EXCEPT (_sdc_batched_at, _sdc_extracted_at, _sdc_received_at, _sdc_sequence, _sdc_table_version)
FROM `x-marketing.ridecell_mysql.ridecell_db_buying_stage` buying_stage
JOIN `x-marketing.ridecell_master_list.Master_List` master_list
  ON master_list._6sense_name = buying_stage._6sensecompanyname 
  AND master_list._6sense_domain = buying_stage._6sensedomain 
  AND master_list._6sense_country = buying_stage._6sensecountry;

CREATE TEMP TABLE TEMP_ridecell_db_6s_target_account AS
SELECT
  target.* EXCEPT (_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version),
  master_list. * EXCEPT (_sdc_batched_at, _sdc_extracted_at, _sdc_received_at, _sdc_sequence, _sdc_table_version)
FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` target
JOIN `x-marketing.ridecell_master_list.Master_List` master_list
  ON master_list._6sense_name = target._6sensecompanyname 
  AND master_list._6sense_domain = target._6sensedomain 
  AND master_list._6sense_country = target._6sensecountry;

CREATE TEMP TABLE TEMP_ridecell_db_6s_reached_account AS
SELECT
  reached.* EXCEPT (_sdc_batched_at, _sdc_received_at, _sdc_sequence, _sdc_table_version),
  master_list. * EXCEPT (_sdc_batched_at, _sdc_extracted_at, _sdc_received_at, _sdc_sequence, _sdc_table_version)
FROM `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` reached
JOIN `x-marketing.ridecell_master_list.Master_List` master_list
  ON master_list._6sense_name = reached._6sensecompanyname 
  AND master_list._6sense_domain = reached._6sensedomain 
  AND master_list._6sense_country = reached._6sensecountry;




CREATE OR REPLACE TABLE `ridecell.db_6sense_buying_stages_movement` AS
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
        SELECT DISTINCT
            ROW_NUMBER() OVER (PARTITION BY sfdc_account_name, sfdc_billing_country, sfdc_website ORDER BY
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate) END DESC) AS _rownum,
            CASE
                WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
                ELSE PARSE_DATE('%F', _extractdate)
            END AS _activities_on,
            sfdc_account_name AS _6sensecompanyname,
            sf_account_18_id AS sf_account_id,
            sfdc_billing_country AS _6sensecountry, 
            sfdc_website AS _6sensedomain,
            CONCAT(sfdc_account_name, sfdc_billing_country, sfdc_website) AS _country_account,
            '6sense' AS _data_source,
            _buyingstagestart AS _previous_stage,
            _buyingstageend AS _current_stage
        FROM `TEMP_ridecell_db_buying_stage` buying_stage
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT
            main.* EXCEPT (_rownum),
            prev._order AS _previous_stage_order,
            curr._order AS _current_stage_order,
            CASE
                WHEN curr._order > prev._order THEN '+ve'
                WHEN prev._order > curr._order THEN '-ve'
                ELSE 'Stagnant'
            END AS _movement
        FROM sixsense_buying_stage_data AS main
        LEFT JOIN sixsense_stage_order AS prev
            ON main._previous_stage = prev._buying_stage
        LEFT JOIN sixsense_stage_order AS curr
            ON main._current_stage = curr._buying_stage
        WHERE main._rownum = 1
    )
SELECT *
FROM latest_sixsense_buying_stage_with_order_and_movement;




CREATE OR REPLACE TABLE `ridecell.db_6sense_account_current_state` AS

WITH target_accounts AS (
    SELECT DISTINCT
        main.*
    FROM (
        SELECT DISTINCT
            target6s.sfdc_account_name AS _6sensecompanyname,
            target6s.sfdc_billing_country AS _6sensecountry,
            target6s.sfdc_website AS _6sensedomain,
            target6s._industrylegacy AS _6senseindustry,
            target6s._6senseemployeerange,
            target6s._6senserevenuerange,
            salesforce.ridecell_industry__c AS _ridecell_industry, 
            salesforce.account_tier__c AS _account_tier, 
            salesforce.x18_digit_id__c AS sfdc_account_18_digit_id, 
            salesforce.company_linkedin_url__c AS _linkedin, 
            salesforce.original_lead_source_details__c, 
            salesforce.most_recent_lead_source_details__c, 
            salesforce.most_recent_lead_source__c,
            user.name AS user_name,
            CASE
                WHEN target6s._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', target6s._extractdate)
                ELSE PARSE_DATE('%F', target6s._extractdate)
            END AS _added_on,
            '6sense' AS _data_source,
            CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) AS _country_account
        FROM `x-marketing.ridecell_salesforce.Account` salesforce 
        JOIN `TEMP_ridecell_db_6s_target_account` target6s
            ON target6s.sfdc_account_name = salesforce.name
            AND target6s.sfdc_billing_country = salesforce.billingcountry
            AND target6s.sfdc_website = salesforce.website 
        LEFT JOIN `x-marketing.ridecell_salesforce.User` user 
            ON user.id = salesforce.ownerid
        ) main
            -- Get the earliest date of appearance of each account
            JOIN (
                SELECT DISTINCT 
                    MIN(CASE
                            WHEN target6s._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', target6s._extractdate)
                            ELSE PARSE_DATE('%F', target6s._extractdate)
                        END ) AS _added_on,
                    CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) AS _country_account
                FROM `TEMP_ridecell_db_6s_target_account` target6s
                GROUP BY 2
            ) scenario 
            ON main._country_account = scenario._country_account 
            AND main._added_on = scenario._added_on
    ),
    reached_related_info AS (  
        SELECT DISTINCT
            MIN(CASE
                    WHEN reached._extractdate LIKE '%/%' AND reached._latestimpression LIKE '%/%'
                    THEN PARSE_DATE('%m/%e/%Y', reached._latestimpression)
                    ELSE PARSE_DATE('%F', reached._latestimpression)
                END) OVER (PARTITION BY CONCAT(reached.sfdc_account_name, reached.sfdc_billing_country, reached.sfdc_website)) AS _first_impressions,
            CASE
                WHEN reached._websiteengagement = '-' THEN CAST(NULL AS STRING)
                ELSE reached._websiteengagement
            END AS _websiteengagement,
            CONCAT(reached.sfdc_account_name, reached.sfdc_billing_country, reached.sfdc_website) AS _country_account
        FROM `TEMP_ridecell_db_6s_reached_account` reached
        WHERE _campaignid IN (  SELECT DISTINCT 
                                    CAST(_campaignid AS STRING)
                                FROM `x-marketing.ridecell_campaign.Campaigns`
                                WHERE _campaignid IS NOT NULL)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(reached.sfdc_account_name, reached.sfdc_billing_country, reached.sfdc_website) ORDER BY CASE
            WHEN reached._extractdate LIKE '%/%' AND reached._latestimpression LIKE '%/%'
            THEN PARSE_DATE('%m/%e/%Y', reached._latestimpression)
            ELSE PARSE_DATE('%F', reached._latestimpression)
            END DESC) = 1      
    ),
    six_qa_related_info AS (
      WITH _6qa_list_sfdc AS (
        SELECT DISTINCT name, 
              website, 
              billingcountry, 
              account6qa6sense__c, 
              account6qastartdate6sense__c 
        FROM `x-marketing.ridecell_salesforce.Account`
        WHERE account6qa6sense__c = true
        )
        SELECT 
            account6qastartdate6sense__c AS _6qa_date,
            true AS _is_6qa,
            CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) AS _country_account
        FROM _6qa_list_sfdc sfdc 
        JOIN `TEMP_ridecell_db_6s_target_account` target6s
            ON target6s.sfdc_account_name = sfdc.name 
            AND target6s.sfdc_website = sfdc.website 
            AND target6s.sfdc_billing_country = sfdc.billingcountry
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) ORDER BY sfdc.account6qastartdate6sense__c) = 1
    ),
-- Get buying stage info for each account
    buying_stage_related_info AS (
        SELECT DISTINCT
            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,
        FROM `ridecell.db_6sense_buying_stages_movement`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _country_account ORDER BY _activities_on DESC) = 1
    ),
-- Attach all other data parts to target accounts
    combined_data AS (
        SELECT DISTINCT 
            target.*, 
            reached.* EXCEPT(_country_account),
            stage.* EXCEPT(_country_account),
            six_qa.* EXCEPT(_country_account)   
        FROM target_accounts AS target
        LEFT JOIN reached_related_info AS reached 
            USING (_country_account)
        LEFT JOIN buying_stage_related_info AS stage
            USING(_country_account) 
        LEFT JOIN six_qa_related_info AS six_qa
            USING(_country_account)
    )
    SELECT * FROM combined_data;




CREATE OR REPLACE TABLE `ridecell.db_6sense_ad_performance` AS
WITH ads AS (
    SELECT DISTINCT
        _campaignid,
        _name AS _advariation,
        _6senseid AS _adid,
        _accountvtr,
        CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
        CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
        SAFE_CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
        CASE 
            WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
            WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
        END AS _date
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_campaign_performance`
    WHERE _datatype = 'Ad'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid, _6senseid, _date ORDER BY
        CASE 
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate) END) = 1
),
-- Get campaign level fields
campaign_fields AS (
    SELECT
        _campaignid,
        _accountvtr,
        _linkedincampaignid,
        CASE 
            WHEN _budget = '-' THEN NULL
            ELSE SAFE_CAST(REGEXP_REPLACE(_budget, r'[^0-9.-]', '') AS FLOAT64)
        END AS _budget,
        CASE 
            WHEN _startDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _startDate)
            WHEN _startDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _startDate)
        END AS _start_date,
        CASE 
            WHEN _endDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _endDate)
            WHEN _endDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _endDate)
        END AS _end_date,
        _status AS _campaign_status,
        _name AS _campaign_name,
        _campaigntype AS _campaign_type, 
        CASE 
            WHEN _accountsnewlyengagedlifetime = '-' THEN 0
            ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
        END AS _newly_engaged_accounts,
        CASE 
            WHEN _accountswithincreasedengagementlifetime = '-' THEN 0
            ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
        END AS _increased_engagement_accounts
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_campaign_performance`
    WHERE _datatype = 'Campaign'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignid ORDER BY
        CASE
            WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
            ELSE PARSE_DATE('%F', _extractdate) END DESC) = 1
),
ads_campaign_combined AS (
    SELECT ads.*,
        campaign_fields._linkedincampaignid,
        campaign_fields._campaign_name,
        campaign_fields._campaign_type,
        campaign_fields._campaign_status,
        campaign_fields._start_date,
        campaign_fields._end_date,
        campaign_fields._budget,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts
    FROM ads
    JOIN campaign_fields
    ON ads._campaignid = campaign_fields._campaignid
),
airtable_fields AS (
    SELECT DISTINCT 
        _campaignid, 
        ad_id,
        ad_group,
        '' AS _screenshot
    FROM `x-marketing.ridecell_campaign.Campaigns` 
    WHERE _campaignid IS NOT NULL
),
combined_data AS (
    SELECT
        ads_campaign_combined.*,
        airtable_fields.ad_group,
        '' AS _screenshot
    FROM ads_campaign_combined
    LEFT JOIN airtable_fields 
    ON (
        (ads_campaign_combined._adid = CAST(airtable_fields.ad_id AS STRING)
        AND ads_campaign_combined._campaignid = CAST(airtable_fields._campaignid AS STRING))
        OR (airtable_fields.ad_id IS NULL
        AND ads_campaign_combined._campaignid = CAST(airtable_fields._campaignid AS STRING))
    )
    LEFT JOIN campaign_fields
    ON ads_campaign_combined._campaignid = CAST(campaign_fields._campaignid AS STRING)
),
-- Add campaign numbers to each ad
campaign_numbers AS (
    SELECT *
    FROM combined_data 
    -- Get accounts that are being targeted
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid, 
            COUNT(*) AS _target_accounts
        FROM (
            SELECT DISTINCT 
                main.sfdc_account_name AS _6sensecompanyname,
                main.sfdc_billing_country AS _6sensecountry,
                main.sfdc_website AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `TEMP_ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
            ON main._segmentname = side.segment_name
        )
        GROUP BY 1
    ) target
    USING(_campaignid)
    -- Get accounts that have been reached
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid, 
            COUNT(*) AS _reached_accounts
        FROM (
            SELECT DISTINCT 
                main.sfdc_account_name AS _6sensecompanyname,
                main.sfdc_billing_country AS _6sensecountry,
                main.sfdc_website AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `TEMP_ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
                ON main._segmentname = side.segment_name
            JOIN `TEMP_ridecell_db_6s_reached_account` extra
                ON main._6sensecompanyname = extra.sfdc_account_name 
                AND main._6sensecountry = extra.sfdc_billing_country 
                AND main._6sensedomain = extra.sfdc_website 
                AND CAST(side._campaignid AS STRING) = extra._campaignid

        )
        GROUP BY 1
    ) reach
    USING(_campaignid)
    -- Get accounts that are 6QA
    LEFT JOIN (
        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _6qa_accounts
        FROM (
            SELECT DISTINCT 
                main.sfdc_account_name AS _6sensecompanyname,
                main.sfdc_billing_country AS _6sensecountry,
                main.sfdc_website AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `TEMP_ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
            ON main._segmentname = side.segment_name
            JOIN `ridecell.db_6sense_account_current_state` extra
            USING(_6sensecompanyname, _6sensecountry, _6sensedomain)
            WHERE extra._6qa_date IS NOT NULL
        )
        GROUP BY 1
    )
    USING(_campaignid)    
    -- Get actr for each campaign - click divide reach
    LEFT JOIN (
      WITH main AS (
        SELECT DISTINCT 
        main.sfdc_account_name AS _6sensecompanyname,
        main.sfdc_billing_country AS _6sensecountry,
        main.sfdc_website AS _6sensedomain,
        main._segmentname,
        CAST(side._campaignid AS STRING) AS _campaignid,  -- Cast NUMERIC to STRING
        CASE WHEN extra._clicks != '0' THEN 1 ELSE 0 END AS _clicked_account,
        CASE WHEN extra._campaignid != '' THEN 1 ELSE 0 END AS _reached_account
        FROM `TEMP_ridecell_db_6s_target_account` main
        JOIN `x-marketing.ridecell.6sense_campaign_list` side 
        ON main._segmentname = side.segment_name
        JOIN `TEMP_ridecell_db_6s_reached_account` extra
            ON main._6sensecompanyname = extra.sfdc_account_name 
            AND main._6sensecountry = extra.sfdc_billing_country 
            AND main._6sensedomain = extra.sfdc_website 
        AND CAST(side._campaignid AS STRING) = extra._campaignid
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
        COUNT(*) OVER (PARTITION BY _campaignid) AS _occurrence
    FROM campaign_numbers
),
-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (
    SELECT
        *,
        _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
        _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
        _target_accounts / _occurrence AS _reduced_target_accounts,
        _reached_accounts / _occurrence AS _reduced_reached_accounts
    FROM total_ad_occurrence_per_campaign
)
SELECT * 
FROM reduced_campaign_numbers;





CREATE OR REPLACE TABLE `ridecell.db_6sense_engagement_log` AS

WITH target_accounts AS (
    SELECT *
    FROM `ridecell.db_6sense_account_current_state`
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
            END AS _latestimpression,
            CASE 
                WHEN main._extractdate LIKE '%/%'
                THEN PARSE_DATE('%m/%e/%Y', main._extractdate)
                ELSE PARSE_DATE('%F', main._extractdate)
            END AS _activities_on, 
            main._campaignid,
            -- Need label to distingush 6sense and Linkedin campaigns
            side.campaign_name AS _campaignname,
            CONCAT(main.sfdc_account_name, main.sfdc_billing_country, main.sfdc_website) AS _country_account
        FROM `TEMP_ridecell_db_6s_reached_account` main
        JOIN (
            SELECT DISTINCT 
                CAST(_campaignid AS STRING) AS _campaignid, 
                campaign_name,
            FROM `x-marketing.ridecell_campaign.Campaigns`
        ) side
        USING(_campaignid)
    ),

-- Get campaign reached engagement for 6sense
    sixsense_campaign_reached AS (
        SELECT DISTINCT 
            _country_account, 
            '' AS sfdc_account_18_digit_id,
            MIN(_latestimpression) OVER(PARTITION BY _country_account, _campaignname
                ORDER BY _latestimpression) AS _timestamp,
            '6sense Campaign Reached' AS _engagement,
            '6sense' AS _engagement_data_source, 
            _campaignname AS _description, 
            1 AS _notes
        FROM reached_accounts_data
    ),

-- Get ad clicks engagement for 6sense
    sixsense_ad_clicks AS (
        SELECT
            * EXCEPT(_old_notes)
        FROM (
            SELECT DISTINCT 
                _country_account,
                '' AS sfdc_account_18_digit_id, 
                _activities_on AS _timestamp,
                '6sense Ad Clicks' AS _engagement, 
                '6sense' AS _engagement_data_source,
                _campaignname AS _description,  
                _clicks AS _notes,
                -- Get last period's clicks to compare
                LAG(_clicks) OVER(
                    PARTITION BY _country_account, _campaignname
                    ORDER BY _activities_on
                ) AS _old_notes
            FROM reached_accounts_data 
            WHERE _clicks >= 1
        )
        -- Get those who have increased in numbers from the last period
        WHERE (_notes - COALESCE(_old_notes, 0)) >= 1
    ),

    -- Get form fills engagement for 6sense
    sixsense_form_fills AS (
        SELECT
            * EXCEPT(_old_notes)
        FROM (
            SELECT DISTINCT  
                _country_account, 
                '' AS sfdc_account_18_digit_id,
                _activities_on AS _timestamp,
                '6sense Influenced Form Fill' AS _engagement, 
                '6sense' AS _engagement_data_source,
                _campaignname AS _description,  
                _influencedformfills AS _notes,
                -- Get last period's clicks to compare
                LAG(_influencedformfills) OVER(
                    PARTITION BY _country_account, _campaignname
                    ORDER BY _activities_on) AS _old_notes
            FROM reached_accounts_data 
            WHERE _influencedformfills >= 1
        )
        -- Get those who have increased in numbers from the last period
        WHERE (_notes - COALESCE(_old_notes, 0)) >= 1
    ),

    sales_intelligence_data AS (
      with sintel_report AS (
        SELECT 
          _activitytype,
          _activitytarget,
          _contactname,
          _email,
          _accountname,
          _contactcountry,
          _contactcity AS _city,
          _contactstate AS _state,
          _websiteaddress AS _domain,
          CASE 
              WHEN _crmaccountid LIKE 'CMA%' THEN SUBSTR(_crmaccountid, 4)  -- Removes 'CMA' by starting from the 5th character
              ELSE _crmaccountid  -- Keeps the original value if 'CMA' is not present
          END AS _crmaccountid,
          CASE 
              WHEN _activitydate LIKE '%/%'
              THEN PARSE_DATE('%m/%e/%Y', _activitydate)
              ELSE PARSE_DATE('%F', _activitydate)
          END  
          AS _date,
        FROM `x-marketing.ridecell_mysql.ridecell_db_sales_intelligence_report`
      ),
      sfdc as (
        select 
          name AS sfdc_account_name,
          website AS sfdc_website,
          billingcountry AS sfdc_billing_country,
          ridecell_industry__c AS _ridecell_industry,
          account_tier__c AS _account_tier,
          company_linkedin_url__c AS _linkedin,
          original_lead_source_details__c,
          most_recent_lead_source_details__c,
          most_recent_lead_source__c,
          x18_digit_id__c AS sfdc_account_18_digit_id
        from `x-marketing.ridecell_salesforce.Account`
      )
      select
        sintel_report.*,
        sfdc.*,
        '' AS user_name,
        CONCAT(sfdc.sfdc_account_name, sfdc.sfdc_billing_country, sfdc.sfdc_website) AS _country_account,
        COUNT(*) AS _count
      from sintel_report
      left join sfdc
        ON sfdc.sfdc_account_18_digit_id = sintel_report._crmaccountid
      GROUP BY ALL
),

    sales_intelligence_campaign_reached AS (

        SELECT DISTINCT 
            _country_account, 
            sfdc_account_18_digit_id,
            _date AS _timestamp, 
            _activitytype AS _engagement,
            'Sales Intelligence' AS _engagement_data_source,
            _activitytarget AS _description,
            _count AS _notes
        FROM
            sales_intelligence_data
        WHERE
            _activitytype LIKE '%Reached%'
    ),

    sales_intelligence_ad_clicks AS (

        SELECT DISTINCT 
            _country_account,
            sfdc_account_18_digit_id, 
            _date AS _timestamp, 
            _activitytype AS _engagement,
            'Sales Intelligence' AS _engagement_data_source,
            _activitytarget AS _description,
            _count AS _notes
        FROM
            sales_intelligence_data
        WHERE
            _activitytype LIKE '%Ad Clicks%'

    ),

    web_visits AS (
        SELECT DISTINCT
            _country_account, 
            sfdc_account_18_digit_id,
            _date AS _timestamp, 
            '6sense Web Visits' AS _engagement, 
            'Sales Intelligence' AS _engagement_data_source,
            _activitytarget AS _description,
            _count AS _notes
        FROM
            sales_intelligence_data
        WHERE
            _activitytype LIKE '%Web Visit%'
    ),

    searched_keywords AS (
        SELECT DISTINCT 
            _country_account,
            sfdc_account_18_digit_id, 
            _date AS _timestamp, 
            '6sense Searched Keywords' AS _engagement, 
            'Sales Intelligence' AS _engagement_data_source,
            _activitytarget AS _description,
            _count AS _notes

        FROM
            sales_intelligence_data
        WHERE
            _activitytype LIKE '%KW Research%'

    ),

    email_engagements AS (
        SELECT DISTINCT 
            _country_account,
            sfdc_account_18_digit_id, 
            _date AS _timestamp, 
            CONCAT(_activitytype, 'ed') AS _engagement, 
            'Sales Intelligence' AS _engagement_data_source,
            _activitytarget AS _description,
            _count AS _notes

        FROM
            sales_intelligence_data
        WHERE
            REGEXP_CONTAINS(_activitytype,'Email Open|Email Click')

    ),

    other_engagements AS (
        SELECT DISTINCT 
            _country_account,
            sfdc_account_18_digit_id, 
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
            'Sales Intelligence' AS _engagement_data_source,
            CASE   
                WHEN _activitytarget = ''
                THEN CONCAT('Contact: ', IF(_contactname != '', _contactname, _email))
                ELSE _activitytarget
            END 
            AS _description,  
            _count AS _notes
        FROM
            sales_intelligence_data
        WHERE NOT REGEXP_CONTAINS (_activitytype,'Reached|Ad Click|Web Visit|KW Research|Email Open|Email Click')

    ),

    -- Only activities involving target accounts are considered
    combined_data_6sense AS (
        SELECT DISTINCT 
            target_accounts.*,
            activities.* EXCEPT(_country_account, sfdc_account_18_digit_id)
        FROM (
            SELECT * FROM sixsense_campaign_reached 
            UNION DISTINCT
            SELECT * FROM sixsense_ad_clicks 
            UNION DISTINCT
            SELECT * FROM sixsense_form_fills
        ) activities
    JOIN target_accounts
    USING (_country_account)
    ),

    six_qa_related_info AS (
    WITH _6qa_list_sfdc AS (
        SELECT DISTINCT name, 
            website, 
            billingcountry, 
            account6qa6sense__c, 
            account6qastartdate6sense__c,
            x18_digit_id__c AS sf_account_id
        FROM `x-marketing.ridecell_salesforce.Account`
        WHERE account6qa6sense__c = true
    )
    SELECT 
        account6qastartdate6sense__c AS _6qa_date,
        true AS _is_6qa,
        CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) AS _country_account,
        sfdc.sf_account_id
    FROM _6qa_list_sfdc sfdc 
    JOIN `TEMP_ridecell_db_6s_target_account` target6s
        ON target6s.sfdc_account_name = sfdc.name 
        AND target6s.sfdc_website = sfdc.website 
        AND target6s.sfdc_billing_country = sfdc.billingcountry
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CONCAT(target6s.sfdc_account_name, target6s.sfdc_billing_country, target6s.sfdc_website) ORDER BY sfdc.account6qastartdate6sense__c) = 1

    ),
-- Get buying stage info for each account
    buying_stage_related_info AS (
        SELECT DISTINCT
            _previous_stage,
            _previous_stage_order,
            _current_stage,
            _current_stage_order,
            _movement,
            _activities_on AS _movement_date,
            _country_account,
            _6sensecompanyname,
            sf_account_id
        FROM `ridecell.db_6sense_buying_stages_movement`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY _country_account ORDER BY _activities_on DESC) = 1
        ),

       combined_data_si AS (
            SELECT DISTINCT 
                _accountname,
                _contactcountry,
                _domain,
                _ridecell_industry,
                '',
                '',
                _ridecell_industry,
                _account_tier,
                sales_intelligence_data.sfdc_account_18_digit_id,
                _linkedin,
                original_lead_source_details__c,
                most_recent_lead_source_details__c,
                most_recent_lead_source__c,
                user_name,
                CAST(NULL AS DATE) AS _added_on,
                '6sense' AS _data_source,
                sales_intelligence_data._country_account,
                CAST(NULL AS DATE) AS _first_impressions,
                CAST(NULL AS STRING) AS _websiteengagement,
                _previous_stage,
                _previous_stage_order,
                _current_stage,
                _current_stage_order,
                _movement,
                _movement_date,
                _6qa_date,
                _is_6qa,
                activities.* EXCEPT(_country_account, sfdc_account_18_digit_id)
            FROM (
                SELECT * FROM sales_intelligence_campaign_reached
                UNION DISTINCT 
                SELECT * FROM sales_intelligence_ad_clicks
                UNION DISTINCT 
                SELECT * FROM web_visits
                UNION DISTINCT 
                SELECT * FROM searched_keywords
                UNION DISTINCT 
                SELECT * FROM email_engagements
                UNION DISTINCT
                SELECT * FROM other_engagements
            ) activities
            LEFT JOIN six_qa_related_info 
                ON activities.sfdc_account_18_digit_id = sf_account_id
            LEFT JOIN buying_stage_related_info
                ON activities.sfdc_account_18_digit_id = buying_stage_related_info.sf_account_id
            LEFT JOIN sales_intelligence_data
                ON activities.sfdc_account_18_digit_id = sales_intelligence_data.sfdc_account_18_digit_id
       ),

       combined_data AS (
        SELECT * FROM combined_data_6sense
        UNION ALL
        SELECT * FROM combined_data_si
       ),

-- Get accumulated values for each engagement
    accumulated_engagement_values AS (
        SELECT
            *,
            -- The aggregated values
            SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
            SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
            SUM(CASE WHEN _engagement = '6sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
            SUM(CASE WHEN _engagement = '6sense Website Visit' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_webpage_visits,
            SUM(CASE WHEN _engagement = 'Current Bombora Company Surge Topics' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics,
            SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
            SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
            SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
        FROM combined_data     
    )
SELECT * FROM accumulated_engagement_values;



CREATE OR REPLACE TABLE `ridecell.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (
    SELECT DISTINCT 
        main.sfdc_account_name AS _6sensecompanyname,
        main.sfdc_billing_country AS _6sensecountry,
        main.sfdc_website AS _6sensedomain,           
        side.segment_name,
        CAST(side._campaignid AS STRING) AS _campaignid,
        side.campaign_name AS _campaignname
    FROM `TEMP_ridecell_db_6s_target_account` main
    JOIN `x-marketing.ridecell_campaign.Campaigns` side
    ON main._segmentname = side.segment_name
    ),

-- Mark those target accounts that have been reached by their campaigns
    reached_accounts AS (
        SELECT DISTINCT 
            main.* EXCEPT(_campaignid),
        CASE 
            WHEN side._campaignid IS NOT NULL THEN true
            END AS _is_reached,
        CASE 
            WHEN SAFE_CAST(side._clicks AS INTEGER) > 0 THEN true 
            END AS _has_clicks,
        CASE 
            WHEN SAFE_CAST(side._impressions AS INTEGER) > 0 THEN true 
            END AS _has_impressions
        FROM target_accounts AS main
        LEFT JOIN `TEMP_ridecell_db_6s_reached_account` side
        USING(_6sensecompanyname, _6sensecountry, _6sensedomain, _campaignid)
    )
SELECT * FROM reached_accounts;





CREATE OR REPLACE TABLE `ridecell.opportunity_influenced_accelerated` AS

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
        _account_tier,
        CASE
            WHEN _engagement LIKE '%6sense%' THEN '6sense'
            WHEN _engagement LIKE '%LinkedIn%' THEN 'LinkedIn'
        END AS _channel
    FROM `ridecell.db_6sense_engagement_log`
    ),

-- Get all generated opportunities
-- Wont be having the current stage and stage change date in this CTE
    opps_created AS (
        WITH closedConversionRate AS (
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
        )
        SELECT
            DISTINCT opps_main.*
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
            * EXCEPT(_rownum),
            -- Setting the rank of the historical stage based on stage change date
            ROW_NUMBER() OVER(PARTITION BY _opp_id ORDER BY _historical_stage_change_date DESC) AS _stage_rank
        FROM (
            SELECT
                *, 
                -- Those on same day are differentiated by timestamp
                ROW_NUMBER() OVER (PARTITION BY _opp_id, _historical_stage_change_date ORDER BY _historical_stage_change_timestamp DESC) AS _rownum
            FROM opps_historical_stage
        )
        WHERE _rownum = 1
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
SELECT * FROM latest_stage_opportunity_only;





-- Opportunity Influenced + Accelerated Without Engagements

CREATE OR REPLACE TABLE `ridecell.opportunity_summarized` AS

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
      _account_tier,
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
      MAX(_is_influencing_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_influencing_activity,
      _is_accelerated_opp,
      MAX(_is_accelerating_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_accelerating_activity,
      _is_later_accelerated_opp,
      MAX(_is_later_accelerating_activity) OVER (PARTITION BY _opp_id, _channel) AS _is_later_accelerating_activity,
      _is_stagnant_opp,
      _channel
      FROM `ridecell.opportunity_influenced_accelerated`;




