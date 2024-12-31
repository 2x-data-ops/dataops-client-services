

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
            CASE
                WHEN buying_stage._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', buying_stage._extractdate)
                ELSE PARSE_DATE('%F', buying_stage._extractdate)
            END AS _activities_on,
            master_list._sfdcaccountname AS _6sensecompanyname,
            master_list._sfdcbillingcountry AS _6sensecountry, 
            -- sf_account_18_id AS sf_account_id,
            master_list._sfdcwebsite AS _6sensedomain,
            CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account,
            '6sense' AS _data_source,
            buying_stage._buyingstagestart AS _previous_stage,
            buying_stage._buyingstageend AS _current_stage
        FROM `x-marketing.ridecell_mysql.ridecell_db_buying_stage` buying_stage
        JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
            ON buying_stage._6sensecompanyname = master_list._6sensename
            AND buying_stage._6sensecountry = master_list._6sensecountry
            AND buying_stage._6sensedomain = master_list._6sensedomain
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite
            ORDER BY CASE
                WHEN buying_stage._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', buying_stage._extractdate)
                ELSE PARSE_DATE('%F', buying_stage._extractdate) 
            END DESC) = 1
    ),
    latest_sixsense_buying_stage_with_order_and_movement AS (
        SELECT
            main.*,
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
    )
SELECT *
FROM latest_sixsense_buying_stage_with_order_and_movement;



CREATE OR REPLACE TABLE `ridecell.db_6sense_account_current_state` AS

WITH target AS (
  SELECT 
    master_list._sfdcaccountname AS _6sensecompanyname,
    master_list._6sensecountry AS _6sensecountry,
    master_list._sfdcwebsite AS _6sensedomain,
    target._industry AS _6senseindustry,
    target._6senseemployeerange,
    target._6senserevenuerange,
    CASE
        WHEN target._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', target._extractdate)
        ELSE PARSE_DATE('%F', target._extractdate)
    END AS _added_on,
    '6sense' AS _data_source,
    CONCAT(master_list._sfdcaccountname, master_list._6sensecountry, master_list._sfdcwebsite) AS _country_account
  FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` target
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON target._6sensecompanyname = master_list._6sensename
        AND target._6sensecountry = master_list._6sensecountry
        AND target._6sensedomain = master_list._6sensedomain
),
get_salesforce_info AS (
    SELECT
        salesforce.x18_digit_id__c AS sfdc_account_18_digit_id,
        salesforce.name,
        salesforce.billingcountry,
        salesforce.website,
        salesforce.ridecell_industry__c AS _ridecell_industry, 
        salesforce.account_tier__c AS _account_tier, 
        salesforce.company_linkedin_url__c AS _linkedin, 
        salesforce.original_lead_source_details__c, 
        salesforce.most_recent_lead_source_details__c, 
        salesforce.most_recent_lead_source__c,
        user.name AS user_name
    FROM `x-marketing.ridecell_salesforce.Account` salesforce
    LEFT JOIN `x-marketing.ridecell_salesforce.User` user 
        ON user.id = salesforce.ownerid
),
combined_target_data AS (
    SELECT target.*,
    get_salesforce_info.*
    FROM target
    LEFT JOIN get_salesforce_info
        ON target._6sensecompanyname = get_salesforce_info.name
        AND target._6sensecountry = get_salesforce_info.billingcountry
        AND target._6sensedomain = get_salesforce_info.website
),
--TARGET AGGREGATION DATA--
combined_data_target_aggregation AS (
    SELECT * FROM combined_target_data
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _country_account ORDER BY _added_on ) =1
),
--REACHED AGGREGATION DATA--
reached_related_info AS (
    SELECT
        MIN(reached._latestimpression) OVER (PARTITION BY CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite)) AS _first_impressions,
        CASE
            WHEN reached._websiteengagement = '-' THEN CAST(NULL AS STRING)
            ELSE reached._websiteengagement
        END AS _websiteengagement,
        CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` reached
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON reached._6sensecompanyname = master_list._6sensename
        AND reached._6sensecountry = master_list._6sensecountry
        AND reached._6sensedomain = master_list._6sensedomain
    WHERE _campaignid IN (  SELECT DISTINCT 
                            CAST(_campaignid AS STRING)
                        FROM `x-marketing.ridecell_campaign.Campaigns`
                        WHERE _campaignid IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) ORDER BY reached._latestimpression DESC) = 1  
),
--6QA DATA--
get_six_qa_account_in_sf AS (
    SELECT
        salesforceacc.account6qastartdate6sense__c AS _6qa_date,
        true AS _is_6qa,
        CONCAT(target._6sensecompanyname, target._6sensecountry, target._6sensedomain) AS _country_account
    FROM `x-marketing.ridecell_salesforce.Account` salesforceacc
    JOIN target
        ON target._6sensecompanyname = salesforceacc.name 
        AND target._6sensecountry = salesforceacc.billingcountry 
        AND target._6sensedomain = salesforceacc.website
    WHERE account6qa6sense__c = true
),
--BUYING STAGES DATA--
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
--COMBINED ALL DATA--
combined_all_aggregation AS (
    SELECT
        combined_data_target_aggregation.* EXCEPT(name, billingcountry, website),
        reached_related_info.* EXCEPT(_country_account),
        get_six_qa_account_in_sf.* EXCEPT(_country_account),
        buying_stage_related_info.* EXCEPT(_country_account)
    FROM combined_data_target_aggregation
    LEFT JOIN reached_related_info
        USING (_country_account)
    LEFT JOIN get_six_qa_account_in_sf
        USING (_country_account)
    LEFT JOIN buying_stage_related_info
        USING (_country_account)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _country_account ORDER BY _added_on  ) =1
)

SELECT * FROM combined_all_aggregation;




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
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain
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
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain 
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
                ON main._segmentname = side.segment_name
            JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` extra
              ON main._6sensecompanyname = extra._6sensecompanyname
              AND main._6sensecountry = extra._6sensecountry
              AND main._6sensedomain = extra._6sensedomain
              AND CAST(side._campaignid AS STRING) = CAST(extra._campaignid AS STRING)


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
                master_list._sfdcaccountname AS _6sensecompanyname,
                master_list._sfdcbillingcountry AS _6sensecountry,
                master_list._sfdcwebsite AS _6sensedomain,
                main._segmentname,
                CAST(side._campaignid AS STRING) AS _campaignid
            FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
            JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
                ON main._6sensecompanyname = master_list._6sensename
                AND main._6sensecountry = master_list._6sensecountry
                AND main._6sensedomain = master_list._6sensedomain  
            JOIN `x-marketing.ridecell_campaign.Campaigns` side
            ON main._segmentname = side.segment_name
            JOIN `ridecell.db_6sense_account_current_state` extra
                ON main._6sensecompanyname = extra._6sensecompanyname
                AND main._6sensecountry = extra._6sensecountry
                AND main._6sensedomain = extra._6sensedomain
            WHERE extra._6qa_date IS NOT NULL
        )
        GROUP BY 1
    )
    USING(_campaignid)    
    -- Get actr for each campaign - click divide reach
    LEFT JOIN (
      WITH main AS (
        SELECT DISTINCT 
            master_list._sfdcaccountname AS _6sensecompanyname,
            master_list._sfdcbillingcountry AS _6sensecountry,
            master_list._sfdcwebsite AS _6sensedomain,
            main._segmentname,
        CAST(side._campaignid AS STRING) AS _campaignid,  -- Cast NUMERIC to STRING
        CASE WHEN extra._clicks != '0' THEN 1 ELSE 0 END AS _clicked_account,
        CASE WHEN extra._campaignid != '' THEN 1 ELSE 0 END AS _reached_account
        FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` main
        JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
            ON main._6sensecompanyname = master_list._6sensename
            AND main._6sensecountry = master_list._6sensecountry
            AND main._6sensedomain = master_list._6sensedomain
        JOIN `x-marketing.ridecell.6sense_campaign_list` side 
            ON main._segmentname = side.segment_name
        JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` extra
            ON main._6sensecompanyname = extra._6sensecompanyname
            AND main._6sensecountry = extra._6sensecountry
            AND main._6sensedomain = extra._6sensedomain
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
reached_accounts_data AS (
    SELECT DISTINCT
        CAST(reached._clicks AS INTEGER) AS _clicks,
        CAST(reached._influencedformfills AS INTEGER) AS _influencedformfills,
        reached._latestimpression,
            CASE
            WHEN reached._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', reached._extractdate)
            ELSE PARSE_DATE('%F', reached._extractdate)
        END AS _activities_on, 
        reached._campaignid,
        -- Need label to distingush 6sense and Linkedin campaigns
        side.campaign_name AS _campaignname,
        CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account
FROM `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` reached
JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
    ON reached._6sensecompanyname = master_list._6sensename
    AND reached._6sensecountry = master_list._6sensecountry
    AND reached._6sensedomain = master_list._6sensedomain
JOIN (
            SELECT DISTINCT 
                CAST(_campaignid AS STRING) AS _campaignid, 
                campaign_name,
            FROM `x-marketing.ridecell_campaign.Campaigns`
        ) side
        USING(_campaignid) 
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(reached._6sensecompanyname, reached._6sensecountry, reached._6sensedomain) ORDER BY reached._latestimpression DESC) = 1
),

combined_sixsense_activities AS (
    SELECT * FROM (
        -- Campaign Reached
        SELECT DISTINCT 
            _country_account, 
            '' AS sfdc_account_18_digit_id,
            MIN(DATE(_latestimpression)) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _latestimpression
            ) AS _timestamp,
            '6sense Campaign Reached' AS _engagement,
            '6sense' AS _engagement_data_source, 
            _campaignname AS _description, 
            1 AS _notes,
            NULL as _old_notes  -- Added for uniformity
        FROM reached_accounts_data

        UNION ALL

        -- Ad Clicks
        SELECT DISTINCT 
            _country_account,
            '' AS sfdc_account_18_digit_id, 
            _activities_on AS _timestamp,
            '6sense Ad Clicks' AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaignname AS _description,  
            _clicks AS _notes,
            LAG(_clicks) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            ) AS _old_notes
        FROM reached_accounts_data 
        WHERE _clicks >= 1

        UNION ALL

        -- Form Fills
        SELECT DISTINCT  
            _country_account, 
            '' AS sfdc_account_18_digit_id,
            _activities_on AS _timestamp,
            '6sense Influenced Form Fill' AS _engagement, 
            '6sense' AS _engagement_data_source,
            _campaignname AS _description,  
            _influencedformfills AS _notes,
            LAG(_influencedformfills) OVER(
                PARTITION BY _country_account, _campaignname
                ORDER BY _activities_on
            ) AS _old_notes
        FROM reached_accounts_data 
        WHERE _influencedformfills >= 1
    ) all_activities
    WHERE 
        -- Apply increase check only for clicks and form fills
        (_engagement = '6sense Campaign Reached'
        OR (_notes - COALESCE(_old_notes, 0)) >= 1)
),
sales_intelligence AS (
    SELECT 
        _activitytype,
        _activitytarget,
        _activitymetainfo,
        _contactname,
        _email,
        _accountname,
        CASE 
            WHEN _crmaccountid LIKE 'CMA%' THEN SUBSTR(_crmaccountid, 4)
            ELSE _crmaccountid
        END AS _crmaccountid,
        CASE 
            WHEN _activitydate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _activitydate)
            ELSE PARSE_DATE('%F', _activitydate)
        END AS _date
    FROM `x-marketing.ridecell_mysql.ridecell_db_sales_intelligence_report`    
),
sfdc AS (
    SELECT
        CONCAT(masterlist._sfdcaccountname, masterlist._sfdcbillingcountry, masterlist._sfdcwebsite) AS _country_account,
        salesforce.ridecell_industry__c AS _ridecell_industry,
        salesforce.account_tier__c AS _account_tier,
        salesforce.company_linkedin_url__c AS _linkedin,
        salesforce.original_lead_source_details__c,
        salesforce.most_recent_lead_source_details__c,
        salesforce.most_recent_lead_source__c,
        salesforce.x18_digit_id__c AS sfdc_account_18_digit_id
    FROM `x-marketing.ridecell_salesforce.Account` salesforce
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` masterlist
        ON salesforce.name = masterlist._sfdcaccountname
        AND salesforce.billingcountry = masterlist._sfdcbillingcountry
        AND salesforce.website = masterlist._sfdcwebsite
),
activity_counts AS (
    SELECT 
        sales_intelligence._activitytype,
        sales_intelligence._activitytarget,
        sales_intelligence._activitymetainfo,
        sales_intelligence._contactname,
        sales_intelligence._email,
        sales_intelligence._date,
        sfdc._country_account,
        sfdc.sfdc_account_18_digit_id,
        COUNT(*) AS _count
    FROM sales_intelligence
    JOIN sfdc
        ON sfdc.sfdc_account_18_digit_id = sales_intelligence._crmaccountid
    GROUP BY 
        sales_intelligence._activitytype,
        sales_intelligence._activitytarget,
        sales_intelligence._activitymetainfo,
        sales_intelligence._contactname,
        sales_intelligence._email,
        sales_intelligence._date,
        sfdc._country_account,
        sfdc_account_18_digit_id
),
aggregate_sales_intel_data AS (
    SELECT 
        activity_counts.* EXCEPT (sfdc_account_18_digit_id),
        sfdc_details._ridecell_industry,
        sfdc_details._account_tier,
        sfdc_details._linkedin,
        sfdc_details.original_lead_source_details__c,
        sfdc_details.most_recent_lead_source_details__c,
        sfdc_details.most_recent_lead_source__c,
        sfdc_details.sfdc_account_18_digit_id
    FROM activity_counts
    LEFT JOIN sfdc sfdc_details
        ON sfdc_details.sfdc_account_18_digit_id = activity_counts.sfdc_account_18_digit_id
),

    sales_intelligence_campaign_reached AS (
        SELECT DISTINCT 
        _country_account,
        sfdc_account_18_digit_id,
        _date AS _timestamp,
        CASE 
            -- Campaign reached activities
            WHEN _activitytype LIKE '%Web Visit%' THEN '6sense Web Visits'
            WHEN _activitytype LIKE '%KW Research%' THEN '6sense Searched Keywords'
            WHEN REGEXP_CONTAINS(_activitytype,'Email Open|Email Click') THEN CONCAT(_activitytype, 'ed')
            -- Other engagements
            WHEN REGEXP_CONTAINS(_activitytype,'Bombora') THEN 'Bombora Topic Surged'
            WHEN REGEXP_CONTAINS(_activitytype,'Form Fill') THEN 'Form Filled'
            WHEN REGEXP_CONTAINS(_activitytype,'Email Reply') THEN 'Email Replied'
            WHEN REGEXP_CONTAINS(_activitytype,'Page Click') THEN 'Webpage Clicked'
            WHEN REGEXP_CONTAINS(_activitytype,'Submit') THEN 'Submitted'
            WHEN REGEXP_CONTAINS(_activitytype,'Video Play') THEN 'Video Played'
            WHEN REGEXP_CONTAINS(_activitytype,'Attend') THEN _activitytype
            WHEN REGEXP_CONTAINS(_activitytype,'Register') THEN _activitytype
            ELSE _activitytype -- Keep original activity type for reached/clicks
        END AS _engagement,
        'Sales Intelligence' AS _engagement_data_source,
        CASE   
            WHEN _activitytarget = '' AND 
                 NOT REGEXP_CONTAINS(_activitytype,'Reached|Ad Click|Web Visit|KW Research|Email Open|Email Click')
                THEN CONCAT('Contact: ', IF(_contactname != '', _contactname, _email))
            ELSE _activitytarget
        END AS _description,
        _count AS _notes,
        -- NULL AS _old_notes
    FROM aggregate_sales_intel_data
    ),
    target_w_6sense AS (
        SELECT target_accounts.*,
        combined_sixsense_activities.* EXCEPT (_old_notes, _country_account, sfdc_account_18_digit_id)
        FROM target_accounts
        LEFT JOIN combined_sixsense_activities
            USING (_country_account)
    ),
    target_w_si AS (
        SELECT target_accounts.*,
        sales_intelligence_campaign_reached.* EXCEPT (_country_account, sfdc_account_18_digit_id)
        FROM target_accounts
        LEFT JOIN sales_intelligence_campaign_reached
            USING (_country_account)
    ),
    combined_data AS (
        SELECT * FROM target_w_6sense
        UNION ALL
        SELECT * FROM target_w_si
    ),


-- Get accumulated values for each engagement
    accumulated_engagement_values AS (
        SELECT
            combined_data.*,
            -- The aggregated values
            SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
            SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
            SUM(CASE WHEN _engagement = '6sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
            SUM(CASE WHEN _engagement = '6sense Website Visit' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_webpage_visits,
            SUM(CASE WHEN _engagement = 'Bombora Topic Surged' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics,
            SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
            SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
            SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
        FROM combined_data     
    ),
    no_engagement_column AS (
        SELECT
            _country_account,
            STRING_AGG(DISTINCT _engagement, ',' ORDER BY _engagement) AS distinct_engagement,
            ARRAY_LENGTH(SPLIT(STRING_AGG(DISTINCT _engagement, ',' ORDER BY _engagement), ',')) as engagement_count
        FROM accumulated_engagement_values
        GROUP BY _country_account
    ),
    no_engagement_table AS (
        SELECT
        _country_account,
        CASE
            WHEN REGEXP_CONTAINS(distinct_engagement, 'Bombora Topic Surged')
            AND REGEXP_CONTAINS(distinct_engagement, '6sense Searched Keywords')
            AND REGEXP_CONTAINS(distinct_engagement, '6sense Campaign Reached')
            AND engagement_count = 3 THEN 'True'
            ELSE NULL
        END AS _no_engagement
        FROM no_engagement_column
    )
SELECT
    -- pulling salesforce account name instead of 6sense (analyst requirement)
    accumulated_engagement_values.*,
    no_engagement_table._no_engagement
FROM accumulated_engagement_values
LEFT JOIN no_engagement_table
    USING (_country_account);


CREATE OR REPLACE TABLE `ridecell.db_6sense_account_performance` AS

-- Get all target accounts and their campaigns
WITH target_accounts AS (
    SELECT DISTINCT 
        master_list._sfdcaccountname AS _6sensecompanyname,
        master_list._sfdcbillingcountry AS _6sensecountry,
        master_list._sfdcwebsite AS _6sensedomain,     
        side.segment_name,
        CAST(side._campaignid AS STRING) AS _campaignid,
        side.campaign_name AS _campaignname
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` target
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
      ON target._6sensecompanyname = master_list._6sensename
      AND target._6sensecountry = master_list._6sensecountry
      AND target._6sensedomain = master_list._6sensedomain 
    JOIN `x-marketing.ridecell_campaign.Campaigns` side
      ON target._segmentname = side.segment_name
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
        JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
          ON main._6sensecompanyname = master_list._sfdcaccountname
          AND main._6sensecountry = master_list._sfdcbillingcountry
          AND main._6sensedomain = master_list._sfdcwebsite 
        LEFT JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` side
          ON master_list._6sensename = side._6sensecompanyname
          AND master_list._6sensecountry = side._6sensecountry
          AND master_list._6sensedomain = side._6sensedomain
          AND main._campaignid = side._campaignid
    )
SELECT
    *
FROM reached_accounts;





CREATE OR REPLACE TABLE `ridecell.opportunity_influenced_accelerated` AS

-- Get account engagements of target account 
WITH target_account_engagements AS (
    SELECT DISTINCT 
        -- prev table pull the salesforce account name instead of 6sense
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
    FROM `ridecell.db_6sense_engagement_log` engagement_log
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



