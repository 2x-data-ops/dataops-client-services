
-- 6sense Demand Capture

CREATE OR REPLACE TABLE `sandler.db_6sense_demand_capture` AS

-- Get all target accounts
WITH target_accounts AS (

    SELECT DISTINCT 

        CONCAT(
            _6sensecountry,
            _6sensedomain, 
            _6sensecompanyname
        ) 
        AS _unique_identifier,

        _6sensecompanyname,
        _6sensecountry,
        _6sensedomain,
        _industrylegacy AS _6senseindustry,
        _6senseemployeerange,
        _6senserevenuerange

    FROM 
        `sandler_mysql.db_dcd_target_accounts`

),

-- Get hubspot related fields
hubspot_info AS (

    -- Get hubspot accounts with domain  
    SELECT 
        * EXCEPT(row_num)
    FROM (

        SELECT DISTINCT 

            property_name.value AS account_name,
            property_domain.value AS domain,
            companyid AS _hs_companyid,
            property_sixsense_account_buying_stage.value AS _buying_stage,
            property_sixsense_account_intent_score.value AS _intent_score,
            
            CASE
                WHEN property_sixsense_account_sixqa.value = '1' THEN '6QA'
                WHEN property_sixsense_account_sixqa.value = '0' THEN 'Non-6QA'
                ELSE 'Non-6QA'
            END 
            AS _is_6qa,

            DATE(TIMESTAMP_MILLIS(CAST(property_sixsense_account_sixqa_start_date.value AS INT64))) AS _start_date_6qa,
            DATE(TIMESTAMP_MILLIS(CAST(property_sixsense_account_sixqa_end_date.value AS INT64))) AS _end_date_6qa,

            ROW_NUMBER() OVER (
                PARTITION BY property_domain.value
                ORDER BY property_createdate.value DESC
            ) 
            AS row_num

        FROM 
            `sandler_hubspot.companies` 
        WHERE 
            property_domain.value IS NOT NULL

    )
    WHERE 
        row_num = 1

    UNION ALL

    -- Get hubspot accounts without domain  
    SELECT 
        * EXCEPT(row_num)
    FROM (

        SELECT DISTINCT 

            property_name.value AS account_name,
            property_domain.value AS domain,
            companyid AS _hs_companyid,
            property_sixsense_account_buying_stage.value AS _buying_stage,
            property_sixsense_account_intent_score.value AS _intent_score,
            
            CASE
                WHEN property_sixsense_account_sixqa.value = '1' THEN '6QA'
                WHEN property_sixsense_account_sixqa.value = '0' THEN 'Non-6QA'
                ELSE 'Non-6QA'
            END 
            AS _is_6qa,

            DATE(TIMESTAMP_MILLIS(CAST(property_sixsense_account_sixqa_start_date.value AS INT64))) AS _start_date_6qa,
            DATE(TIMESTAMP_MILLIS(CAST(property_sixsense_account_sixqa_end_date.value AS INT64))) AS _end_date_6qa,

            ROW_NUMBER() OVER (
                PARTITION BY property_name.value
                ORDER BY property_createdate.value DESC
            ) 
            AS row_num

        FROM 
            `sandler_hubspot.companies` 
        WHERE 
            property_domain.value IS NULL
    
    )
    WHERE 
        row_num = 1

),

-- Get all keyword researches from email alert
all_email_alert_keyword_research AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _source_id,
        _accountname,
        _domain,
        
        CASE
            WHEN _timeframe LIKE '%,%' THEN PARSE_DATE('%h %d, %Y', _timeframe) 
            WHEN _timeframe LIKE '%/%' THEN PARSE_DATE('%m/%e/%y', _timeframe) 
            WHEN _timeframe LIKE '%-%' THEN PARSE_DATE('%F', _timeframe)
        END 
        AS _timestamp,

        'Email Alert' AS _source,
        'Keyword Research' AS _engagement, 
        SPLIT(keywords, " (")[OFFSET(0)] AS _description,
        CAST(REGEXP_EXTRACT(keywords, r'\((\d+)\)') AS INTEGER) AS _notes

    FROM
        `sandler_mysql.db_6qa_alert`,
        UNNEST(SPLIT(_keywords, ", ")) AS keywords
    WHERE
        LENGTH(_keywords) > 1
    AND 
        _sdc_deleted_at IS NULL

),

-- Get all web visits from email alert
all_email_alert_web_visit AS (

    SELECT DISTINCT 

        CAST(NULL AS STRING) AS _source_id,
        _accountname,
        _domain,
        
        CASE
            WHEN _timeframe LIKE '%,%' THEN PARSE_DATE('%h %d, %Y', _timeframe) 
            WHEN _timeframe LIKE '%/%' THEN PARSE_DATE('%m/%e/%y', _timeframe) 
            WHEN _timeframe LIKE '%-%' THEN PARSE_DATE('%F', _timeframe)
        END 
        AS _timestamp,

        'Email Alert' AS _source,
        'Web Visit' AS _engagement, 
        SPLIT(weburls, " (")[OFFSET(0)] AS _description,
        
        IF(
            REGEXP_CONTAINS(weburls, r'\(' ), 
            CAST(REGEXP_EXTRACT(weburls, r'\((\d+)\)') AS INTEGER),
            1
        ) 
        AS _notes

    FROM
        `sandler_mysql.db_6qa_alert`,
        UNNEST(SPLIT(_weburls, ", ")) AS weburls
    WHERE
        LENGTH(_weburls) > 1
    AND 
        _sdc_deleted_at IS NULL

),

-- Get all web visits from mouseflow
all_mouseflow_web_visit AS (

    SELECT DISTINCT 

        _visitorid AS _source_id,
        _name AS _accountname,
        _domain,
        DATE(_timestamp) AS _timestamp,
        'Webtrack' AS _source,
        'Web Visit' AS _engagement,
        _entryURL AS _description,
        _totalPages AS _notes

    FROM 
        `x-marketing.sandler.dashboard_webtrack_kickfire` 
    WHERE 
        _name IS NOT NULL  
    AND 
        _name != ''

),

-- Add all activities together and supplement mouseflow data with additional fields
combined_engagements AS (

    SELECT
        
        main.* EXCEPT(_source_id),
        side.* EXCEPT(_source_id)

    FROM (

        SELECT * FROM all_email_alert_keyword_research
        UNION ALL
        SELECT * FROM all_email_alert_web_visit
        UNION ALL
        SELECT * FROM all_mouseflow_web_visit

    ) main

    LEFT JOIN (

        SELECT
            
            _visitorid AS _source_id,
            _webActivityURL AS _pages_viewed,
            _utmcampaign AS _utm_campaign,
            _utmmedium AS _utm_medium,
            _utmsource AS _utm_source

        FROM 
            `x-marketing.sandler.dashboard_webtrack_kickfire` 
        WHERE 
            _name IS NOT NULL  
        AND 
            _name != '' 

    ) side

    USING(_source_id)

),

-- Tying everything together
combined_data AS (

    SELECT 

        target.*,
        hubspot.* EXCEPT(account_name, domain),
        activity.* EXCEPT(_accountname, _domain)

    FROM 
        target_accounts AS target

    LEFT JOIN 
        hubspot_info AS hubspot

    ON (
            target._6sensedomain = hubspot.domain
        AND 
            target._6sensedomain != ''
    ) 
    OR  (
            LOWER(target._6sensecompanyname) = LOWER(hubspot.account_name) 
        AND 
            target._6sensedomain = ''
    )

    LEFT JOIN 
        combined_engagements AS activity

    ON (
            target._6sensedomain = activity._domain 
        AND 
            target._6sensedomain != ''
    ) 
    OR  (
            LOWER(target._6sensecompanyname) = LOWER(activity._accountname) 
        AND 
            target._6sensedomain = ''
    )

),

-- Count the total keyword researched and web visits for each account
-- To be use for labelling the account if they have ever performed those engagements
count_total_engagements AS (

    SELECT
        *,

        SUM(IF(_engagement = 'Keyword Research', 1, 0)) OVER (
            PARTITION BY 
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain 
        )
        AS _kr_count,

        SUM(IF(_engagement = 'Web Visit', 1, 0)) OVER (
            PARTITION BY 
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain 
        )
        AS _wv_count

    FROM 
        combined_data

),

-- Label accounts if they have any engagements for keyword researched and web visits
set_boolean_labels AS (

    SELECT
        *,

        CASE 
            WHEN _kr_count > 0 THEN true 
            ELSE false
        END 
        AS _has_intent,

        CASE 
            WHEN _wv_count > 0 THEN true 
            ELSE false
        END 
        AS _has_engagement,

    FROM 
        count_total_engagements

),

-- Recreate formula fields found in the dashboard for the ease of cross filtering 
set_custom_fields AS (

    SELECT
        *,

        CASE 
            WHEN _has_intent = TRUE AND _has_engagement = TRUE THEN 1
            WHEN _has_intent = TRUE AND _has_engagement = FALSE THEN 2
            WHEN _has_intent = FALSE AND _has_engagement = TRUE THEN 3
            WHEN _has_intent = FALSE AND _has_engagement = FALSE THEN 4
        END 
        AS _account_tier,

        CASE 
            WHEN _has_intent = TRUE AND _has_engagement = TRUE THEN _unique_identifier 
        END 
        AS _tier1_account,

        CASE 
            WHEN _has_intent = TRUE AND _has_engagement = FALSE THEN _unique_identifier 
        END 
        AS _tier2_account,

        CASE 
            WHEN _has_intent = FALSE AND _has_engagement = TRUE THEN _unique_identifier 
        END 
        AS _tier3_account,

        CASE 
            WHEN _has_intent = FALSE AND _has_engagement = FALSE THEN _unique_identifier 
        END 
        AS _tier4_account

    FROM 
        set_boolean_labels

)

-- Set empty fields to be filled later through updates
SELECT 
    
    *,
    CAST(NULL AS INT64) AS _active_deal_count,
    CAST(NULL AS INT64) AS _active_deal_amount,
    CAST(NULL AS INT64) AS _2023_won_deal_count,
    CAST(NULL AS INT64) AS _2023_won_deal_amount

FROM 
    set_custom_fields;


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- 6sense Demand Capture Contacts

CREATE OR REPLACE TABLE `sandler.db_6sense_demand_capture_leads` AS

-- Get all target accounts
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._industrylegacy AS _6senseindustry,
        main._6senseemployeerange,
        main._6senserevenuerange,
        side._is_6qa,
        side._has_intent,
        side._has_engagement,
        side._account_tier,
        side._tier1_account,
        side._tier2_account,
        side._tier3_account,
        side._tier4_account

    FROM 
        `sandler_mysql.db_dcd_target_accounts` main
    
    LEFT JOIN (

        SELECT DISTINCT

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            _is_6qa,
            _has_intent,
            _has_engagement,
            _account_tier,
            _tier1_account,
            _tier2_account,
            _tier3_account,
            _tier4_account

        FROM 
            `sandler.db_6sense_demand_capture` 

    ) side 

    ON 
        main._6sensedomain = side._6sensedomain
    OR 
        main._6sensecompanyname = side._6sensecompanyname

),


-- Get all contacts in Hubspot
hubspot_contacts AS (

    SELECT 
        * EXCEPT(rownum) 
    FROM (

        SELECT DISTINCT

            associated_company.company_id AS _hs_companyid,
            associated_company.properties.name.value AS account_name,
            associated_company.properties.domain.value AS domain,
            
            CONCAT(
                property_firstname.value, 
                property_lastname.value
            ) 
            AS _name, 
            
            property_email.value AS _email, 
            properties.jobtitle.value AS _title, 
            property_job_function.value AS _function,

            CASE
                WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
                WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
                ELSE INITCAP(property_lifecyclestage.value)
            END 
            AS _lifecycleStage,
            

            ROW_NUMBER() OVER(
                PARTITION BY property_email.value  
                ORDER BY vid DESC
            ) 
            AS rownum

        FROM 
            `sandler_hubspot.contacts`
        
        WHERE 
            property_email.value IS NOT NULL
            
    )
    WHERE rownum = 1

),

-- Tying everything together
combined_data AS (

    SELECT 

        target.*,
        hubspot.* EXCEPT(account_name, domain)

    FROM 
        target_accounts AS target

    LEFT JOIN 
        hubspot_contacts AS hubspot
    ON 
        target._6sensedomain = hubspot.domain 
    OR  (
        LOWER(target._6sensecompanyname) = LOWER(hubspot.account_name) 
        AND 
        target._6sensedomain != ''
    )

)

SELECT * FROM combined_data;


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- 6sense Demand Capture Opportunities

CREATE OR REPLACE TABLE `sandler.db_6sense_demand_capture_opps` AS

-- Get all target accounts
WITH target_accounts AS (

    SELECT DISTINCT 

        main._6sensecompanyname,
        main._6sensecountry,
        main._6sensedomain,
        main._industrylegacy AS _6senseindustry,
        main._6senseemployeerange,
        main._6senserevenuerange,
        side._is_6qa,
        side._has_intent,
        side._has_engagement,
        side._account_tier,
        side._tier1_account,
        side._tier2_account,
        side._tier3_account,
        side._tier4_account

    FROM 
        `sandler_mysql.db_dcd_target_accounts` main
    
    LEFT JOIN (

        SELECT DISTINCT

            _6sensecompanyname,
            _6sensecountry,
            _6sensedomain,
            _is_6qa,
            _has_intent,
            _has_engagement,
            _account_tier,
            _tier1_account,
            _tier2_account,
            _tier3_account,
            _tier4_account

        FROM 
            `sandler.db_6sense_demand_capture` 

    ) side 

    ON 
        main._6sensedomain = side._6sensedomain
    OR 
        main._6sensecompanyname = side._6sensecompanyname

),

-- Get all opportunities in Hubspot
hubspot_opportunities AS (

    SELECT  

        CAST(companies.companyid AS STRING) AS _hs_companyid, 
        companies.property_name.value AS account_name,
        companies.property_domain.value AS domain,
        CAST(deals.dealid AS STRING) AS _opp_id,
        deals.property_dealname.value AS _opp_name, 
        CONCAT(owners.firstname, ' ', owners.lastname) AS _opp_owner,
        
        CASE 
            WHEN deals.property_dealtype.value = 'newbusiness' 
            THEN 'New Business' 
            WHEN deals.property_dealtype.value = 'existingbusiness' 
            THEN 'Existing Business' 
        END 
        AS _type,
        
        DATE(deals.property_createdate.value) AS _create_date,
        DATE(deals.property_closedate.value) AS _close_date,
        CAST(deals.property_amount.value AS INT) AS _amount, 
        CAST(deals.property_hs_acv.value AS INT) AS _acv,
        DATE(deals.property_dealstage.timestamp) AS _stage_change_date,
        
        DATE_DIFF(
            CURRENT_DATE(), 
            DATE(deals.property_dealstage.timestamp), 
            DAY
        ) 
        AS _days_in_stage,

        INITCAP(stages.label) AS _current_stage,
        stages.probability AS _current_stage_prob

    FROM 
        `sandler_hubspot.deals` deals
    
    LEFT JOIN
        UNNEST(associations.associatedcompanyids) AS deals_company

    LEFT JOIN 
        `sandler_hubspot.companies` companies
    ON 
        deals_company.value = companies.companyid

    LEFT JOIN 
        `sandler_hubspot.owners` owners 
    ON 
        deals.properties.hubspot_owner_id.value = CAST(owners.ownerid AS STRING)

    JOIN (

        SELECT DISTINCT 
            stages.value.* 
        FROM 
            `sandler_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages

    ) stages 

    ON 
        deals.property_dealstage.value = stages.stageid 

    WHERE 
        LOWER(deals.property_dealtype.value) IN ('newbusiness', 'existingbusiness')

    AND (
            LOWER(companies.property_name.value) NOT LIKE '%sandler%'
        OR 
            companies.property_name.value IS NULL
    )

    AND 
        deals.property_pipeline.value IS NOT NULL
    AND 
        deals.isdeleted = false

),

-- Get the previous stage of each opportunity
hubspot_opportunity_history AS (

    SELECT
        main.*,
        side._previous_stage,
        side._previous_stage_prob

    FROM
        hubspot_opportunities AS main

    LEFT JOIN (
        
        SELECT DISTINCT 

            _opportunityID AS _opp_id,
            INITCAP(_stage) AS _current_stage,

            LEAD(INITCAP(_stage)) OVER(
                PARTITION BY _opportunityID 
                ORDER BY _timestamp DESC
            ) 
            AS _previous_stage,
            
            LEAD(_probability) OVER(
                PARTITION BY _opportunityID 
                ORDER BY _timestamp DESC
            ) 
            AS _previous_stage_prob

        FROM
            `sandler.hubspot_opportunity_stage_history`

    ) AS side

    USING(
        _opp_id,
        _current_stage
    )

),

-- Tying everything together
combined_data AS (

    SELECT 

        target.*,
        hubspot.* EXCEPT(account_name, domain)

    FROM 
        target_accounts AS target

    LEFT JOIN 
        hubspot_opportunity_history AS hubspot
    ON 
        target._6sensedomain = hubspot.domain 

    OR  (
            LOWER(target._6sensecompanyname) = LOWER(hubspot.account_name) 
        AND 
            target._6sensedomain != ''
    )

)

SELECT * FROM combined_data;


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- Update opportunity related fields in the Account + Engagement table

UPDATE 
    `sandler.db_6sense_demand_capture` AS main
SET 
    main._active_deal_count = side._active_deal_count,
    main._active_deal_amount = side._active_deal_amount,
    main._2023_won_deal_count = side._2023_won_deal_count,
    main._2023_won_deal_amount = side._2023_won_deal_amount

FROM (

    SELECT
        _6sensecompanyname,
        _6sensedomain,
        
        COUNT(
            CASE
                WHEN 
                    _current_stage NOT LIKE '%Closed%' 
                THEN 
                    _opp_id 
            END
        ) 
        AS _active_deal_count,
        
        SUM(
            CASE 
                WHEN 
                    _current_stage NOT LIKE '%Closed%' 
                THEN 
                    _amount 
            END
        ) 
        AS _active_deal_amount,
        
        COUNT(
            CASE 
                WHEN 
                    _current_stage LIKE '%Won%' 
                AND
                    EXTRACT(YEAR FROM _close_date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                THEN 
                    _opp_id 
            END
        ) 
        AS _2023_won_deal_count,
        
        SUM(
            CASE 
                WHEN 
                    _current_stage LIKE '%Won%' 
                AND
                    EXTRACT(YEAR FROM _close_date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                THEN 
                    _amount 
            END
        ) 
        AS _2023_won_deal_amount

    FROM 
        `sandler.db_6sense_demand_capture_opps`

    GROUP BY 
        1, 2

) side

WHERE 
    main._6sensecompanyname = side._6sensecompanyname 
AND 
    main._6sensedomain = side._6sensedomain;


