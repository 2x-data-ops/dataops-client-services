
-- CREATE OR REPLACE TABLE
--     `x-marketing.corcentric.marketo_lead_movement`
-- CLUSTER BY
--     week_str,
--     campaign,
--     email
-- AS (

-- );

-- CREATE TABLE
--     `x-marketing.corcentric.marketo_lead_movement`
-- CLUSTER BY
--     week_str,
--     region,
--     category,
--     campaign
-- AS (

-- );
  

TRUNCATE TABLE `x-marketing.corcentric.marketo_lead_movement`;

INSERT INTO `x-marketing.corcentric.marketo_lead_movement`

-- Get all marketo leads and their scores
WITH all_scores AS (

    SELECT 

        -- End Dates
        DATE(DATE_TRUNC(extract_date, WEEK(MONDAY))) AS first_date,
        LAST_DAY(extract_date, WEEK(MONDAY)) AS last_date,

        -- Date Breakdown
        EXTRACT(YEAR FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS year,
        EXTRACT(WEEK FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS week,
        EXTRACT(DAYOFWEEK FROM extract_date) - 1 AS day_of_week,

        -- ID and Scores
        CAST(id AS STRING) AS marketoid,
        ROUND(leadscore, 1) AS lead_score,
        ROUND(behavior_score__c, 1) AS behaviour_score,
        ROUND(demographic_score__c, 1) AS demographic_score

    -- True source of all types of lead score
    FROM 
        `x-marketing.corcentric.marketo_lead_score_snapshot`

),

-- Obtain the rank of week plus week description based on historical snapshot timeline
week_rank_and_week_str AS (

    SELECT

        main.*,
        side.week_rank,
        side.week_str

    FROM 
        all_scores AS main
    
    -- Rank and range already pre-calculated in here
    LEFT JOIN 
        `x-marketing.corcentric.marketo_weekly_dates` side
    ON 
        main.first_date = side.rep_date

),

-- Get the max week scores, these are the representative scores
max_week_scores AS (

    SELECT

        *,

        MAX(lead_score) OVER(
            PARTITION BY 
                week_rank, 
                marketoid
        ) 
        AS max_week_score,

        MAX(behaviour_score) OVER(
            PARTITION BY 
                week_rank, 
                marketoid
        ) 
        AS max_week_behaviour_score,

        MAX(demographic_score) OVER(
            PARTITION BY 
                week_rank, 
                marketoid
        ) 
        AS max_week_demographic_score

    FROM 
        week_rank_and_week_str

),

-- Each score as a column on its own
score_pivot_table AS (
    
    SELECT

        marketoid,

        -- All lead scores
        MAX(CASE WHEN day_of_week = 1 THEN lead_score ELSE NULL END) AS day1_score,
        MAX(CASE WHEN day_of_week = 2 THEN lead_score ELSE NULL END) AS day2_score,
        MAX(CASE WHEN day_of_week = 3 THEN lead_score ELSE NULL END) AS day3_score,
        MAX(CASE WHEN day_of_week = 4 THEN lead_score ELSE NULL END) AS day4_score,
        MAX(CASE WHEN day_of_week = 5 THEN lead_score ELSE NULL END) AS day5_score,
        MAX(CASE WHEN day_of_week = 6 THEN lead_score ELSE NULL END) AS day6_score,
        MAX(CASE WHEN day_of_week = 0 THEN lead_score ELSE NULL END) AS day7_score,

        -- All behaviour scores
        MAX(CASE WHEN day_of_week = 1 THEN behaviour_score ELSE NULL END) AS day1_behaviour_score,
        MAX(CASE WHEN day_of_week = 2 THEN behaviour_score ELSE NULL END) AS day2_behaviour_score,
        MAX(CASE WHEN day_of_week = 3 THEN behaviour_score ELSE NULL END) AS day3_behaviour_score,
        MAX(CASE WHEN day_of_week = 4 THEN behaviour_score ELSE NULL END) AS day4_behaviour_score,
        MAX(CASE WHEN day_of_week = 5 THEN behaviour_score ELSE NULL END) AS day5_behaviour_score,
        MAX(CASE WHEN day_of_week = 6 THEN behaviour_score ELSE NULL END) AS day6_behaviour_score,
        MAX(CASE WHEN day_of_week = 0 THEN behaviour_score ELSE NULL END) AS day7_behaviour_score,
        
        -- All demographic scores
        MAX(CASE WHEN day_of_week = 1 THEN demographic_score ELSE NULL END) AS day1_demographic_score,
        MAX(CASE WHEN day_of_week = 2 THEN demographic_score ELSE NULL END) AS day2_demographic_score,
        MAX(CASE WHEN day_of_week = 3 THEN demographic_score ELSE NULL END) AS day3_demographic_score,
        MAX(CASE WHEN day_of_week = 4 THEN demographic_score ELSE NULL END) AS day4_demographic_score,
        MAX(CASE WHEN day_of_week = 5 THEN demographic_score ELSE NULL END) AS day5_demographic_score,
        MAX(CASE WHEN day_of_week = 6 THEN demographic_score ELSE NULL END) AS day6_demographic_score,
        MAX(CASE WHEN day_of_week = 0 THEN demographic_score ELSE NULL END) AS day7_demographic_score,
        
        -- All date info
        first_date AS week_start_date,
        last_date AS week_end_date,
        week,
        week_str,
        week_rank,
        year,

        -- All max week scores
        COALESCE(max_week_score, 0) AS max_week_score,
        COALESCE(max_week_behaviour_score, 0) AS max_week_behaviour_score,
        COALESCE(max_week_demographic_score, 0) AS max_week_demographic_score

    FROM 
        max_week_scores
    GROUP BY 
        1, 23, 24, 25, 26, 27, 28, 29, 30, 31

),

-- Get marketo info of leads
marketo_info AS (

    SELECT 
        * EXCEPT(rownum) 
    FROM (
        
        SELECT

            marketoid,
            sfdcleadorcontactid,
            email, 
            name,
            title,
            job_function,
            state,
            country,
            COALESCE(region, 'Non-Marketable') AS region,

            -- Set the rank of region for display order in dashboard's filter
            CASE
                WHEN region = 'North America' THEN 1
                WHEN region = 'North Europe' THEN 2
                WHEN region = 'South Europe' THEN 3
                WHEN region = 'Non-Marketable' THEN 4
            END 
            AS region_rank,
            
            company,
            industry,
            annual_revenue_amount AS annual_revenue,
            lead_source,
            lead_status,
            utm_campaign,
            utm_medium,
            utm_source,

            ROW_NUMBER() OVER(
                PARTITION BY 
                    marketoid
                ORDER BY 
                    extract_date DESC
            ) 
            AS rownum

        FROM 
            `x-marketing.corcentric.marketo_lead_info_snapshot`

    )
    WHERE rownum = 1
    
),

-- Get salesforce info of leads
sf_info AS (

    SELECT 
        * EXCEPT(rownum) 
    FROM (

        SELECT

            marketoid,
            sfdcleadorcontactid,
            sfdctype,
            sfdcleadstatus,
            sfdccontactstatus,
            sf_account_website,
            sf_account_type,
            sf_account_source,
            
            ROW_NUMBER() OVER(
                PARTITION BY 
                    marketoid
                ORDER BY 
                    extract_date DESC
            ) 
            AS rownum

        FROM 
            `x-marketing.corcentric.marketo_lead_info_snapshot`

    )
    WHERE 
        rownum = 1

),

-- Combine marketo and salesforce info with the main data
add_marketo_and_sf_info AS (

    SELECT

        base.*,
        extra_mkt.* EXCEPT(marketoid),
        extra_sf.* EXCEPT(marketoid, sfdcleadorcontactid)

    FROM 
        score_pivot_table AS base
    
    -- Join with marketo based on ID 
    JOIN 
        marketo_info AS extra_mkt
    ON 
        base.marketoid = extra_mkt.marketoid

    -- Join with salesforce based on ID 
    LEFT JOIN 
        sf_info AS extra_sf
    ON (
            extra_mkt.marketoid = extra_sf.marketoid
        AND 
            extra_mkt.sfdcleadorcontactid = extra_mkt.sfdcleadorcontactid 
    )

),

-- Get all people in salesforce 
salesforce_leads_and_contacts AS (

    SELECT  
        name,
        email
    FROM 
        `x-marketing.corcentric_salesforce.Lead` 
    WHERE 
        isdeleted = false

    UNION DISTINCT

    SELECT  
        name,
        email
    FROM 
        `x-marketing.corcentric_salesforce.Contact` 
    WHERE 
        isdeleted = false

),

-- Label those from marketo that are also present in salesforce
check_marketo_people_in_salesforce AS (

    SELECT

        main.*,

        CASE 
            WHEN ( 
                    side_name.name IS NOT NULL 
                OR 
                    side_email.email IS NOT NULL 
            )
            THEN true
            ELSE false
        END 
        AS found_in_salesforce 

    FROM 
        add_marketo_and_sf_info AS main 

    -- For checking those with emails
    LEFT JOIN (

        SELECT DISTINCT
            email
        FROM 
            salesforce_leads_and_contacts
        WHERE 
            email IS NOT NULL

    ) AS side_email

    ON 
        main.email = side_email.email

    -- For checking those without emails
    LEFT JOIN (

        SELECT DISTINCT
            name
        FROM 
            salesforce_leads_and_contacts
        WHERE 
            email IS NULL

    ) AS side_name

    ON 
        LOWER(main.name) = LOWER(side_name.name) 
    AND 
        main.email IS NULL

),

-- Get all campaign engagements in salesforce for 2X campaigns
salesforce_2x_campaign_members AS (

    SELECT 

        DATE(member.createddate) AS activity_date,
        EXTRACT(YEAR FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_year,
        EXTRACT(WEEK FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_week,
        member.email,
        member.status AS activity_status,
        member.leadorcontactid AS sf_campaign_member_id,

        CASE 
            -- Campaigns with fixed names
            WHEN 
                campaign.name IN (
                    -- Old 2X's campaigns
                    '2022-04-NUR-2X-MPT-Campaign',
                    '2022-10-NUR-2X-MPT-2-Campaign',

                    -- New 2X's campaigns
                    '2023-08-EM-LWB-ARD-State-of-ePayables-OoCFO',
                    '2023-08-LWB-ARD-State-of-ePayables-OoCFO',
                    '2023-11-LWB-Credit-Professionals-Overcoming-the-Challenges-of-eInvoicing',
                    '2024-06-EM-AP-Payments-Stop-Fraud',
                    '2024-08-EM-Managed-AP-Managed-Payments'
                ) 
            THEN campaign.name
            
            -- Campaigns with varying names due to waves 
            WHEN campaign.name LIKE '%2023-01-EM-ARD-2X-__-CFO%'
            THEN '2023-01-EM-ARD-2X-CFO'

            WHEN campaign.name LIKE '%2023-01-EM-ARD-2X-%-Office-of-the-CFO%'
            THEN '2023-01-EM-ARD-2X-Office-of-the-CFO'

            WHEN campaign.name LIKE '%2023-01-EM-ARD-2X-%-AP-GF%'
            THEN '2023-01-EM-ARD-2X-AP-GF'

            WHEN campaign.name LIKE '%2023-01-EM-ARD-2X-%-AP-Proc%'
            THEN '2023-01-EM-ARD-2X-AP-Proc'

            WHEN campaign.name LIKE '%2023-03-EM-Managed-AR-%'
            THEN '2023-03-EM-Managed-AR'

            WHEN campaign.name LIKE '%2023-06-EM-2X-%-Managed-Services-Office-of-the-CFO%'
            THEN '2023-06-EM-2X-Managed-Services-Office-of-the-CFO'

            WHEN campaign.name LIKE '%2023-06-NE-EM-2X-%-Managed-Services-Office-of-the-CFO%'
            THEN '2023-06-NE-EM-2X-Managed-Services-Office-of-the-CFO'

            WHEN campaign.name LIKE '%2023-09-EM-2X-%-MAR-Office-of-the-CFO%'
            THEN '2023-09-EM-2X-MAR-Office-of-the-CFO'

            WHEN campaign.name LIKE '%2023-10-NE-EM-2X-%-Managed-AR-25-200M%'
            THEN '2023-10-NE-EM-2X-Managed-AR-25-200M'

            WHEN campaign.name LIKE '%2023-10-NE-EM-2X-%-Managed-AR-200-500M%'
            THEN '2023-10-NE-EM-2X-Managed-AR-200-500M'

            WHEN campaign.name LIKE '%2023-10-EM-Managed-AR-25-75M%'
            THEN '2023-10-EM-Managed-AR-25-75M'

            WHEN campaign.name LIKE '%2023-10-EM-Managed-AR-75-200M%'
            THEN '2023-10-EM-Managed-AR-75-200M'

            WHEN campaign.name LIKE '%2023-11-EM-2X-%-AP+Payments-Office-of-the-CFO%'
            THEN '2023-11-EM-2X-AP+Payments-Office-of-the-CFO'

            WHEN campaign.name LIKE '%2024-02-EM-2X-%-MAR-Reg-ICP-Lower-Seniority%'
            THEN '2024-02-EM-2X-MAR-Reg-ICP-Lower-Seniority'

            WHEN campaign.name LIKE '%2024-02-NE-EM-2X-%-Managed-AR-Phase-2%'
            THEN '2024-02-NE-EM-2X-Managed-AR-Phase-2'

            WHEN campaign.name LIKE '%2024-02-EM-2X-%-MAR-Office-of-the-CFO-Phase-2%'
            THEN '2024-02-EM-2X-MAR-Office-of-the-CFO-Phase-2'

            WHEN campaign.name LIKE '%2024-01-EM-S2P-NA-250M-5B%'
            THEN '2024-01-EM-S2P-NA-250M-5B'

            WHEN campaign.name LIKE '%2024-02-NE-EM-2X-%-S2P-NE-150M-5B%'
            THEN '2024-02-NE-EM-2X-S2P-NE-150M-5B'

            WHEN campaign.name LIKE '%2024-02-EM-AP+Payments-Office-of-the-CFO-Phase-2%'
            THEN '2024-02-EM-AP+Payments-Office-of-the-CFO-Phase-2'

            WHEN campaign.name LIKE '%2024-05-NE-EM-2X-%-Managed-AR-5M-and-above%'
            THEN '2024-05-NE-EM-2X-Managed-AR-5M-and-above'

            WHEN campaign.name LIKE '%2024-05-EM-2X-%-S2P-NA-250M-5B-Phase-2%'
            THEN '2024-05-EM-2X-S2P-NA-250M-5B-Phase-2'

            WHEN campaign.name LIKE '%2024-07-EM-ARD-2X-%-Procurement-Metrics-that-Matter-in-2024%'
            THEN '2024-07-EM-ARD-2X-Procurement-Metrics-that-Matter-in-2024'

            WHEN campaign.name LIKE '%2024-07-LWB-NE-S2-P2-Automation-to-Humanization%'
            THEN '2024-07-LWB-NE-S2-P2-Automation-to-Humanization'

            WHEN campaign.name LIKE '%2024-07-TS-NE-S2P-P2-Finance-Operations-Leadership%'
            THEN '2024-07-TS-NE-S2P-P2-Finance-Operations-Leadership'

            WHEN campaign.name LIKE '%2024-07-NE-EM-2X-%-S2P-P2-NE-150M-5B%'
            THEN '2024-07-NE-EM-2X-S2P-P2-NE-150M-5B'

            WHEN campaign.name LIKE '%2024-09-NA-NE-EM-2X-%-MAR-Negative-Trending-DSO%'
            THEN '2024-09-NA-NE-EM-2X-MAR-Negative-Trending-DSO'

            WHEN campaign.name LIKE '%2024-09-NE-EM-2X-%-MAR-LS-25M-500M%'
            THEN '2024-09-NE-EM-2X-MAR-LS-25M-500M'

        END 
        AS campaign,

        campaign.name AS campaign_original_name,
        campaign.type AS campaign_type,
        '2X Campaigns' AS campaign_category,
        DATE(campaign.createddate) AS campaign_start_date,

        -- There are some recent campaigns with no end date
        -- Use the current date as a filler 
        COALESCE(
            DATE(campaign.enddate),
            CURRENT_DATE()
        ) 
        AS campaign_end_date

    FROM 
        `x-marketing.corcentric_salesforce.CampaignMember` member
    JOIN 
        `x-marketing.corcentric_salesforce.Campaign` campaign
    ON 
        member.campaignid = campaign.id

    WHERE (
    
        campaign.name IN (
            -- Old 2X's campaigns
            '2022-04-NUR-2X-MPT-Campaign',
            '2022-10-NUR-2X-MPT-2-Campaign',

            -- New 2X's campaigns
            '2023-08-EM-LWB-ARD-State-of-ePayables-OoCFO',
            '2023-08-LWB-ARD-State-of-ePayables-OoCFO',
            '2023-11-LWB-Credit-Professionals-Overcoming-the-Challenges-of-eInvoicing',
            '2024-06-EM-AP-Payments-Stop-Fraud',
            '2024-08-EM-Managed-AP-Managed-Payments'
        ) 

        -- Old 2X's campaigns with many waves
        OR campaign.name LIKE '%2023-01-EM-ARD-2X-__-CFO%'
        OR campaign.name LIKE '%2023-01-EM-ARD-2X-%-Office-of-the-CFO%'
        OR campaign.name LIKE '%2023-01-EM-ARD-2X-%-AP-GF%'
        OR campaign.name LIKE '%2023-01-EM-ARD-2X-%-AP-Proc%'
        OR campaign.name LIKE '%2023-03-EM-Managed-AR-%'
        OR campaign.name LIKE '%2023-06-EM-2X-%-Managed-Services-Office-of-the-CFO%'

        -- New 2X's campaigns with many waves
        OR campaign.name LIKE '%2023-06-NE-EM-2X-%-Managed-Services-Office-of-the-CFO%'
        OR campaign.name LIKE '%2023-09-EM-2X-%-MAR-Office-of-the-CFO%'
        OR campaign.name LIKE '%2023-10-EM-Managed-AR-%'
        OR campaign.name LIKE '%2023-10-NE-EM-2X-%-Managed-AR-25-200M%'
        OR campaign.name LIKE '%2023-10-NE-EM-2X-%-Managed-AR-200-500M%' 
        OR campaign.name LIKE '%2023-10-EM-Managed-AR-25-75M%'
        OR campaign.name LIKE '%2023-10-EM-Managed-AR-75-200M%'
        OR campaign.name LIKE '%2023-11-EM-2X-%-AP+Payments-Office-of-the-CFO%'
        OR campaign.name LIKE '%2024-02-EM-2X-%-MAR-Reg-ICP-Lower-Seniority%'
        OR campaign.name LIKE '%2024-02-NE-EM-2X-%-Managed-AR-Phase-2%'
        OR campaign.name LIKE '%2024-02-EM-2X-%-MAR-Office-of-the-CFO-Phase-2%'
        OR campaign.name LIKE '%2024-01-EM-S2P-NA-250M-5B%'
        OR campaign.name LIKE '%2024-02-NE-EM-2X-%-S2P-NE-150M-5B%'
        OR campaign.name LIKE '%2024-02-EM-AP+Payments-Office-of-the-CFO-Phase-2%'
        OR campaign.name LIKE '%2024-05-NE-EM-2X-%-Managed-AR-5M-and-above%'
        OR campaign.name LIKE '%2024-05-EM-2X-%-S2P-NA-250M-5B-Phase-2%'
        OR campaign.name LIKE '%2024-07-EM-ARD-2X-%-Procurement-Metrics-that-Matter-in-2024%'
        OR campaign.name LIKE '%2024-07-LWB-NE-S2-P2-Automation-to-Humanization%'
        OR campaign.name LIKE '%2024-07-TS-NE-S2P-P2-Finance-Operations-Leadership%'
        OR campaign.name LIKE '%2024-07-NE-EM-2X-%-S2P-P2-NE-150M-5B%'
        OR campaign.name LIKE '%2024-09-NA-NE-EM-2X-%-MAR-Negative-Trending-DSO%'
        OR campaign.name LIKE '%2024-09-NE-EM-2X-%-MAR-LS-25M-500M%'
    )
    AND 
        member.isdeleted = false
    
),

-- Get all campaign engagements in salesforce for 2X campaigns
salesforce_non_2x_campaign_members AS (

    SELECT 

        DATE(member.createddate) AS activity_date,
        EXTRACT(YEAR FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_year,
        EXTRACT(WEEK FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_week,
        member.email,
        member.status AS activity_status,
        member.leadorcontactid AS sf_campaign_member_id,
        campaign.name AS campaign,
        campaign.name AS campaign_original_name,
        campaign.type AS campaign_type,
        'Non 2X Campaigns' AS campaign_category,
        DATE(campaign.createddate) AS campaign_start_date,
        DATE(campaign.enddate) AS campaign_end_date

    FROM 
        `x-marketing.corcentric_salesforce.CampaignMember` member
    JOIN 
        `x-marketing.corcentric_salesforce.Campaign` campaign
    ON 
        member.campaignid = campaign.id

    WHERE 
        campaign.name IN (
            '2022-01-NUR-Lifecycle.2023-05-EM-corcentric-managed-services',
            '2022-01-NUR-Lifecycle.2023-05-EM-Corcentric-named-Leader-in-ISG-Provider-Report',
            '2022-01-NUR-Lifecycle.2023-05-EM-How CFOs-Are-Driving-Business-Through-Digital-I',
            '2022-01-NUR-Lifecycle.2023-05-EM-why-cfos-are-going-all-in-on-digital',
            '2022-01-NUR-Lifecycle.2023-06-EM-A-View-From-Above:-the-Role-of-the-Modern-CFO',
            '2022-01-NUR-Lifecycle.2023-06-EM-Digital-Payments:-A-Changing-Economy-Sparks-New',
            '2022-01-NUR-Lifecycle.2023-06-EM-Digital-Savvy-CFOs-Innovate-to-Drive-Improved-W',
            '2022-01-NUR-Lifecycle.2023-06-EM-Shortening-the-cash-conversion-cycle-through-mg',
            '2022-01-NUR-Lifecycle.2023-07-EM-Accounts-Payable-2023:-BIG-Trends',
            '2022-01-NUR-Lifecycle.2023-07-EM-Corcentric-Conversations:-Getting-Opportunistic',
            '2022-01-NUR-Lifecycle.2023-07-EM-Corcentric-Managed-AP-Datasheet',
            '2022-01-NUR-Lifecycle.2023-07-EM-Digital-Payments:-A-Changing-Economy-Sparks-New',
            '2022-01-NUR-Lifecycle.2023-07-EM-IOFM: How-AP-Can-Help-Business-Navigate',
            '2022-01-NUR-Lifecycle.2023-07-EM-Smart-moves-where-AP-and-epayables-stand',
            '2022-01-NUR-Lifecycle.2023-08-EM-9-Ways-Managed-AR-Services-Help-Businesses',
            "2022-01-NUR-Lifecycle.2023-08-EM-CFO's-Guide-to-Automating-AR",
            '2022-01-NUR-Lifecycle.2023-08-EM-Daimler-Trucks-NA-Drives-Growth-with-Mgd-AR',
            '2022-01-NUR-Lifecycle.2023-08-EM-How-Automations-Reduce-Receivables-Delays',
            '2022-01-NUR-Lifecycle.2023-08-EM-How-to-Achieve-a-Permanent-DSO-of-15-days',
            '2022-01-NUR-Lifecycle.2023-08-EM-The-Business-Case-for-Accounts-Receivable-as-a-',
            '2022-01-NUR-Lifecycle.2023-08-EM-The-Evolution-of-AR-and-Credit-Mgt'
        ) 
    AND 
        member.isdeleted = false

),

-- Rank the campaigns based on campaign launched date 
-- Get the campaign week and year for joining later
set_campaign_rank_and_date AS (

    SELECT
        *,

        EXTRACT(YEAR FROM DATE(DATE_TRUNC(campaign_end_date, WEEK(MONDAY)))) AS campaign_year,
        EXTRACT(WEEK FROM DATE(DATE_TRUNC(campaign_end_date, WEEK(MONDAY)))) AS campaign_week

    FROM (

        SELECT
            *,

            -- Set campaign rank
            DENSE_RANK() OVER(
                ORDER BY campaign_start_date  
            )
            AS campaign_rank

        -- Combine campaign members from 2X campaigns and non 2X campaigns 
        FROM (

            SELECT * FROM salesforce_2x_campaign_members

            UNION ALL 

            SELECT * FROM salesforce_non_2x_campaign_members

        )
    
    )

),

-- Tie marketo leads to campaigns
add_campaign AS (

    SELECT

        main.*,
        side.campaign,
        side.campaign_original_name,
        side.campaign_rank,
        side.campaign_start_date,
        side.campaign_end_date,
        side.campaign_category,
        side.campaign_type,
        side.activity_date,
        side.activity_status,
        side.sf_campaign_member_id

    FROM 
        check_marketo_people_in_salesforce AS main

    LEFT JOIN 
        set_campaign_rank_and_date AS side
        
    ON 
        main.email = side.email 
    AND 
        main.sfdcleadorcontactid = side.sf_campaign_member_id
    AND 
        main.year >= side.activity_year
    AND 
        main.week >= side.activity_week
    AND 
        main.year <= side.campaign_year
    AND 
        main.week <= side.campaign_week

),

-- Obtain the latest week scores out of the 7 days of the week
latest_week_scores AS (

    SELECT
        *,

        -- Latest lead score of the week
        CASE
            WHEN day7_score IS NOT NULL THEN day7_score
            WHEN day6_score IS NOT NULL THEN day6_score
            WHEN day5_score IS NOT NULL THEN day5_score
            WHEN day4_score IS NOT NULL THEN day4_score
            WHEN day3_score IS NOT NULL THEN day3_score
            WHEN day2_score IS NOT NULL THEN day2_score
            WHEN day1_score IS NOT NULL THEN day1_score
            ELSE 0
        END 
        AS latest_week_score,

        -- Latest behaviour score of the week
        CASE
            WHEN day7_behaviour_score IS NOT NULL THEN day7_behaviour_score
            WHEN day6_behaviour_score IS NOT NULL THEN day6_behaviour_score
            WHEN day5_behaviour_score IS NOT NULL THEN day5_behaviour_score
            WHEN day4_behaviour_score IS NOT NULL THEN day4_behaviour_score
            WHEN day3_behaviour_score IS NOT NULL THEN day3_behaviour_score
            WHEN day2_behaviour_score IS NOT NULL THEN day2_behaviour_score
            WHEN day1_behaviour_score IS NOT NULL THEN day1_behaviour_score
            ELSE 0
        END 
        AS latest_week_behaviour_score,
        
        -- Latest demographic score of the week
        CASE
            WHEN day7_demographic_score IS NOT NULL THEN day7_demographic_score
            WHEN day6_demographic_score IS NOT NULL THEN day6_demographic_score
            WHEN day5_demographic_score IS NOT NULL THEN day5_demographic_score
            WHEN day4_demographic_score IS NOT NULL THEN day4_demographic_score
            WHEN day3_demographic_score IS NOT NULL THEN day3_demographic_score
            WHEN day2_demographic_score IS NOT NULL THEN day2_demographic_score
            WHEN day1_demographic_score IS NOT NULL THEN day1_demographic_score
            ELSE 0
        END 
        AS latest_week_demographic_score

    FROM 
        add_campaign

),

-- Obtain the previous week scores
previous_week_scores AS (

    SELECT
        *,

        COALESCE(
            LAG(latest_week_score) OVER(
                PARTITION BY marketoid
                ORDER BY week_rank
            ),
            0
        ) 
        AS previous_week_score,

        COALESCE(
            LAG(latest_week_behaviour_score) OVER(
                PARTITION BY marketoid
                ORDER BY week_rank
            ),
            0
        ) 
        AS previous_week_behaviour_score,

        COALESCE(
            LAG(latest_week_demographic_score) OVER(
                PARTITION BY marketoid
                ORDER BY week_rank
            ),
            0
        ) 
        AS previous_week_demographic_score

    FROM 
        latest_week_scores

),

-- Find change in score between current day and the day before
daily_score_changes AS (

    SELECT
        *,

        -- All differences in lead score
        CASE
            WHEN day1_score IS NULL THEN 0 - previous_week_score
            ELSE day1_score - previous_week_score
        END 
        AS D1_D7_change,

        CASE
            WHEN day1_score IS NULL THEN day2_score - 0
            WHEN day2_score IS NULL THEN 0 - day1_score
            ELSE day2_score - day1_score
        END 
        AS D2_D1_change,

        CASE
            WHEN day2_score IS NULL THEN day3_score - 0
            WHEN day3_score IS NULL THEN 0 - day2_score
            ELSE day3_score - day2_score
        END 
        AS D3_D2_change, 

        CASE
            WHEN day3_score IS NULL THEN day4_score - 0
            WHEN day4_score IS NULL THEN 0 - day3_score
            ELSE day4_score - day3_score
        END 
        AS D4_D3_change,

        CASE
            WHEN day4_score IS NULL THEN day5_score - 0
            WHEN day5_score IS NULL THEN 0 - day4_score
            ELSE day5_score - day4_score
        END 
        AS D5_D4_change,

        CASE
            WHEN day5_score IS NULL THEN day6_score - 0
            WHEN day6_score IS NULL THEN 0 - day5_score
            ELSE day6_score - day5_score
        END 
        AS D6_D5_change,

        CASE
            WHEN day6_score IS NULL THEN day7_score - 0
            WHEN day7_score IS NULL THEN 0 - day6_score
            ELSE day7_score - day6_score
        END 
        AS D7_D6_change,

        -- All differences in behaviour scores
        CASE
            WHEN day1_behaviour_score IS NULL THEN 0 - previous_week_behaviour_score
            ELSE day1_behaviour_score - previous_week_behaviour_score
        END 
        AS D1_D7_behaviour_change,

        CASE
            WHEN day1_behaviour_score IS NULL THEN day2_behaviour_score - 0
            WHEN day2_behaviour_score IS NULL THEN 0 - day1_behaviour_score
            ELSE day2_behaviour_score - day1_behaviour_score
        END 
        AS D2_D1_behaviour_change,

        CASE
            WHEN day2_behaviour_score IS NULL THEN day3_behaviour_score - 0
            WHEN day3_behaviour_score IS NULL THEN 0 - day2_behaviour_score
            ELSE day3_behaviour_score - day2_behaviour_score
        END 
        AS D3_D2_behaviour_change, 

        CASE
            WHEN day3_behaviour_score IS NULL THEN day4_behaviour_score - 0
            WHEN day4_behaviour_score IS NULL THEN 0 - day3_behaviour_score
            ELSE day4_behaviour_score - day3_behaviour_score
        END 
        AS D4_D3_behaviour_change,

        CASE
            WHEN day4_behaviour_score IS NULL THEN day5_behaviour_score - 0
            WHEN day5_behaviour_score IS NULL THEN 0 - day4_behaviour_score
            ELSE day5_behaviour_score - day4_behaviour_score
        END 
        AS D5_D4_behaviour_change,

        CASE
            WHEN day5_behaviour_score IS NULL THEN day6_behaviour_score - 0
            WHEN day6_behaviour_score IS NULL THEN 0 - day5_behaviour_score
            ELSE day6_behaviour_score - day5_behaviour_score
        END 
        AS D6_D5_behaviour_change,

        CASE
            WHEN day6_behaviour_score IS NULL THEN day7_behaviour_score - 0
            WHEN day7_behaviour_score IS NULL THEN 0 - day6_behaviour_score
            ELSE day7_behaviour_score - day6_behaviour_score
        END 
        AS D7_D6_behaviour_change,

        -- All differences in demographic scores
        CASE
            WHEN day1_demographic_score IS NULL THEN 0 - previous_week_demographic_score
            ELSE day1_demographic_score - previous_week_demographic_score
        END 
        AS D1_D7_demographic_change,

        CASE
            WHEN day1_demographic_score IS NULL THEN day2_demographic_score - 0
            WHEN day2_demographic_score IS NULL THEN 0 - day1_demographic_score
            ELSE day2_demographic_score - day1_demographic_score
        END 
        AS D2_D1_demographic_change,

        CASE
            WHEN day2_demographic_score IS NULL THEN day3_demographic_score - 0
            WHEN day3_demographic_score IS NULL THEN 0 - day2_demographic_score
            ELSE day3_demographic_score - day2_demographic_score
        END 
        AS D3_D2_demographic_change, 

        CASE
            WHEN day3_demographic_score IS NULL THEN day4_demographic_score - 0
            WHEN day4_demographic_score IS NULL THEN 0 - day3_demographic_score
            ELSE day4_demographic_score - day3_demographic_score
        END 
        AS D4_D3_demographic_change,

        CASE
            WHEN day4_demographic_score IS NULL THEN day5_demographic_score - 0
            WHEN day5_demographic_score IS NULL THEN 0 - day4_demographic_score
            ELSE day5_demographic_score - day4_demographic_score
        END 
        AS D5_D4_demographic_change,

        CASE
            WHEN day5_demographic_score IS NULL THEN day6_demographic_score - 0
            WHEN day6_demographic_score IS NULL THEN 0 - day5_demographic_score
            ELSE day6_demographic_score - day5_demographic_score
        END 
        AS D6_D5_demographic_change,

        CASE
            WHEN day6_demographic_score IS NULL THEN day7_demographic_score - 0
            WHEN day7_demographic_score IS NULL THEN 0 - day6_demographic_score
            ELSE day7_demographic_score - day6_demographic_score
        END 
        AS D7_D6_demographic_change

    FROM 
        previous_week_scores

),

-- Obtain all positive score change only
positive_daily_score_changes AS (

    SELECT
        *,
        -- All positive change in lead score
        CASE 
            WHEN D1_D7_change > 0 THEN D1_D7_change
            ELSE NULL
        END 
        AS positive_D1_D7_change,

        CASE 
            WHEN D2_D1_change > 0 THEN D2_D1_change
            ELSE NULL
        END 
        AS positive_D2_D1_change,

        CASE 
            WHEN D3_D2_change > 0 THEN D3_D2_change
            ELSE NULL
        END 
        AS positive_D3_D2_change,

        CASE 
            WHEN D4_D3_change > 0 THEN D4_D3_change
            ELSE NULL
        END 
        AS positive_D4_D3_change,

        CASE 
            WHEN D5_D4_change > 0 THEN D5_D4_change
            ELSE NULL
        END 
        AS positive_D5_D4_change,

        CASE 
            WHEN D6_D5_change > 0 THEN D6_D5_change
            ELSE NULL
        END 
        AS positive_D6_D5_change,

        CASE 
            WHEN D7_D6_change > 0 THEN D7_D6_change
            ELSE NULL
        END 
        AS positive_D7_D6_change,

        -- All positive change behaviour scores
        CASE 
            WHEN D1_D7_behaviour_change > 0 THEN D1_D7_behaviour_change
            ELSE NULL
        END 
        AS positive_D1_D7_behaviour_change,

        CASE 
            WHEN D2_D1_behaviour_change > 0 THEN D2_D1_behaviour_change
            ELSE NULL
        END 
        AS positive_D2_D1_behaviour_change,
        
        CASE 
            WHEN D3_D2_behaviour_change > 0 THEN D3_D2_behaviour_change
            ELSE NULL
        END 
        AS positive_D3_D2_behaviour_change,
        
        CASE 
            WHEN D4_D3_behaviour_change > 0 THEN D4_D3_behaviour_change
            ELSE NULL
        END 
        AS positive_D4_D3_behaviour_change,
        
        CASE 
            WHEN D5_D4_behaviour_change > 0 THEN D5_D4_behaviour_change
            ELSE NULL
        END 
        AS positive_D5_D4_behaviour_change,
        
        CASE 
            WHEN D6_D5_behaviour_change > 0 THEN D6_D5_behaviour_change
            ELSE NULL
        END 
        AS positive_D6_D5_behaviour_change,
        
        CASE 
            WHEN D7_D6_behaviour_change > 0 THEN D7_D6_behaviour_change
            ELSE NULL
        END 
        AS positive_D7_D6_behaviour_change,
        
        -- All positive change demographic scores
        CASE 
            WHEN D1_D7_demographic_change > 0 THEN D1_D7_demographic_change
            ELSE NULL
        END 
        AS positive_D1_D7_demographic_change,
        
        CASE 
            WHEN D2_D1_demographic_change > 0 THEN D2_D1_demographic_change
            ELSE NULL
        END 
        AS positive_D2_D1_demographic_change,
        
        CASE 
            WHEN D3_D2_demographic_change > 0 THEN D3_D2_demographic_change
            ELSE NULL
        END 
        AS positive_D3_D2_demographic_change,
        
        CASE 
            WHEN D4_D3_demographic_change > 0 THEN D4_D3_demographic_change
            ELSE NULL
        END 
        AS positive_D4_D3_demographic_change,
        
        CASE 
            WHEN D5_D4_demographic_change > 0 THEN D5_D4_demographic_change
            ELSE NULL
        END 
        AS positive_D5_D4_demographic_change,
        
        CASE 
            WHEN D6_D5_demographic_change > 0 THEN D6_D5_demographic_change
            ELSE NULL
        END 
        AS positive_D6_D5_demographic_change,
        
        CASE 
            WHEN D7_D6_demographic_change > 0 THEN D7_D6_demographic_change
            ELSE NULL
        END 
        AS positive_D7_D6_demographic_change
    
    FROM 
        daily_score_changes

),

-- Sum all positive score changes to get weekly positive score changes
positive_weekly_score_changes AS (

    SELECT
        * EXCEPT(
            -- Remove positive score change fields
            positive_D1_D7_change, 
            positive_D2_D1_change, 
            positive_D3_D2_change, 
            positive_D4_D3_change,
            positive_D5_D4_change, 
            positive_D6_D5_change, 
            positive_D7_D6_change,
            positive_D1_D7_behaviour_change, 
            positive_D2_D1_behaviour_change, 
            positive_D3_D2_behaviour_change, 
            positive_D4_D3_behaviour_change,
            positive_D5_D4_behaviour_change, 
            positive_D6_D5_behaviour_change, 
            positive_D7_D6_behaviour_change,
            positive_D1_D7_demographic_change, 
            positive_D2_D1_demographic_change, 
            positive_D3_D2_demographic_change, 
            positive_D4_D3_demographic_change,
            positive_D5_D4_demographic_change, 
            positive_D6_D5_demographic_change, 
            positive_D7_D6_demographic_change
        ), 
        
        -- All positive weekly score change in lead scores
        (
            COALESCE(positive_D1_D7_change, 0) + 
            COALESCE(positive_D2_D1_change, 0) + 
            COALESCE(positive_D3_D2_change, 0) + 
            COALESCE(positive_D4_D3_change, 0) +
            COALESCE(positive_D5_D4_change, 0) + 
            COALESCE(positive_D6_D5_change, 0) + 
            COALESCE(positive_D7_D6_change, 0)
        ) 
        AS positive_week_change,
        
        -- All positive weekly score change in behaviour scores
        (
            COALESCE(positive_D1_D7_behaviour_change, 0) + 
            COALESCE(positive_D2_D1_behaviour_change, 0) + 
            COALESCE(positive_D3_D2_behaviour_change, 0) + 
            COALESCE(positive_D4_D3_behaviour_change, 0) +
            COALESCE(positive_D5_D4_behaviour_change, 0) + 
            COALESCE(positive_D6_D5_behaviour_change, 0) + 
            COALESCE(positive_D7_D6_behaviour_change, 0)
        ) 
        AS positive_week_behaviour_change,
    
        -- All positive weekly score change in demographic scores
        (
            COALESCE(positive_D1_D7_demographic_change, 0) + 
            COALESCE(positive_D2_D1_demographic_change, 0) + 
            COALESCE(positive_D3_D2_demographic_change, 0) + 
            COALESCE(positive_D4_D3_demographic_change, 0) +
            COALESCE(positive_D5_D4_demographic_change, 0) + 
            COALESCE(positive_D6_D5_demographic_change, 0) + 
            COALESCE(positive_D7_D6_demographic_change, 0)
        )
        AS positive_week_demographic_change
    
    FROM 
        positive_daily_score_changes

)

SELECT * FROM positive_weekly_score_changes;

