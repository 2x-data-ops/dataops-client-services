
-- Historical lead score for salesforce campaigns on a daily basis

TRUNCATE TABLE `x-marketing.corcentric.marketo_lead_daily_salesforce_campaigns`;

INSERT INTO `x-marketing.corcentric.marketo_lead_daily_salesforce_campaigns`

-- CREATE OR REPLACE TABLE `x-marketing.corcentric.marketo_lead_daily_salesforce_campaigns` AS

-- Get all campaign engagements in salesforce
WITH salesforce_campaign_members AS (

    SELECT 
        campaign.name AS campaign, 
        DATE(member.createddate) AS activity_date,
        member.status AS activity_status,
        member.email,
        member.leadorcontactid AS sf_lead_or_contact_id
        
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

-- Tie the salesforce members to their marketo ID
get_marketo_id_for_salesforce_members AS (

    SELECT
        main.*,
        side.id AS marketo_id

    FROM 
        salesforce_campaign_members AS main 
    
    JOIN 
        `x-marketing.corcentric_marketo.leads` AS side 
    
    ON 
        main.sf_lead_or_contact_id = side.sfdcleadid
    OR 
        main.sf_lead_or_contact_id = side.sfdccontactid

),

-- Obtain the historical scores of the salesforce members
get_marketo_scores AS (

    SELECT
        id AS marketo_id,
        extract_date,
        ROUND(behavior_score__c, 1) AS behaviour_score,
        ROUND(demographic_score__c, 1) AS demographic_score,
        ROUND(leadscore, 1) AS current_lead_score,

        LAG (
            ROUND(leadscore, 1)
        )
        OVER (
            PARTITION BY 
                id
            ORDER BY 
                extract_date
        )
        AS previous_lead_score

    FROM 
        `x-marketing.corcentric.marketo_lead_score_snapshot`

),

combined_data AS (

    SELECT
        main.*,
        side.* EXCEPT(marketo_id),
        (side.current_lead_score - COALESCE(side.previous_lead_score, 0)) AS lead_score_diff 

    FROM 
        get_marketo_id_for_salesforce_members AS main 

    JOIN 
        get_marketo_scores AS side 

    USING(marketo_id)

)

SELECT * FROM combined_data

ORDER BY email, extract_date;

