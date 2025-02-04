
-- Get snapshot of marketo and salesforce related info of leads

INSERT INTO `x-marketing.corcentric.marketo_lead_info_snapshot` (
    extract_date,
    name,
    email,
    title,
    job_function,
    seniority,
    industry,
    company,
    annual_revenue,
    annual_revenue_amount,
    city,
    state,
    country,
    lead_source,
    lead_status,
    lead_score,
    relative_score,
    behaviour_score,
    demographic_score,
    ds03_mql_date,
    ds03_qp_date,
    ds04_discovery_date,
    ds05_mql_date,
    utm_campaign,
    utm_medium,
    utm_source,
    marketoid,
    sfdctype,
    sfdcleadid,
    sfdccontactid,
    sfdcleadorcontactid,
    sfdcaccountid,
    sfdcleadstatus,
    sfdccontactstatus,
    sf_account_website,
    sf_account_type,
    sf_account_source,
    region
)
-- Get marketo related fields for those leads with email
WITH marketo_leads_with_email AS (
  SELECT
            mktoname AS name, 
            email, 
            title,
            zoominfo_job_function__c AS job_function, 
            zoominfo_management_level__c AS seniority, 
            industry,
            company,
            annual_revenue__c_account AS annual_revenue,
            annualrevenue AS annual_revenue_amount,
            city, 
            state, 
            country, 
            leadsource AS lead_source,
            leadstatus AS lead_status, 
            leadscore AS lead_score,  
            relativescore AS relative_score, 
            behavior_score__c AS behaviour_score, 
            demographic_score__c AS demographic_score, 
            ds_03_last_mql__c AS ds03_mql_date,
            ds_03_last_qp__c AS ds03_qp_date,
            ds_04_last_discovery__c AS ds04_discovery_date,
            ds_05_last_mql__c AS ds05_mql_date,
            utm_campaign__c AS utm_campaign,
            utm_medium__c AS utm_medium,
            utm_source__c	 AS utm_source,
            CAST(id AS STRING) AS marketoid,
            sfdctype, 
            sfdcleadid, 
            sfdccontactid,
            
            CASE
                WHEN sfdccontactid IS NOT NULL THEN sfdccontactid
                ELSE sfdcleadid
            END 
            AS sfdcleadorcontactid, 
            
            sfdcaccountid,
            removed_from_marketo__c AS is_deleted,
            ROW_NUMBER() OVER(
                PARTITION BY email 
                ORDER BY updatedat DESC
            ) rownum
        FROM 
            `x-marketing.corcentric_marketo.leads`
        
        WHERE email IS NOT NULL
        AND email NOT LIKE '%2x.marketing%'
        AND email NOT LIKE '%corcentric%'
        AND email NOT LIKE '%@determine%'
        AND company NOT IN('2x', '2X', '2X LLC', '2X LLC Sdn Bhd', '2x Marketing', '2X Marketing')
        AND UPPER(company) NOT LIKE '%CORCENTRIC%'
        AND UPPER(company) NOT LIKE '%DETERMINE%'
        AND LOWER(mktoname) NOT LIKE '%test%test%'
        AND email NOT LIKE 'test%@straightnorth.com'
        AND email NOT LIKE '%@new.com'
        AND email NOT LIKE '%@test.com'
        AND email NOT LIKE '%@company.com'
        AND email NOT LIKE '%@bpack.com'
        AND email NOT LIKE '%@vorker.com'
        AND email NOT LIKE 'test@%'
        AND email NOT LIKE '%@email.tst%'
),


unique_marketo_leads_with_email AS (

    SELECT 
        * EXCEPT(rownum, is_deleted) 
    FROM marketo_leads_with_email
    WHERE 
        rownum = 1
    AND 
        is_deleted IS NULL

),

marketo_leads_without_email AS (
  SELECT
            mktoname AS name, 
            email, 
            title,
            zoominfo_job_function__c AS job_function, 
            zoominfo_management_level__c AS seniority, 
            industry,
            company,
            annual_revenue__c_account AS annual_revenue,
            annualrevenue AS annual_revenue_amount,
            city, 
            state, 
            country, 
            leadsource AS lead_source, 
            leadstatus AS lead_status,
            leadscore AS lead_score,  
            relativescore AS relative_score, 
            behavior_score__c AS behaviour_score, 
            demographic_score__c AS demographic_score, 
            ds_03_last_mql__c AS ds03_mql_date,
            ds_03_last_qp__c AS ds03_qp_date,
            ds_04_last_discovery__c AS ds04_discovery_date,
            ds_05_last_mql__c AS ds05_mql_date,
            utm_campaign__c AS utm_campaign,
            utm_medium__c AS utm_medium,
            utm_source__c	 AS utm_source,
            CAST(id AS STRING) AS marketoid,
            sfdctype, 
            sfdcleadid, 
            sfdccontactid,

            CASE
                WHEN sfdccontactid IS NOT NULL THEN sfdccontactid
                ELSE sfdcleadid
            END 
            AS sfdcleadorcontactid, 

            sfdcaccountid,
            removed_from_marketo__c AS is_deleted,

            ROW_NUMBER() OVER(
                PARTITION BY mktoname 
                ORDER BY updatedat DESC
            ) 
            AS rownum

        FROM 
            `x-marketing.corcentric_marketo.leads`

        WHERE email IS NULL
        AND mktoname IS NOT NULL
        AND company NOT IN('2x', '2X', '2X LLC', '2X LLC Sdn Bhd', '2x Marketing', '2X Marketing')
        AND UPPER(company) NOT LIKE '%CORCENTRIC%' 
        AND UPPER(company) NOT LIKE '%DETERMINE%'
),

-- Get marketo related fields for those leads without email
unique_marketo_leads_without_email AS (

    SELECT 
        * EXCEPT(rownum, is_deleted) 
    FROM marketo_leads_without_email
    WHERE 
        rownum = 1
    AND 
        is_deleted IS NULL

),

-- Get salesforce leads info
sf_leads AS (

    SELECT
        id,
        status AS sfdcleadstatus
    FROM 
        `x-marketing.corcentric_salesforce.Lead`

),

-- Get salesforce contact info
sf_contacts AS (

    SELECT
        id,
        status__c AS sfdccontactstatus
    FROM 
        `x-marketing.corcentric_salesforce.Contact`

),

-- Get salesforce account info
sf_accounts AS (

    SELECT
        id,
        website sf_account_website, 
        type AS sf_account_type, 
        accountsource AS sf_account_source
    FROM 
        `x-marketing.corcentric_salesforce.Account`

),

unique_marketo_leads_combined AS (
  SELECT * FROM unique_marketo_leads_with_email
        UNION ALL
        SELECT * FROM unique_marketo_leads_without_email
),

-- Join marketo and salesforce data together
combined_data AS (

    SELECT 
        CURRENT_TIMESTAMP() AS extract_date,
        marketo_leads.*,
        sf_leads.* EXCEPT(id),
        sf_contacts.* EXCEPT(id),
        sf_accounts.* EXCEPT(id)

    FROM unique_marketo_leads_combined AS marketo_leads
    
    LEFT JOIN sf_leads ON marketo_leads.sfdcleadid = sf_leads.id
    LEFT JOIN sf_contacts ON marketo_leads.sfdccontactid = sf_contacts.id
    LEFT JOIN sf_accounts ON marketo_leads.sfdcaccountid = sf_accounts.id

),

-- Set the region based on country
set_region AS (

    SELECT
        *,
        CASE 
            -- North America region
            WHEN country LIKE '%Canada%' THEN 'North America'
            WHEN country LIKE '%United States%' THEN 'North America'
            --- North Europe region
            WHEN country LIKE '%Austria%' THEN 'North Europe'
            WHEN country LIKE '%Denmark%' THEN 'North Europe'
            WHEN country LIKE '%Estonia%' THEN 'North Europe'
            WHEN country LIKE '%Finland%' THEN 'North Europe'
            WHEN country LIKE '%Iceland%' THEN 'North Europe'
            WHEN country LIKE '%Ireland%' THEN 'North Europe'
            WHEN country LIKE '%Germany%' THEN 'North Europe'
            WHEN country LIKE '%Latvia%' THEN 'North Europe'
            WHEN country LIKE '%Liechtenstein%' THEN 'North Europe'
            WHEN country LIKE '%Lithuania%' THEN 'North Europe'
            WHEN country LIKE '%Netherlands%' THEN 'North Europe'
            WHEN country LIKE '%Norway%' THEN 'North Europe'
            WHEN country LIKE '%Sweden%' THEN 'North Europe'
            WHEN country LIKE '%United Kingdom%' THEN 'North Europe'
            -- South Europe region
            WHEN country LIKE '%Belgium%' THEN 'South Europe'
            WHEN country LIKE '%France%' THEN 'South Europe'
            WHEN country LIKE '%Italy%' THEN 'South Europe'
            WHEN country LIKE '%Luxembourg%' THEN 'South Europe'
            WHEN country LIKE '%Monaco%' THEN 'South Europe'
            WHEN country LIKE '%Portugal%' THEN 'South Europe'
            WHEN country LIKE '%Spain%' THEN 'South Europe'
            WHEN country LIKE '%Switzerland%' THEN 'South Europe'
            -- Empty country
            WHEN country IS NULL THEN NULL
            -- Non Marketable
            ELSE 'Non-Marketable'
        END 
        AS region
    
    FROM 
        combined_data

)

SELECT * FROM set_region
;

