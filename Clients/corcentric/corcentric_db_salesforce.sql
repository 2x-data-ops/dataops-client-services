CREATE OR REPLACE TABLE `x-marketing.corcentric.salesforce_qp_and_mql` AS 
WITH 
qualified_prospects AS (
    SELECT  
        'QP' AS label,
        ds_03_last_mql__c AS qualified_date,
        id AS contact_id,
        name AS contact_name,
        email,
        mailingcountry AS country,
        status__c AS status,
        CASE WHEN converted_lead_id__c IS NOT NULL THEN TRUE ELSE FALSE END AS converted,
        ds_03_last_qp__c AS ds_03_last_qp,
        ds_04_last_discovery__c AS ds_04_last_discovery,
        ds_05_last_mql__c AS ds_05_last_mql,
        ds_06_last_sal__c AS ds_06_last_sal,
        ds_07_last_sql__c AS ds_07_last_sql,
        ds_08_last_opportunity__c AS ds_08_last_opportunity
    FROM `x-marketing.corcentric_salesforce.Contact` 
    WHERE ds_03_last_mql__c >= '2023-01-01'
    -- AND firstname NOT LIKE '%test%'
    -- AND lastname NOT LIKE '%test%'
    AND email NOT LIKE '%@corcentric' AND email <> 'juliamadison@dayrep.com' AND email <> 'info@josesantosconstruction.com'
    AND mailingcountry IN ('Canada', 'United States')
    AND status__c != 'Unqualified'
),
marketing_qualified_leads AS (
    SELECT 
        'MQL' AS label,
        ds_04_last_sal__c AS qualified_date,
        id AS contact_id,
        name AS contact_name,
        email,
        mailingcountry AS country,
        status__c AS status,
        CASE WHEN converted_lead_id__c IS NOT NULL THEN TRUE ELSE FALSE END AS converted,
        ds_03_last_qp__c AS ds_03_last_qp,
        ds_04_last_discovery__c AS ds_04_last_discovery,
        ds_05_last_mql__c AS ds_05_last_mql,
        ds_06_last_sal__c AS ds_06_last_sal,
        ds_07_last_sql__c AS ds_07_last_sql,
        ds_08_last_opportunity__c AS ds_08_last_opportunity
    FROM `x-marketing.corcentric_salesforce.Contact` 
    WHERE ds_04_last_sal__c >= '2023-01-01'
    -- AND firstname NOT LIKE '%test%'
    -- AND lastname NOT LIKE '%test%'
    AND name NOT LIKE '%Test Test%'
    AND email NOT LIKE '%@corcentric%' AND email <> 'juliamadison@dayrep.com'
    AND mailingcountry IN ('Canada', 'United States')
)
SELECT * FROM qualified_prospects
UNION ALL
SELECT * FROM marketing_qualified_leads;
