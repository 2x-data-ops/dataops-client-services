CREATE OR REPLACE TABLE `devo.account_lifecyclestage_historical` AS

WITH created_date AS (
SELECT 
  comp.companyid AS companyID,
  comp.properties.name.value AS companyName,
  CONCAT(owner.firstname, ' ', owner.lastname) AS companyOwner,
  DATE(comp.properties.createdate.value) AS createDate,
  comp.properties.phone.value AS phoneNumber,
  DATE(comp.properties.hs_last_sales_activity_date.value) AS lastActivityDate,
  comp.properties.city.value AS city,
  comp.properties.country.value AS country,
  comp.properties.industry.value AS industry,
  comp.properties.account_priority__sales_.value AS accountPriority,
  comp.properties.first_conversion_event_name.value AS firstConversion,
  comp.properties.domain.value AS companyDomainName,
  comp.properties.linkedin_company_page.value AS linkedinCompanyPage,
  comp.properties.annualrevenue.value AS annualRevenue,
  CAST(comp.properties.numberofemployees.value AS INT64) AS numberEmployees,
  comp.properties.account_lifecycle.value AS accountStage,
  comp.properties.lifecyclestage.value AS lifecycleStage,
  DATE(comp.properties.hs_lastmodifieddate.value) AS lastModifiedDate,
  --c.properties.hs_v2_date_entered_lead.value AS leadDate,
  DATE(c.properties.hs_lifecyclestage_lead_date.value) AS leadDate,
  DATE(c.properties.hs_lifecyclestage_subscriber_date.value) AS subscriberDate,
  DATE(c.properties.hs_lifecyclestage_opportunity_date.value) AS opportunityDate,
  DATE(c.properties.hs_lifecyclestage_customer_date.value) AS customerDate,
  --ROW_NUMBER() OVER(PARTITION BY comp.companyid, comp.properties.lifecyclestage.value ORDER BY comp.properties.createdate.value DESC) AS _rownum,
  comp.properties.lifecyclestage.value AS historicalLifecycleStage,
  DATE(comp.properties.createdate.value) AS historicalLifecycleDate,
  --ROW_NUMBER() OVER(PARTITION BY comp.companyid, comp.properties.lifecyclestage.value ORDER BY comp.properties.createdate.value DESC) AS _rownum
FROM 
  `x-marketing.devo_hubspot.companies` comp
LEFT JOIN 
  `x-marketing.devo_hubspot.owners` owner
ON comp.properties.hubspot_owner_id.value = CAST(owner.ownerid AS STRING)
LEFT JOIN
  `x-marketing.devo_hubspot.contacts` c
ON comp.companyid = c.properties.associatedcompanyid.value
--WHERE properties.createdate.value IS NOT NULL
),
lead_date AS (
  SELECT * EXCEPT (historicalLifecycleStage, historicalLifecycleDate),
    'Lead' AS historicalLifecycleStage,
    leadDate AS historicalLifecycleDate,
    --ROW_NUMBER() OVER(PARTITION BY companyID ORDER BY leadDate DESC) AS _rownum
  FROM 
    created_date
  WHERE
    leadDate IS NOT NULL
),
_subcriber_date AS (
  SELECT * EXCEPT (historicalLifecycleStage, historicalLifecycleDate),
    'Subscriber' AS historicalLifecycleStage,
    subscriberDate AS historicalLifecycleDate,
    --ROW_NUMBER() OVER(PARTITION BY companyID ORDER BY subscriberDate DESC) AS _rownum
  FROM 
    created_date
  WHERE
    subscriberDate IS NOT NULL
),
_opportunity_date AS (
  SELECT * EXCEPT (historicalLifecycleStage, historicalLifecycleDate),
    'Opportunity' AS historicalLifecycleStage,
    opportunityDate AS historicalLifecycleDate,
    --ROW_NUMBER() OVER(PARTITION BY companyID ORDER BY opportunityDate DESC) AS _rownum
  FROM 
    created_date
  WHERE
    opportunityDate IS NOT NULL
),
_customer_date AS (
  SELECT * EXCEPT (historicalLifecycleStage, historicalLifecycleDate),
    'Customer' AS historicalLifecycleStage,
    customerDate AS historicalLifecycleDate,
    --ROW_NUMBER() OVER(PARTITION BY companyID ORDER BY customerDate DESC) AS _rownum
  FROM 
    created_date
  WHERE
    customerDate IS NOT NULL
)
SELECT * FROM created_date
UNION ALL
SELECT * FROM lead_date
UNION ALL
SELECT * FROM _subcriber_date
UNION ALL
SELECT * FROM _opportunity_date
UNION ALL
SELECT * FROM _customer_date;