 CREATE OR REPLACE TABLE `sdi.db_opportunity_log` AS
 WITH companies AS (
    SELECT companyid,
    property_name.value AS _accountName,
    property_domain.value AS _domain,
    FROM `x-marketing.sdi_hubspot.companies`
 ), 
 contacts AS (
    SELECT
  vid AS _id,
  property_email.value AS _email,
  CONCAT( COALESCE(INITCAP(property_firstname.value), ''), ' ', COALESCE(INITCAP(property_lastname.value), '') ) AS _name,
  property_lifecyclestage.value AS _lifecycleStage,
  property_createdate.value AS _createddate,
  property_salesforceaccountid.value AS _sfdcaccountid,
  property_salesforcecontactid.value AS _sfdccontactid,
  property_salesforceleadid.value AS _sfdcleadid,
  associated_company.company_id AS _company_id,`merged_vids`[SAFE_OFFSET(0)].value,
  split_part AS contactid
FROM
  `x-marketing.x3x_hubspot.contacts` hs,
  UNNEST(
    CASE
      WHEN REGEXP_CONTAINS(property_hs_all_contact_vids.value, ";") THEN SPLIT(property_hs_all_contact_vids.value, ";")
      ELSE ARRAY<STRING>[property_hs_all_contact_vids.value]
    END
  ) AS split_part


 ) ,
 
 opps AS (
 SELECT 
        CAST(deals.dealid AS STRING) AS _opportunityID, 
        associations.`associatedcompanyids`[SAFE_OFFSET(0)].value AS _accountID , 
        deals.property_dealname.value AS _opportunityName, 
        stages.label AS _currentStage,
        stages.probability AS _currentStageProbability,
        deals.property_createdate.value AS _createTS,
        deals.property_closedate.value AS _closeTS,
        deals.property_amount.value AS _amount, 
        deals.property_hs_acv.value AS _acv,
        deals.property_dealtype.value AS _type,
        deals.property_closed_lost_reason.value AS _reason,
        deals.property_dealstage.timestamp AS _oppLastChangeinStage,
        associations.`associatedvids`[SAFE_OFFSET(0)].value AS _contactid,
        DATE_DIFF(
            CURRENT_TIMESTAMP(), 
            deals.property_dealstage.timestamp, 
            DAY
        ) AS _daysCurrentStage

    FROM 
        `x-marketing.sdi_hubspot.deals` deals

    JOIN (

        SELECT DISTINCT 
            stages.value.* 
        FROM 
            `x-marketing.sdi_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages

    ) stages 

    ON 
        deals.property_dealstage.value = stages.stageid 
 ) SELECT opps.*,
 companies.* EXCEPT (companyid),
 contacts.* EXCEPT(_id)
 FROM opps
 LEFT JOIN companies ON opps._accountID = companies.companyid
 LEFT JOIN  contacts ON CAST(opps._contactid AS STRING) = contacts.contactid