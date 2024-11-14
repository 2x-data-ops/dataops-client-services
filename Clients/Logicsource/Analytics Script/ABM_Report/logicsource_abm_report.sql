CREATE OR REPLACE TABLE `x-marketing.logicsource.db_abm_account` AS 
WITH account AS (
  SELECT * EXCEPT (_rownum)
  FROM (
  SELECT 
      cast(acc.associated_company.company_id AS STRING) AS account_id,
      CAST(vid AS STRING) AS contact_id,
      acc.associated_company.properties.name.value AS  _companyname,
      acc.associated_company.properties.description.value AS  _aboutcompany,
      /*CASE WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet' ELSE associated_company.properties.industry.value
      END*/ associated_company.properties.all_industry.value AS _industry,
      acc.associated_company.properties.annualrevenue.value AS _revenue,
      acc.associated_company.properties.numberofemployees.value AS _numberofemployees,
      acc.associated_company.properties.website.value AS _website,
      acc.associated_company.properties.address.value AS _address,
      -- CONCAT(acc.associated_company.properties.address.value,' ',acc.associated_company.properties.address2.value)
      acc.associated_company.properties.state.value AS _companyState,
      acc.associated_company.properties.zip.value AS _companyZip,
      acc.associated_company.properties.city.value AS _companyCity,
      acc.associated_company.properties.country.value AS _companyCountry,
    CAST( _interestingtopicsdate AS TIMESTAMP) AS _interestingtopicsdate, 
  _interestingtopicssourceurl, 
  _interestingtopicid, 
  _interestingtopicsevent, 
  _interestingtopicseventclassification,
  exp._accountid, _abmstatus, _abmlink,    _abmcreateddate AS _abmcreateddate,
   ROW_NUMBER() OVER( PARTITION BY acc.associated_company.company_id,vid, _interestingtopicid ORDER BY property_lastmodifieddate.value DESC) AS _rownum,
 FROM `x-marketing.logicsource_hubspot.contacts` acc
 JOIN (SELECT * FROM `x-marketing.logicsource_mysql.db_contact_experience` WHERE _sdc_deleted_at IS NULL ) exp on CAST(vid AS STRING) = exp._contactid 
   LEFT JOIN `x-marketing.logicsource_mysql.db_account_news`  news ON exp._accountid = news. _accountid 
  )
   WHERE _rownum = 1
), contact AS (
  SELECT 
  CAST(vid AS STRING) AS contact_id,
  properties.firstname.value AS _firstName,
  properties.lastname.value AS _lastName,
  properties.jobtitle.value AS _jobTitle,
  properties.email.value AS _email,
  properties.mobilephone.value AS _mobile_phone,
  properties.phone.value AS _phone,
  properties.state.value AS _state,
  properties.country.value AS _country,
  properties.linkedin__c.value AS _linked_c,
  properties.lead_segment.value AS _leadSegment,
  properties.management_level.value AS _managementLevel,
  properties.job_role.value AS _jobRole,
  properties.hs_marketable_reason_id.value AS _marketableReasonID,
  properties.hs_latest_source.value AS _latestSource,
  properties.hs_analytics_source.value AS _analyticsSource,
  properties.hubspotscore.value AS _score,
  properties.hs_analytics_num_page_views.value AS _analyticsNumPageViews,
  properties.hs_analytics_last_timestamp.value AS _analyticsLastTimestamp,
  properties.hs_email_delivered.value AS _emailDelivered,
  properties.hs_email_click.value AS _emailClick,
    _experienceid,
  _experienceyear, 
  _experiencetitle,
  _experiencecompany,
  _accountid AS account_id
   FROM `x-marketing.logicsource_hubspot.contacts`
    JOIN (SELECT * FROM `x-marketing.logicsource_mysql.db_contact_experience` WHERE _sdc_deleted_at IS NULL ) exp on CAST(vid AS STRING)= exp._contactid 
    --WHERE  properties.email.value = 'trina_gizel@echo-usa.com'
    
 ), hubspot_activites AS (
  
WITH hubspot_activity AS (
  SELECT 
  CAST(associated_company.company_id AS STRING) AS company_id,
  property_hs_analytics_first_url.value AS urls, 
  "Page View" AS _hubspotactivitesdetails,
  CAST(vid AS STRING)  contact_id, 
  property_hs_analytics_first_visit_timestamp.value AS _timestamp
FROM `x-marketing.logicsource_hubspot.contacts` 
--WHERE associated_company.company_id = 15864846915
UNION ALL 
SELECT CAST(associated_company.company_id AS STRING),property_hs_analytics_last_url.value, "Page View",CAST(vid AS STRING), property_hs_analytics_last_timestamp.value 
FROM `x-marketing.logicsource_hubspot.contacts`
UNION ALL 
SELECT CAST(associated_company.company_id AS STRING),form.value.title, "Form submission",CAST(vid AS STRING),form.value.timestamp,
 FROM
        `x-marketing.logicsource_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      JOIN
        `x-marketing.logicsource_hubspot.forms` forms
      ON
        form.value.form_id = forms.guid
      UNION ALL 
      SELECT CAST(_company_id AS STRING), _contentTitle, CONCAT("Email ", _engagement),_prospectID,_timestamp FROM `x-marketing.logicsource.db_email_engagements_log` 
WHERE _engagement IN ( "Clicked", "Opened")
)
--SELECT * FROM abm_hubspot

 --UNION ALL
SELECT hubspot_activity.* FROM hubspot_activity
JOIN (SELECT DISTINCT _contactid  FROM `x-marketing.logicsource_mysql.db_contact_experience` 
WHERE _sdc_deleted_at IS NULL ) contact ON hubspot_activity. contact_id = contact._contactid
 WHERE urls IS NOT NULL

 ),hubspot_data AS (
  SELECT CAST(vid AS STRING) AS contact_id, 
property_first_conversion_event_name.value AS first_conversion, 
property_lifecyclestage.value AS _lifecyclestage, 
property_hubspotscore.value AS _hubspotscore, 
property_tell_us_more.value AS  _tell_us_more, 
property_lead_segment__c.value AS _lead_segment__c,
property_ip_country.value AS _ip_country, 
property_ip_country_code.value AS _ip_country_code,
property_job_function.value AS _job_function, 
property_mobilephone.value AS _mobilephone
FROM `x-marketing.logicsource_hubspot.contacts` 
 )
 SELECT * ,
 --ROW_NUMBER() OVER( PARTITION BY property_email.value ORDER BY property_lastmodifieddate.value DESC) AS _rownum,
 FROM ( 
 SELECT contact.*,
 hubspot_activites.* EXCEPT (contact_id,company_id),
 hubspot_data.* EXCEPT (  contact_id),
 account.* EXCEPT ( account_id, contact_id) 
 FROM contact
 LEFT JOIN hubspot_activites ON contact.contact_id = hubspot_activites.contact_id
 LEFT JOIN hubspot_data ON contact.contact_id = hubspot_data.contact_id
 LEFT JOIN account ON contact.account_id = account.account_id
 )
 --WHERE contact.contact_id  = '104401'
 --WHERE _companyname IS NOT NULL