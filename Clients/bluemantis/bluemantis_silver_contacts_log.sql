TRUNCATE TABLE `x-marketing.bluemantis.contacts_log`;
INSERT INTO `x-marketing.bluemantis.contacts_log` ( 
  _lead_id,
  _last_name,
  _first_name,
  _job_title,
  _email,
  _owner_id,
  _converted_contact_id,
  _converted_account_id,
  _converted_opportunity_id,
  _lead_source,
  _company,
  _lead_created_date,
  _lead_status,
  _industry,
  _state,
  _country,
  _owner_name,
  _mql_first_date,
  _mql_create_date,
  _sql_create_date,
  _converted_date,
  _qualified_lead_status,
  _lead_source_description
)
SELECT 
  leads.id AS _lead_id,
  leads.lastname AS _last_name, 
  leads.firstname AS _first_name,
  leads.title AS _job_title,
  leads.email AS _email,
  ownerid AS _owner_id,
  convertedcontactid AS _converted_contact_id,
  convertedaccountid AS _converted_account_id,
  convertedopportunityid AS _converted_opportunity_id,
  leadsource AS _lead_source,
  company AS _company,
  leads.createddate AS _lead_created_date,
  status AS _lead_status,
  industry AS _industry,
  leads.state AS _state,
  leads.country AS _country,
  user.name AS _owner_name,
  mql_first_date__c AS _mql_first_date, 
  mql_create_date__c AS _mql_create_date, 
  sql_create_date__c AS _sql_create_date, 
  converted_date__c AS _converted_date, 
  qualified_lead_status__c AS _qualified_lead_status,
  lead_source_description__c AS _lead_source_description
FROM `x-marketing.bluemantis_salesforce.Lead` leads
LEFT JOIN `x-marketing.bluemantis_salesforce.User` user
  ON user.id = ownerid
WHERE isdeleted IS FALSE