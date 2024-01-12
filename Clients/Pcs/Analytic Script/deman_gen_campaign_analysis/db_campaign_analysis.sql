TRUNCATE TABLE `x-marketing.pcs.db_campaign_analysis`;

INSERT INTO `x-marketing.pcs.db_campaign_analysis` (

  _sdc_sequence, 
  _prospectID, 
  _campaignID, 
  _engagement, 
  _email, 
  _timestamp,
  _lastday_timestamp, 
  _name, 
  _company, 
  _territory, 
  --_categoryID, 
  _state, 
  --_segment,
  _campaignSentDate, 
  _contentTitle, 
  _campaignSendDate, 
  _status, 
  _emailID, 
  _subject, 
  _fromname, 
  _utm_campaign, 
  _campaignCreatedDate,
  _landingPage,
  firm_crd__c,
  individual_crd__c,
  data_link2__ddl_firmid__c,
  data_link2__ddl_repid__c,
  _contentID,
  _screenshot,
  _whatwedo,
  campaignName,
  _campaign_live_date,
  _utm_source,
  _rootcampaign,
  _utm_medium,
  _pardotid,
  _landingpage_airtable,
  _url_param,
  _download_utm_content, 
  _download_utm_source, 
  _download_utm_medium, 
  _download_content_downloaded,
  _phone,
  _type,
  _linked_clicked,
  _lead_status,
  _leadsource,
  _salesforceLeadStage,
  _Salesforceownername, 
  _Salesforceownerid,
  _target,
  average_retirement_plan_size_aua__c, 
  number_of_retirement_plans__c, 
  retirement_aum__c, 
  of_plans_acquired_per_year__c,
  mql_source__c,
  convertedcontactid,
  convertedopportunityid,
  isconverted,
  converteddate,
  convertedaccountid,
  isdeleted,
  leadid,  
  field, 
  oldvalue, 
  newvalue, 
  segment__c,
  _email_segment, 
  masterrecordid,
  Salesforce_Link,
  new_lead_status, 
  new_mql_source,
  salesforce_mastercontactid,
  _lead_score,
  _mql_dates,
   planname__c, 
   plan_lead_name, 
   excl_pcs_revenue__c, 
   forecast_amount_of_assets__c, 
   participants__c, 
   PROP_Total_Participants__c_n, 
   StageName__c, 
   WIN_E_mail_Date__c, 
   Converted_to_New_Plan__c, 
   Converted_to_New_Plan__c_name, 
   plan_id,
   _showExport,
   _dropped,
   _isBot
  --, _salesforce_lead_status,_last_timestamp_sec
  )
 WITH prospect_info AS (
  WITH  plan AS (
  SELECT p.id, p.name AS Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.Plan__c`  p
JOIN `x-marketing.pcs_salesforce.PlanLead__c`  l ON p.id = l.Converted_to_New_Plan__c

 ),planlead AS (
  SELECT name AS plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, forecast_amount_of_assets__c, participants__c,PROP_Total_Participants__c,StageName__c,WIN_E_mail_Date__c,	Converted_to_New_Plan__c,c.id AS plan_id, advisor__c,Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.PlanLead__c` c
LEFT JOIN plan on c.converted_to_new_plan__c = plan.id
 )
 , opps_id  AS  (
    SELECT l.id AS ops_lead_id,g.id as  opsid
FROM `x-marketing.pcs_salesforce.Lead` l
JOIN `x-marketing.pcs_salesforce.Opportunity` g ON l.id = g.lead_id__c
WHERE  g.recordtypeid  = '0125x00000071cnAAA'
 ), lead_history AS
(
  SELECT leadid,field,oldvalue,newvalue
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('leadConverted','leadMerged')
) ,status_change AS (
     SELECT  
    news.status AS new_status,
    old.status AS old_status,
    old.masterrecordid AS old_masterrecordid ,
    old.id AS old_id,
    news.id AS new_id,
    CASE WHEN news.status = old.status THEN TRUE ELSE FALSE END AS _status_different,
    old.email AS _old_email,news.email AS _new_email,
    news.mql_source__c AS mql_source,
    news.mql_date__c
FROM `x-marketing.pcs_salesforce.Lead` old
JOIN `x-marketing.pcs_salesforce.Lead` News ON old.masterrecordid=news.id
)
, contact AS
(
  SELECT id AS _contactid,
  segment__c,leadsource
  FROM `x-marketing.pcs_salesforce.Contact`
)
, leads AS
(
    SELECT
    routable.name, 
    routable.firstname, 
    routable.lastname, 
    routable.id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    routable.title, 
    dd_bd_title_categories__c, 
    routable.phone, 
    CASE WHEN state._code IS NULL THEN IF(UPPER(routable.state) = 'INDIANA', 'IN', UPPER(routable.state)) ELSE UPPER(state._code) END AS _state, 
    routable.email,
    leadsource,
    CASE WHEN status = "Open" THEN 1 
          WHEN status = "Nurture" THEN 6
          WHEN status = "Sales Qualified" THEN 5 
          WHEN status = "Archived" THEN 7
          WHEN status = "Contacted" THEN 3
          WHEN status= "Engaged" THEN 4
          WHEN status = "Unqualified" THEN 8
          WHEN status = "New" THEN 2
          END AS _salesforceLeadStage,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',routable.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    routable.ownerid AS ownerid, 
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c,
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
   CASE WHEN routable.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN routable.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
WHEN routable.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c END AS mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    CAST(converteddate AS DATETIME) AS converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS link_m,
    total_lead_score__c,
    CASE WHEN mql_date__c IS NULL THEN routable.createddate ELSE mql_date__c END AS _mql_date,
    routable.createddate
    

    /*
    dd_current_ria_firm_1_crd__c, 
    dd_current_ria_firm_2_crd__c, 
    dd_current_bd_firm_1_crd__c, 
    dd_primary_bd_firm_crd__c, 
    dd_primary_ria_firm_crd__c, 
    dd_primary_firm_crd__c, 
    dd_prior_firm_2_firm_crd__c, 
    dd_prior_firm_1_firm_crd__c, 
    dd_prior_firm_3_firm_crd__c,
    dd_branch_address_id__c,individual_crd__c,
    firm_crd__c,
    dd_home_address_id__c*/
    FROM `x-marketing.pcs_salesforce.Lead` routable
    LEFT JOIN `x-marketing.pcs.db_state_code_lookup` state on CAST(routable.state AS STRING) = CAST(state._state AS STRING) or routable.state = state._code
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = routable.ownerid
    --WHERE id = '00Q5x000021VdQjEAK'
)
SELECT * EXCEPT(_rownum,createddate)
FROM (
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY email,id ORDER BY createddate DESC) AS _rownum
FROM (
SELECT name, 
    firstname, 
    lastname, 
    id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    title, 
    dd_bd_title_categories__c, 
    phone, 
    _state, 
    email,
    leads.leadsource,
    _salesforceLeadStage,
    link,
    ownername,
    ownerid, 
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c,
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    mql_source__c ,
    convertedcontactid,
    opsid AS  convertedopportunityid,
    isconverted,
    converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
    link_m,
    lead_history.*,
    segment__c,
    new_status,
    mql_source,total_lead_score__c,_mql_date,
createddate,
plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, forecast_amount_of_assets__c, participants__c,PROP_Total_Participants__c,StageName__c,WIN_E_mail_Date__c,	Converted_to_New_Plan__c,Converted_to_New_Plan__c_name,plan_id, 
FROM leads
LEFT JOIN contact ON leads.convertedcontactid = contact._contactid
LEFT JOIN status_change ON leads.id = old_id
LEFT JOIN lead_history ON leads.id = leadid
LEFT JOIN opps_id  ON leads.id = opps_id.ops_lead_id
LEFT JOIN planlead  ON leads.convertedcontactid = planlead.advisor__c
) 
)
WHERE _rownum = 1
    
),email_campaign AS (
    SELECT * FROM (
    SELECT *,  ROW_NUMBER() OVER(PARTITION BY id,_code ORDER BY _livedate DESC) AS _rownum
    FROM (
    SELECT DISTINCT 
  _notes, 
  _status, 
  _trimcode, 
  _screenshot, _assettitle, _subject, _whatwedo, _campaignid, _utm_campaign, _preview, _code, _journeyname, _emailsegment AS _campaignname, _formsubmission, _id, _livedate, _utm_source, _emailname, _assignee, _utm_medium, _landingpage,
 -- CASE 
 /*WHEN _code = 'DG_EM1_W1_D1' THEN 125883
  WHEN _code ='DG_EM1_W1_D2' THEN 125885
  WHEN _code ='DG_EM1_W1_D3' THEN 125887
  WHEN _code ='DG_EM1_W1_D4' THEN 127013
  WHEN _code ='DG_EM1_W1_D5' THEN 128177
  WHEN _code ='DG_EM1_W1_D6' THEN 128515*/
  /*WHEN _code ='DG_EM1_W2_D1' THEN '129927'
  WHEN _code ='DG_EM1_W2_D2' THEN '130701'
  WHEN _code ='DG_EM1_W2_D3' THEN '131367'
  WHEN _code ='DG_EM1_W2_D4' THEN '131369'
  WHEN _code ='DG_EM6_W1-D2' THEN '160129'*/
  /*WHEN _code ='DG_EM2_W1_D2' THEN 136530
  WHEN _code ='DG_EM2_W1_D3' THEN 136532
  WHEN _code ='DG_EM2_W2_D1' THEN 138604
  WHEN _code ='DG_EM2_W2_D2' THEN 139767
  WHEN _code ='DG_EM2_W2_D3' THEN 139769
  WHEN _code ='DG_EM3_W1_D1' THEN 143847
  WHEN _code ='DG_EM3_W1_D2' THEN 143850
  WHEN _code ='DG_EM3_W2_D1' THEN 146385
  WHEN _code ='DG_EM3_W2_D2' THEN 146387
  WHEN _code ='DG_EM4_W1-D1' THEN 149402
  WHEN _code ='DG_EM4_W1-D2' THEN 149404
  WHEN _code ='DG_EM4_W2-D1' THEN 151003
  WHEN _code ='DG_EM4_W2-D2' THEN 151005*/ 
  --ELSE 
  _campaignid 
  --END 
  AS id,
  /*CASE WHEN _code = 'DG_EM1_W1_D1' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W1_D2' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W1_D3' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W1_D4' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W1_D5' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W1_D6' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W2_D1' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W2_D2' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W2_D3' THEN 'Email 1'
      WHEN _code = 'DG_EM1_W2_D4' THEN 'Email 1'
      WHEN _code = 'DG_EM2_W1_D1' THEN 'Email 2'
      WHEN _code = 'DG_EM2_W1_D2' THEN 'Email 2'
      WHEN _code = 'DG_EM2_W1_D3' THEN 'Email 2'
      WHEN _code = 'DG_EM2_W2_D1' THEN 'Email 2'
      WHEN _code = 'DG_EM2_W2_D2' THEN 'Email 2'
      WHEN _code = 'DG_EM2_W2_D3' THEN 'Email 2'
      WHEN _code = 'DG_EM3_W1_D1' THEN 'Email 3'
      WHEN _code = 'DG_EM3_W1_D2' THEN 'Email 3'
      WHEN _code = 'DG_EM3_W2_D1' THEN 'Email 3'
      WHEN _code = 'DG_EM3_W2_D2' THEN 'Email 3'
      WHEN _code = 'DG_EM4_W1-D1' THEN 'Email 4'
      WHEN _code = 'DG_EM4_W1-D2' THEN 'Email 4'
      WHEN _code = 'DG_EM4_W2-D1' THEN 'Email 4'
      WHEN _code = 'DG_EM4_W2-D2' THEN 'Email 4'
      WHEN _code LIKE  '%DG_EM5%' THEN 'Email 5'
      WHEN _code LIKE  '%DG_EM6%' THEN 'Email 6'
      WHEN _code LIKE  '%DG_EM7%' THEN 'Email 7'
    WHEN _code LIKE  '%DG_EM8%' THEN 'Email 8'
    WHEN _code LIKE  '%DG_EM9%' THEN 'Email 9'
    WHEN _code LIKE  '%DG_EM10%' THEN 'Email 10'
    WHEN _code LIKE  '%2023-04-18_PCS-EM-01_ADGN_C%' THEN 'Cold Nurture Email 1'
    WHEN _code LIKE  '%2023-04-27_PCS-EM-01_ADGN_MH%' THEN 'Mod Hot Nurture Email 1'
    WHEN _code LIKE  '%2023-05-02_PCS-EM-02_ADGN_C%' THEN 'Cold Nurture Email 2'
    WHEN _code LIKE  '%2023-05-04_PCS-EM-02_ADGN_MH%' THEN 'Mod Hot Nurture Email 2'
     END AS*//*CASE  WHEN _code ='DG_EM1_W2_D1' THEN 'Email 1'
  WHEN _code ='DG_EM1_W2_D2' THEN 'Email 1'
  WHEN _code ='DG_EM1_W2_D3' THEN 'Email 1'
    WHEN _code ='DG_EM1_W2_D4' THEN 'Email 1'
  WHEN _code ='DG_EM6_W1-D2' THEN 'Email 1'
      WHEN _campaignID  = '160127' THEN 'Email 6'
      WHEN _campaignID  = '169425' THEN 'Email 8'ELSE */
      _emailsequence 
      --END 
      AS _email_segment

  FROM `x-marketing.pcs_mysql.db_airtable_email_participant_engagement` 
  WHERE _rootcampaign = 'Demand Generation' AND  _campaignID  <> 'Obtain from DE>Campaign>Email JobID' 
  AND _id <> 3321
  ORDER BY _code
    )
    ) WHERE _rownum = 1 

)
,airtable AS (
  SELECT * EXCEPT(_rownum)
  FROM (
  SELECT 
  sentdate, 
  name.value.name AS _utm_campaign, 
  senddate, 
  status, 
  airtable.emailid, 
  TRIM(subject) AS subject, 
  fromname, 
  TRIM(airtable.emailname) AS emailname, 
  --fromaddress, 
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",airtable.createddate) AS createddate, 
  isalwayson,
  airtable.id,
  _code,_trimcode, _screenshot, _assettitle, _subject, _preview AS _whatwedo, _campaignname AS campaignName, _id, 
  safe.timestamp(_livedate) AS _livedate, 
  _utm_source, _utm_medium, _landingpage,_journeyname,_email_segment,
  /*CASE WHEN airtable.id = 125885 THEN 'DG_W1_D2'
  WHEN airtable.id = 125883 THEN 'DG_W1_D1'
  WHEN airtable.id = 125887 THEN 'DG_W1_D3'
  WHEN airtable.id = 127013 THEN 'DG_W1_D4' 
  WHEN airtable.id = 128177 THEN 'DG_W1_D5' 
  WHEN airtable.id = 128515  THEN 'DG_W1_D6' 
  WHEN airtable.id = 129927  THEN 'DG_E1_W2_D1'
  WHEN airtable.id = 130701  THEN 'DG_E1_W2_D2'
  WHEN airtable.id = 131369  THEN 'DG_E1_W2_D4'
  WHEN airtable.id = 131367  THEN 'DG_E1_W2_D3' 
  WHEN airtable.id = 135657  THEN 'DG_EM2_W1_D1'
  WHEN airtable.id = 136530  THEN 'DG_EM2_W1_D2' 
  WHEN airtable.id = 125946 THEN 'DG-01' 
  ENd AS _type*/_code AS _type,
  ROW_NUMBER() OVER(PARTITION BY emailname,airtable.id,emailid,_email_segment ORDER BY senddate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
  JOIN  email_campaign ON airtable.id  = SAFE_CAST(email_campaign.id AS INT64)
  )
  WHERE _rownum = 1

), open_event AS (
  SELECT * EXCEPT(_rownum)
  FROM (
  SELECT
  activity._sdc_sequence AS _scd_sequence,
  subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Opened' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  url,'' AS utm_source ,'' AS utm_content, '' AS utm_medium, '' AS content_downloaded,
 '' AS _linked_clicked,
  ROW_NUMBER() OVER(PARTITION BY email,sendid ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  WHERE eventtype = 'Open' 
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'*/
  ORDER BY email asc
   )
  WHERE _rownum = 1
),click_event AS (
/*SELECT * EXCEPT (_rownum)
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC) AS _rownum FROM (
  SELECT
  activity._sdc_sequence AS _scd_sequence,
  activity.subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Clicked' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  linkclick.url,
  '' AS utm_source ,'' AS utm_content, '' AS utm_medium, '' AS content_downloaded,
  CASE WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy'
  WHEN activity.url LIKE '%PCSRetirement.accountsvc.com%' THEN 'DG-EM-01-LP'
   ELSE 'Empty' END AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or activity.subscriberkey = id
  LEFT JOIN (SELECT * FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks`  WHERE linkname LIKE '%DG-EM%' )linkclick ON activity.subscriberkey = linkclick.subscriberkey AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,linkclick.clickdate )AS DATE) 
  WHERE eventtype = 'Click'
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'*/ 
  /*UNION ALL 
  SELECT 
  linkclick._sdc_sequence,
  activity.subscriberkey,
  CAST(sendid AS STRING) AS _campaignID, 
  'Clicked' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,clickdate),
    CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  state, 
  linkclick.url AS urlss,
  '' AS utm_source ,
  '' AS utm_content, 
  '' AS utm_medium, 
  '' AS content_downloaded,
  linkname, 
FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks` linkclick
JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ /*linkclick.subscriberkey = l.id
LEFT JOIN `x-marketing.pcs_sfmc.event` activity ON activity.subscriberkey = linkclick.subscriberkey AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,linkclick.clickdate )AS DATE)
WHERE eventtype = 'Click' AND linkname LIKE '%DG-EM-01-LP%'*/
--))where _rownum = 1 
    SELECT * 
--EXCEPT(_rownum)
  FROM (  SELECT
  activity._sdc_sequence AS _scd_sequence,
  activity.subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Clicked' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  activity.url,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_source=') + 11), '&')[ORDINAL(1)]  AS utm_source ,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_content=') + 12), '&')[ORDINAL(1)]  AS utm_content, 
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_medium=') + 11), '&')[ORDINAL(1)] AS utm_medium,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'content_downloaded=') + 19), '&')[ORDINAL(1)]  AS content_downloaded,
  CASE WHEN activity.url LIKE '%link=botclick%' THEN 'Bot'
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=a1750c00d375757a537cb77b861e575b971442e6e414532e846b66934db994e944342936425ed54bee62d0ad8e13faf757cb98b38b8f815b73d44c9caed2203f8a0da121cadb3e86a61a007661569887%" THEN "PL_DG_22_EM15"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=a151ee28ee321868fe1eb06406ee4c83c89cef65eb35f1f72e57748751b538fc2b01752ec938ea2a2f8ed18b4e6569fc8b79455702a2526f6b201c6ee37b5d61813237b7efa21189eb88856daa059b96%" THEN "PL_DG_22_EM14"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=c61d8af15a2a4939c27281231f9d00dd51568cb49d4c51ef60a0c0050a19e9f250297e1b488bfbfb8e3686e01869a8ac1b4cad905c1bbd2242ce4687ffaca31a26ed5102e35f73bcf0b771d1ca5d260d%" THEN "PL_DG_22_EM13"
  WHEN activity.url LIKE "%https://click.accountsvc.com/?qs=1dca42e1fbbcba1099479edea74867122249b193196f414d3bb302d0d49236e222d43cf202b5e524ffb983ed349a254e4d8127fc9b74a126%" THEN "PL_DG_22_EM12"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=11560409d02742177b2afd6f863255a14da554350fcdc2ed56e4f7e351a0953f0e13105889f6e941828fbaba38bcef42c7c71f926069a3d772226d23aa71dd5e83fe3c8e6a745bd82dd20baf49a0eee8%" THEN "PL_DG_22_EM11"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=9efefb7a5f716f10590a8c441168caee7686cc1bca8d585d7a2f2aae68790185dd5edbb9089b2c80e8d5283b26cece2b8109cd11b3391bf66db0ae6c24693dc12bde19901a0e34073b1d3a881339fe91%" THEN "PL_DG_22_EM10"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=3c6f1949452cb9be9b229937ee9794630d218195c89032c846de4224563fa93fc6a68e1757a50a797faa80a98867b3504fa7c86abe88fec714173effd5f0d57db87d21a41761594b9e6f99a16781f607%" THEN "PL_DG_22_EM9"
  WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=fec83371983e29009de250b63c4d8eedb5051d341518a99643452651328e9f23a33c03aa4155352831c3e10caa620a5959a771bb35b8832e8458778a27e427f21ac60337426d5dc9fb16e2e27dab0404%" THEN "PL_DG_22_EM8"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=fedaa84323cb39ca11dc3c421f4c176af6cbf5cb6ec87d8864e2c7f296151ac211164efa8989f7cacfd238e1ca59244ba3e8e96bbe0f80fc8abac6855a1164416defb3f8b4c5822a85c79008b846f410%" THEN "PL_DG_22_EM7"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=c36003a4012e8d9229f06301c348ddffecbadd5d2e82c4fb11f2bf41f54cda52e701d2a653cd3978b8b12fadb04fbd922aabcb2620455a1b035f9d5056618d7c0598a2fba7893cf45cfceb5a941a4fea%" THEN "PL_DG_22_EM6"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=2b269b8172be0537c510ea8eda8dbd1690101f764d3d5f2c7d0fe88ff4138a69fa65a208ee40ba2db7c31c2dd48a07afae1f2fd31ca114837f6a9f080734fab790e213c8375771da0dff5c2960131597%" THEN "PL_DG_22_EM5"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=175ee85cddf2409f3c02c9c9fd71cba1f76cd57f8b265482dd8343d85d5f11e984979a624a5c5d4be66adfafbc758cd8bb40ead913cc426d16dc3985bbd01ffaa084d80fa30540f0b95e7c60551221cc%" THEN "PL_DG_22_EM4"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=73577fed4cf96d3157e31408fe4cdd8d9d05692460779bd52fcfd62846d722987dea421b93fd1dcb87f71b5f6e9ebbec45ce0736182a6b268de8b794cc5091f02e23613f9d7e6bbd78ccf69bb997db7b%" THEN "PL_DG_22_EM3"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=a20f46b6052c6899090975eeae4918da43285560bd881821423cbd3198665a8587338d43d57ca5a41137b159de5bd842cbb5855bef02bb16f574935370f92024b942844c03944eecffe43d5afca320fd%" THEN "PL_DG_22_EM2"
 WHEN activity.url LIKE "%https://view.accountsvc.com/?qs=e29027e2a262872b9b383c07c321ea0c20497e86d3a5beb55ffb34a18a1f9f53874c7acd16fa6ca655d99557904847d976e352ab661c6eb6c4855effbf33a01adb28468ac8c8ccc07423e5e5f687a424%" THEN "PL_DG_22_EM1"
 WHEN activity.url LIKE  "%click.accountsvc.com/unsub_center.aspx?%" THEN "Unsubscribe"
 WHEN activity.url LIKE    "%click.accountsvc.com/subscription_center.aspx?%" THEN "Subscription Center"
  WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'Bot'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy'
  WHEN activity.url LIKE "%utm_content=cta_button01%" THEN "DG-EM-01-LP-A (cta_button01)"
  WHEN activity.url LIKE "%utm_content=cta_img%" THEN "DG-EM-01-LP-B (cta_img)"
  WHEN activity.url LIKE "%utm_content=cta_text01%" THEN "DG-EM-01-LP-C (cta_text01)"
  WHEN activity.url LIKE "%utm_content=cta_button02%" THEN "DG-EM-01-LP-D (cta_button02)"
   WHEN activity.url LIKE "%utm_content=cta_text%" THEN "DG-EM-01-LP-E (cta_text)"
  WHEN activity.url LIKE "%content_downloaded=%" THEN "Content_downloaded"
  --WHEN linkclick.url LIKE '%PCSRetirement.accountsvc.com%' OR linkclick.url LIKE '%pcsretirement.accountsvc.com%' THEN 'DG-EM-01-LP'
   ELSE 'Empty' END AS _linked_clicked,
  --ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.url,activity.subscriberkey ORDER BY eventdate DESC, activity.url DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  /*LEFT JOIN (SELECT subscriberkey, emailname, url, sf, linkname,id, clickdate FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks`  WHERE 
  linkname LIKE '%G-EM%' 
  UNION ALL 
  SELECT subscriberkey, emailname, url, sf, linkname,id, clickdate FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks___Nurture___Mod_Hot` 
  WHERE linkname LIKE '%DGN%' 
    UNION ALL 
  SELECT subscriberkey, emailname, url, sf, linkname,id, clickdate FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks___Nurture` 
  WHERE linkname LIKE '%DGN%' 
  )linkclick ON activity.subscriberkey = linkclick.subscriberkey AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,linkclick.clickdate )AS DATE) */
  WHERE eventtype = 'Click' 
  --AND activity.subscriberkey = '00Q5x00001wOTneEAG'
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'*/
  ORDER BY email asc
  ) 
  --WHERE 
  --_linked_clicked = "DG-EM-01-LP-B (cta_img)" AND
   --_rownum = 1
  ORDER BY _timestamp DESC
),sent_event AS (
 SELECT * EXCEPT(_rownum)
  FROM (
  SELECT
  activity._sdc_sequence AS _scd_sequence,
  subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Sent' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  url,'' AS utm_source ,'' AS utm_content, '' AS utm_medium, '' AS content_downloaded,
  '' AS _linked_clicked,
  ROW_NUMBER() OVER(PARTITION BY email,sendid ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  WHERE eventtype = 'Sent' 
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'*/
  ORDER BY email asc
   )
  WHERE _rownum = 1
),HardBounced_event AS (
  SELECT * EXCEPT(_rownum)
  FROM
  (
    SELECT
  activity._sdc_sequence AS _scd_sequence,
  subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  ELSE bounce_category END AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  url,'' AS utm_source ,'' AS utm_content, '' AS utm_medium, '' AS content_downloaded,
  '' AS _linked_clicked,
  ROW_NUMBER() OVER(PARTITION BY email,sendid ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  JOIN (
    SELECT * FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce`
    UNION ALL 
    SELECT * FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture___Mod_Hot`
    UNION ALl 
    SELECT * FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture`
    )  Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
  WHERE eventtype IN ('HardBounce','OtherBounce','SoftBounce') 
  --AND subscriberkey NOT IN (SELECT subscriberkey FROM `x-marketing.pcs_sfmc.event` WHERE eventtype IN( 'Click','Open'))
  )WHERE _rownum = 1
),unsubscribe_event AS (
 SELECT * EXCEPT(_rownum)
  FROM (
  SELECT
  activity._sdc_sequence AS _scd_sequence,
  subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Unsubscribe' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  url,'' AS utm_source ,
  '' AS utm_content, 
  '' AS utm_medium, 
  '' AS content_downloaded,
  '' AS _linked_clicked,
  ROW_NUMBER() OVER(PARTITION BY email,sendid ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  WHERE eventtype = 'Unsubscribe'
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'*/
  ORDER BY email asc
   )
  WHERE _rownum = 1
), lead_form_download AS (

 WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form`  activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
    CASE WHEN CAST(sendid AS STRING) = '149402' THEN '129927' ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
     JOIN (SELECT *,   CASE WHEN subscriberkey = '00Q5x00001wOAH7EAO' THEN 'EM_2022-09-15_PCS-EM-01_ADG_W2'  ELSE REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') END AS _campaign FROM `x-marketing.pcs_sfmc.event` ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
    
  
) , lead_form_download2 AS (

  WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_2`  activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
     JOIN (SELECT *,   CASE WHEN subscriberkey = '00Q5x00001wOAH7EAO' THEN 'EM_2022-09-15_PCS-EM-01_ADG_W2'  ELSE REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') END AS _campaign FROM `x-marketing.pcs_sfmc.event` ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
  
) , lead_form_download3 AS (
   WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_3`  activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
    CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847' ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847' ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click'AND sendid IN (137757,146385,146387,143847,143850) ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1 
)
, lead_form_downloaded4 AS (
   WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO'
   WHEN email_address = 'joeweber@myifs.com' THEN '00Q5x00001wO49NEAS' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_4`  activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
    CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847' ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
    UNION ALL 
       SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CASE WHEN email_address = 'joeweber@myifs.com' THEN '146385' ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   LEFT JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847' ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE email_address = 'joeweber@myifs.com'
        ))WHERE _rownum = 1 
)
, lead_form_downloaded5 AS (
   WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_5 activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
    CASE WHEN email_address = 'freddie@cassillyfinancial.com' THEN '155998'
    WHEN email_address = 'gp@stratlegacy.com' THEN '155998'
    WHEN email_address = 'randall@park10financial.com' THEN '155998'
    WHEN email_address = 'adam@vaalfinancial.com' THEN '155998' ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
    UNION ALL 
     SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
    CASE WHEN email_address = '007albertida@gmail.com' THEN '155998' ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   LEFT JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847' ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE email_address = '007albertida@gmail.com'
        ))WHERE _rownum = 1
), lead_form_download6 AS (
    WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_6 activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
), lead_form_download7 AS ( 
    WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_7 activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
),lead_form_download8 AS (
      WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_8` activity
    WHERE email_address NOT LIKE "%2x.marketing%" AND lead_id <>  '00Q5x00001wO6pGEAS'
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1

),lead_form_download10 AS (
     WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_10 activity
    WHERE email_address NOT LIKE "%2x.marketing%" AND lead_id <>  '00Q5x00001wO6pGEAS'
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
),lead_form_nature_cold AS (
     WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_PCS_Demand_Gen_Cold_Nurture_Campaign activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
),lead_form_mod_hot AS (
    WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' 
   WHEN email_address = 'melanie@larmannfinancial.com' THEN '00Q5x00001wNrrBEAS'
   WHEN email_address = 'sunflower.0606@outlook.com' THEN '00Q5x00001wO9I7EAK'
    ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com'
     WHEN email_address = 'melanie@larmannfinancial.com' THEN 'matt@larmannfinancial.com' 
      ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CASE WHEN CONCAT(first_name, ' ', last_name ) = 'Melanie Jacobsen' THEN 'Matthew Larmann' ELSE CONCAT(first_name, ' ', last_name ) END AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,CASE WHEN lead_id =  '00Q5x00001zzSTVEA2' THEN 'DG_N' ELSE utm_campaign END AS utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_PCS_Demand_Gen_Mod_Hot_Nurture_Campaign activity
    WHERE email_address NOT LIKE "%2x.marketing%" 

   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CASE WHEN l.id = '00Q5x00001wO9I7EAK' THEN "189106" ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    LEFT JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND utm_campaign = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE l.id IN (  '00Q5x00001wO9I7EAK',"00Q5x00001wOOfGEAW")
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        )
        )WHERE _rownum = 1 AND _prospectID IS NOT NULL
),lead_form_download11 AS (
      WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_11` activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
) ,lead_form_download12 AS (
     WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_12` activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
        AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
) ,lead_form_download13 AS ( 
    WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_13` activity
    WHERE email_address NOT LIKE "%2x.marketing%" AND lead_id <>  '00Q5x00001wO6pGEAS'
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
   
 ),lead_form_download14 AS ( 
    WITH activity AS (
  SELECT * FROM (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO'
   WHEN email_address = 'karen.klassen@lpl.com' THEN '00Q5x000024mmBLEAY'
   WHEN lead_id =  '00Q5x000021WDw5EAG' THEN '00Q5x00001wNqAwEAK'
 
    ELSE lead_id END  AS lead_id,  
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' 
      WHEN lead_id =  '00Q5x000021WDw5EAG' THEN 'christopher.ruspi@lpl.com' 
     ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Lead_Form___Email_14` activity
  )
    WHERE email_address NOT LIKE "%2x.marketing%"  
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CASE WHEN l.id = '00Q5x000024YRXjEAO' THEN "212520" 
     WHEN l.id = '00Q5x00001wNqAwEAK' THEN "219169" ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   JOIN (SELECT * EXCEPT (subscriberkey), CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
   WHEN subscriberkey = 'bryan.chan@2x.marketing' AND CAST(sendid AS STRING) = '212439' THEN '212520' 
  
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign, 
    CASE WHEN subscriberkey = 'bryan.chan@2x.marketing' THEN "00Q5x000024YRXjEAO"
    WHEN subscriberkey = '00Q5x00001wNqAwEAK' THEN "00Q5x000024mmBLEAY"
    WHEN subscriberkey = '00Q5x000021WDw5EAG' THEN '00Q5x00001wNqAwEAK'
     ELSE subscriberkey END AS subscriberkey,
      FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1 

 ),lead_form_schwab AS (
     WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO'
    ELSE lead_id END  AS lead_id,  
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Schwab_Lead_Form activity
    WHERE email_address NOT LIKE "%2x.marketing%"  
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CASE WHEN l.id = '00Q5x000024YRXjEAO' THEN "212520" ELSE CAST(sendid AS STRING) END AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
   JOIN (SELECT * EXCEPT (subscriberkey), CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
   WHEN subscriberkey = 'bryan.chan@2x.marketing' AND CAST(sendid AS STRING) = '212439' THEN '212520' 
  
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign, CASE WHEN subscriberkey = 'bryan.chan@2x.marketing' THEN "00Q5x000024YRXjEAO" ELSE subscriberkey END AS subscriberkey,
      FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _campaign)
    JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1

 ), lead_universal AS (
   WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address ,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    --CONCAT(firstname, ' ', lastname ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM x-marketing.pcs_sfmc.data_extension_Demand_Gen_Campaign_Universal activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded') + 19), '&')[ORDINAL(1)], '%ModHot_', ' '), '%ModHot_',':') AS _campaign FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _campaign)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1

 ), 
 website AS (

  SELECT * 
  FROM (
  SELECT 
    _sdc_sequence AS _scd_sequence,
    k.id AS _prospectID,
    '0' AS _campaignid,
    'Web'  AS _event_type ,
    k.email AS _email,
    CASE WHEN mql_date__c IS NULL THEN  createddate ELSE mql_date__c END AS _timestamp,
    k.name AS _name,
    k.company AS _companyname,
    k.territory__c AS territory,
     k.state,
     '' AS url,
     k.leadsource AS utm_source ,
     '' AS utm_content, 
     '' AS utm_medium, 
     '' AS content_downloaded,
     phone AS phone_number,
    '' AS _linked_clicked,CASE WHEN k.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN k.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
WHEN k.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN k.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN k.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN k.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN k.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c  END AS mql_source__c,
    ROW_NUMBER() OVER(PARTITION BY email,k.id ORDER BY CASE WHEN mql_date__c IS NULL THEN  createddate ELSE mql_date__c END DESC) AS _rownum
  FROM `x-marketing.pcs_salesforce.Lead` k
  
  WHERE 
    k.leadsource = 'Web'/*AND k.status = 'New'*/ AND isdeleted IS NOT TRUE 
  )
  WHERE _rownum = 1
/*AND k.email not LIKE "%test.com%"*/
),lead_score AS (

  SELECT *
 FROM (
 SELECT
    k._sdc_sequence AS _scd_sequence,
    subscriberkey AS _prospectID,
    CASE WHEN k.id = '00Q5x00001wOYrqEAG' THEN "189106"
    ELSE CAST(sendid AS STRING) END AS _campaignID,
   'MQL Score'  AS _event_type ,
    k.email AS _email,
     PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
    --CASE WHEN mql_date__c IS NULL THEN  k.createddate ELSE mql_date__c END AS _timestamp,
    k.name AS _name,
    k.company AS _companyname,
    k.territory__c AS territory,
     k.state,
     '' AS url,
     k.leadsource AS utm_source ,
     '' AS utm_content, 
     '' AS utm_medium, 
     '' AS content_downloaded,
     phone AS phone_number,
    '' AS _linked_clicked,
    CASE WHEN k.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN k.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
    WHEN k.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN k.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN k.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN k.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN k.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c  END AS mql_source__c,

  ROW_NUMBER() OVER(PARTITION BY email ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  k ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
  JOIN airtable ON  activity.sendid = airtable.id
  WHERE eventtype NOT IN ('Sent','HardBounce','OtherBounce','SoftBounce','Unsubscribe') AND  mql_source__c = "MQL Score" AND

   total_lead_score__c >= 75
 AND isdeleted IS NOT TRUE AND k.id NOT IN ('00Q5x00001wOOBpEAO','00Q5x00001xufRBEAY','00Q5x00001wO02LEAS','00Q5x00001wNngXEAS','00Q5x00001wOJ3MEAW')
 ) WHERE _rownum = 1
)/*,downloaded AS (

  SELECT * EXCEPT(_rownum)
  FROM (
  SELECT
  activity._sdc_sequence AS _scd_sequence,
  subscriberkey AS _prospectID,
  CAST(sendid AS STRING) AS _campaignID,
  'Downloaded' AS _event_type,
  email AS _email,
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
  CONCAT(firstname, ' ', lastname ) AS _name, 
  company AS _companyname, 
  territory__c, 
  --categoryid, 
  state, 
  --segment,
  url,
  ROW_NUMBER() OVER(PARTITION BY email,sendid ORDER BY eventdate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or activity.subscriberkey = id
  WHERE eventtype = 'Click' 
  AND url like '%content_downloaded%' AND status = 'Open'
  /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
  AND subscriberkey NOT LIKE '%2x.marketing%'
  AND email NOT LIKE '%pcsretirement.com%' 
  AND email NOT LIKE '%2x.marketing%'
  ORDER BY email asc
   )
  WHERE _rownum = 1
  
  
)*/
--, all_engagement AS (
, click_download AS (
    WITH activity AS (
      SELECT
    activity._sdc_sequence AS _scd_sequence,
   CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE lead_id END  AS lead_id,
    --CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Downloaded'AS _event_type ,
     CASE WHEN lead_id =  '00Q5x00001wOUNEEA4' THEN 'j.trachta@fi.com'
     WHEN lead_id =  '00Q5x00001zzSI5EAM' THEN 'gbarjesteh@firstrepublic.com' ELSE  email_address END  email_address,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,activity.created_date ) AS _timestamp,
    CONCAT(first_name, ' ', last_name ) AS _name, 
    --l.company AS _companyname, 
    --territory__c AS territory__c, 
    --0 AS categoryid, 
    --state AS state, 
    --'' AS segment,
    --url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,utm_campaign
   
    FROM `x-marketing.pcs_sfmc.data_extension_Demand_Gen_Campaign_Ungated_Asset` activity
    WHERE email_address NOT LIKE "%2x.marketing%" 
   ) 
    SELECT * EXCEPT (_rownum)
 FROM (
    SELECT * , ROW_NUMBER() OVER(PARTITION BY _email,_campaignID ORDER BY LENGTH(url) DESC)  AS _rownum
    FROM (
    SELECT
    _scd_sequence AS _scd_sequence,
    l.id AS _prospectID,
     CAST(sendid AS STRING) AS _campaignID,
    --CAST(sendid AS STRING) AS _campaignID,
    'Clicked Downloaded'AS _event_type ,
     email_address AS _email,
    _timestamp,
    CONCAT(firstname, ' ', lastname ) AS _name, 
    l.company AS _companyname, 
    territory__c AS territory__c, 
    --0 AS categoryid, 
    state AS state, 
    --'' AS segment,
    url AS url,
    utm_source , utm_content, utm_medium, content_downloaded,
    '' AS _linked_clicked,
   FROM activity
    JOIN (SELECT *, CASE WHEN CAST(sendid AS STRING) = '137757' THEN '143847'
    ELSE CAST(sendid AS STRING) END AS _sendid,  REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
    
     FROM `x-marketing.pcs_sfmc.event`  WHERE eventtype = 'Click' ) campaignn ON (campaignn.subscriberkey = activity.lead_id 
        AND content_downloaded = _content_downloaded)
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
        --WHERE eventtype = 'Click' AND url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
), combine_all AS (
SELECT 
    engagements._scd_sequence,
    engagements._prospectID,
    engagements._campaignID,
    engagements._event_type,
    engagements._email,
    engagements._timestamp,
    LAST_DAY(CAST(_timestamp AS DATE), MONTH) AS _lastday_timestamp,
    engagements._name,
    engagements._companyname,
    engagements.territory__c,
    --engagements.categoryid,
    prospect_info._state,
    --engagements.segment,
    sentdate, 
    _utm_campaign, 
    senddate, 
    airtable.status, 
    CAST(emailid AS STRING), 
    subject, 
    fromname, 
    emailname, 
    --fromaddress, 
    createddate,
    engagements.url,
    firm_crd__c,
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    _code,
    _screenshot, 
   _whatwedo, 
    campaignName AS campaignName, 
    _livedate, 
    _utm_source, 
    _journeyname, 
    _utm_medium, 
    _code, 
    _landingpage,
    _landingpage,
    engagements.utm_source, 
    engagements.utm_content, 
    engagements.utm_medium, 
    engagements.content_downloaded,
    CASE WHEN convertedcontactid = "0035x00003Wv85RAAR" THEN "(651) 702-1513" ELSE phone END AS phone,
    _type,
    engagements._linked_clicked,
    prospect_info.status AS _lead_status,
    leadsource,
    CAST(_salesforceLeadStage AS STRING),
    ownername,
    ownerid,
    100,
    
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c, 
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    mql_source__c,
        convertedcontactid,
    convertedopportunityid,
    isconverted,
    converteddate,
    convertedaccountid,
    isdeleted,
    leadid,field,oldvalue,newvalue,
    segment__c,
    _email_segment, 
    masterrecordid,
    prospect_info.link,
    new_status,
    mql_source,
    link_m,
    total_lead_score__c,
    _mql_date,
    plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, 
  forecast_amount_of_assets__c, 
  participants__c,
  PROP_Total_Participants__c,
  StageName__c,
  WIN_E_mail_Date__c,	
  Converted_to_New_Plan__c,
  Converted_to_New_Plan__c_name,
  plan_id


  FROM (
  SELECT * FROM open_event
  UNION ALL 
  SELECT * FROM click_event
    UNION ALL 
  SELECT * FROM sent_event
    UNION ALL 
  SELECT * FROM HardBounced_event
    UNION ALL 
  SELECT * FROM unsubscribe_event
  /*UNION ALL
  SELECT * FROM lead_form_download*/
  ) engagements
JOIN airtable ON CAST(engagements._campaignID AS INT64) = airtable.id
LEFT JOIN prospect_info ON  engagements._prospectID = prospect_info.id
--WHERE CONCAT(_scd_sequence, _event_type) NOT IN (SELECT DISTINCT CONCAT(_sdc_sequence, _engagement) FROM `pcs.db_campaign_analysis`)
--WHERE engagements._prospectID ='00Q5x00001wO4NyEAK'
UNION ALL 
SELECT 
    engagements._scd_sequence,
    engagements._prospectID,
    engagements._campaignID,
    engagements._event_type,
    engagements._email,
    engagements._timestamp,
    LAST_DAY(CAST(_timestamp AS DATE), MONTH) AS _lastday_timestamp,
    engagements._name,
    engagements._companyname,
    engagements.territory__c,
    --engagements.categoryid,
    prospect_info._state,
    --engagements.segment,
    sentdate, 
    _utm_campaign, 
    senddate, 
    airtable.status, 
    CAST(emailid AS STRING), 
    subject, 
    fromname, 
    emailname, 
    --fromaddress, 
    createddate,
    engagements.url,
    firm_crd__c,
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    _code,
    _screenshot, 
   _whatwedo, 
    campaignName AS campaignName, 
    _livedate, 
    _utm_source, 
    _journeyname, 
    _utm_medium, 
    _code, 
    _landingpage,
    _landingpage,
    engagements.utm_source, 
    engagements.utm_content, 
    engagements.utm_medium, 
    engagements.content_downloaded,
    CASE WHEN convertedcontactid = "0035x00003Wv85RAAR" THEN "(651) 702-1513" ELSE phone END AS phone,
    _type,
    engagements._linked_clicked,
    prospect_info.status AS _lead_status,
    leadsource,
    CAST(_salesforceLeadStage AS STRING),
    ownername,
    ownerid,
    100,
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c, 
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    converteddate,
    convertedaccountid,
    isdeleted,
    leadid,field,oldvalue,newvalue,
    segment__c,
    _email_segment, 
    masterrecordid,
    prospect_info.link,
    new_status,
    mql_source,
    link_m,total_lead_score__c,_mql_date,
    plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, 
  forecast_amount_of_assets__c, 
  participants__c,
  PROP_Total_Participants__c,
  StageName__c,
  WIN_E_mail_Date__c,	
  Converted_to_New_Plan__c,
  Converted_to_New_Plan__c_name,
  plan_id


  FROM (
  SELECT * FROM lead_form_download
    UNION ALL
  SELECT * FROM lead_form_download2
  UNION ALL 
  SELECT * FROM lead_form_download3
  UNION ALL
  SELECT * FROM lead_form_downloaded4
  UNION ALL 
  SELECT * FROM lead_form_downloaded5
  UNION ALL 
  SELECT * FROM lead_form_download6
  UNION ALL 
  SELECT * FROM lead_form_download7
   UNION ALL 
  SELECT * FROM lead_form_download8
  UNION ALL 
  SELECT * FROM lead_form_download10
  UNION ALL 
  SELECT * FROM lead_form_nature_cold
  UNION ALL 
  SELECT * FROM lead_form_mod_hot
  UNION ALL 
  SELECT * FROM lead_form_download11
  UNION ALL
  SELECT * FROM lead_form_download12
  UNION ALL 
  SELECT * FROM lead_form_download13 
  UNION ALL 
  SELECT * FROM lead_form_download14
  UNION ALL 
  SELECT * FROM lead_form_schwab
  UNION ALL 
  SELECT * FROM lead_universal
  UNION ALL 
  SELECT * FROM click_download 
  ) engagements
LEFT JOIN airtable ON CAST(engagements._campaignID AS INT64) = airtable.id
LEFT JOIN prospect_info ON  engagements._prospectID = prospect_info.id
UNION ALL
SELECT 
    engagements._scd_sequence,
    engagements._prospectID,
    engagements._campaignID,
    engagements._event_type,
    engagements._email,
    engagements._timestamp,
    LAST_DAY(CAST(_timestamp AS DATE), MONTH) AS _lastday_timestamp,
    engagements._name,
    engagements._companyname,
    engagements.territory,
    --engagements.categoryid,
    prospect_info._state,
    --engagements.segment,
    sentdate, 
    _utm_campaign, 
    senddate, 
    airtable.status, 
    CAST(emailid AS STRING), 
    subject, 
    fromname, 
    emailname, 
    --fromaddress, 
    createddate,
    engagements.url,
    firm_crd__c,
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    _code,
    _screenshot, 
   _whatwedo, 
   campaignName AS campaignName, 
    _livedate, 
    _utm_source, 
    _journeyname, 
    _utm_medium, 
    _code, 
    _landingpage,
    _landingpage,
    engagements.utm_source, 
    engagements.utm_content, 
    engagements.utm_medium, 
    engagements.content_downloaded,
    phone_number,
    _type,
    engagements._linked_clicked,
    prospect_info.status AS _lead_status ,
    leadsource,
    CAST(_salesforceLeadStage AS STRING),
    ownername,ownerid,
    100,
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c, 
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    engagements.mql_source__c,
        convertedcontactid,
    convertedopportunityid,
    isconverted,
    converteddate,
    convertedaccountid,
    isdeleted,
    leadid,field,oldvalue,newvalue,
    segment__c,
    _email_segment, 
    masterrecordid,
    prospect_info.link,
    new_status,
    mql_source,
    link_m,
    total_lead_score__c,
    _mql_date,
    plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, 
  forecast_amount_of_assets__c, 
  participants__c,
  PROP_Total_Participants__c,
  StageName__c,
  WIN_E_mail_Date__c,	
  Converted_to_New_Plan__c,
  Converted_to_New_Plan__c_name,
  plan_id
  FROM (
  SELECT * FROM lead_score
  UNION ALL
  SELECT * FROM website
  ) engagements
LEFT JOIN airtable ON CAST(engagements._campaignID AS INT64) = airtable.id
LEFT JOIN prospect_info ON prospect_info.id = engagements._prospectID
), _dropped AS (

  SELECT 
        _campaignID, 
        _email,_prospectID,"True" AS _dropped
    FROM (
        SELECT 
            _campaignID, 
            _email,_prospectID,
            SUM(CASE WHEN _event_type = 'Opened' THEN 1 END) AS _hasOpened,
            SUM(CASE WHEN _event_type = 'Clicked' THEN 1 END) AS _hasClicked,
            SUM(CASE WHEN _event_type IN( 'Soft bounce','Hard bounce','Block bounce') THEN 1 END) AS _hasBounced,
        FROM 
            combine_all
        WHERE
            _event_type IN ('Opened', 'Clicked', 'Soft bounce','Hard bounce','Block bounce')
        GROUP BY
            1, 2,3
    )
    WHERE 
    (_hasClicked IS NOT NULL
    AND _hasBounced IS NOT NULL) OR (_hasOpened IS NOT NULL
    AND _hasBounced IS NOT NULL)

),_isBot AS ( 
    SELECT _campaignID, _email, _prospectID,'True' AS _isBot
    FROM (
    SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linked_clicked LIKE "%DG-EM%" THEN 1
    WHEN _linked_clicked = "Content_downloaded" THEN 1  END) AS _content_donwloaded,
    SUM(CASE WHEN _linked_clicked = "Bot"  THEN 1 END) AS _bot
    FROM combine_all
    WHERE _event_type = 'Clicked' 
    GROUP BY 1,2,3
    ) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
)
--, main_data AS (
 SELECT origin.*,
CASE WHEN _event_type = 'Opened' THEN "Yes"
WHEN _event_type = 'Clicked' THEN "Yes"
WHEN _event_type = 'Downloaded' THEN "Yes"
WHEN _event_type = 'Clicked Downloaded' THEN "Yes"
END AS _priority,_dropped,_isBot
FROM combine_all origin 
LEFT JOIN _dropped scenario ON  (origin._email = scenario._email
AND origin._campaignID = scenario._campaignID
AND origin._prospectID = scenario._prospectID
AND origin._event_type IN('Soft bounce','Hard bounce','Block bounce'))
LEFT JOIN _isBot scenarios ON (origin._email = scenarios._email 
AND origin._campaignID = scenarios._campaignID 
AND origin._prospectID = scenarios._prospectID
AND origin._event_type = "Clicked" AND _linked_clicked LIKE  "%DG-EM%");


INSERT INTO `x-marketing.pcs.db_campaign_analysis` (
_lead_status,
_timestamp,
_engagement,
_email
)
WITH all_dates AS(
  SELECT DISTINCT LAST_DAY(CAST(_timestamp AS DATE), MONTH) AS dates

  FROM `x-marketing.pcs.db_campaign_analysis` 
),
all_leadstage AS (
  SELECT DISTINCT _lead_status
  FROM `x-marketing.pcs.db_campaign_analysis` 
)   SELECT _lead_status,CAST(dates AS TIMESTAMP),'MQL',"@gopeanut.com"
 FROM all_leadstage
CROSS JOIN all_dates;

INSERT INTO `x-marketing.pcs.db_campaign_analysis` (
_lead_status,
_timestamp,
_engagement,
_email
)
SELECT 'Open',TIMESTAMP("2023-02-01 12:34:56+00"),'MQL',"@gopeanut.com"
UNION ALL
SELECT 'Nurture',TIMESTAMP("2023-02-01 12:34:56+00"),'MQL',"@gopeanut.com"
UNION ALL
SELECT 'Archived',TIMESTAMP("2023-02-01 12:34:56+00"),'MQL',"@gopeanut.com";

/*UPDATE `x-marketing.pcs.db_campaign_analysis` origin
SET origin._target_value = scenario.c,origin._target_per_month = scenario._target_par
FROM (
WITH count_prospect AS (
  SELECT *,
  COUNT(DISTINCT _prospectID) OVER(PARTITION BY EXTRACT(MONTH FROM _timestamp)) AS _target_par 
  FROM `x-marketing.pcs.db_campaign_analysis` 
WHERE
            _engagement IN ('Web','Downloaded','MQL Score'))
            SELECT *,_target/_target_par AS c
            FROM count_prospect
            ORDER BY _email DESC
)scenario
WHERE 
    origin._email = scenario._email
AND origin._campaignID = scenario._campaignID
AND origin._prospectID = scenario._prospectID
AND origin._timestamp = scenario._timestamp
AND origin._emailID = scenario._emailID
AND origin._sdc_sequence = scenario._sdc_sequence
AND origin._utm_campaign = scenario._utm_campaign
AND origin._leadsource = scenario._leadsource
AND origin._engagement IN ('Web','Downloaded','MQL Score');*/


CREATE OR REPLACE TABLE `x-marketing.pcs.Lead_Scoring` AS
WITH _engagement AS (
  SELECT *,CASE WHEN _engagement LIKE '%bounce%' THEN "Bounced" ELSE _engagement END AS _engagements FROM `x-marketing.pcs.db_campaign_analysis` WHERE
  -- _prospectID = '00Q5x00001wO7uzEAC' AND
    _engagement not in ("Sent")
), score AS(
--SELECT * FROM airtable /*
SELECT 
lead_id__c, 
email_name__c, 
--total_score__c, 
lead_name__c, 
k.id, 
k.createdbyid, 
k.lastmodifieddate, 
k.name, 
k.ownerid, 
k.createddate, 
l.firstname, 
l.lastname, 
l.name AS lead_name,
company,
--CONCAT(first_name, ' ', last_name) AS name,
email,territory__c,
k.lastvieweddate,
CAST(k.createddate AS DATE) AS date,
CASE WHEN open_score__c > 0 THEN "Opened"
WHEN cta_score__c > 0 THEN "Clicked"
WHEN form_submission_score__c > 0 THEN "Downloaded"
WHEN bounced_score__c > 0 THEN "Bounced"
WHEN unsubscribe_score__c > 0 THEN "Unsubscribed" END AS _engagement,
--form_lead.*,
total_score__c,
open_score__c,
cta_score__c,
bounced_score__c,
unsubscribe_score__c,
total_scores__c,
form_submission_score__c,total_lead_score__c,  
--SUM(total_scores__c) AS total_scores__c,
--SUM(open_score__c) AS open_score__c,
--SUM(cta_score__c) AS cta_score__c,
--SUM(bounced_score__c) AS bounced_score__c,
--SUM( form_submission_score__c) AS form_submission_score__c, 
--SUM(unsubscribe_score__c) AS unsubscribe_score__c
--Lead_scoring.categoryid
FROM `x-marketing.pcs_salesforce.Lead_Score__c` k
LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON k.lead_name__c = l.id
--LEFT JOIN airtable ON  k.email_name__c = form_lead._utm_campaign 
WHERE k.isdeleted IS FALSE
--lead_name__c = '00Q5x00001wO7uzEAC'
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) 
SELECT * EXCEPT( _rownum )
FROM(
SELECT score.*EXCEPT(_engagement),_engagement.*EXCEPT(_engagements) , ROW_NUMBER() OVER(PARTITION BY lead_id__c, 
email_name__c, 
--total_score__c, 
lead_name__c,createddate,lastvieweddate,createdbyid,email  ORDER BY lastmodifieddate DESC) AS _rownum FROM score
LEFT JOIN _engagement ON score.lead_name__c = _engagement._prospectID AND 
CAST(score.createddate AS DATE) = CAST(_timestamp AS DATE) AND 
score._engagement = _engagement._engagements
)
WHERE _rownum = 1;

CREATE OR REPLACE TABLE `x-marketing.pcs.Lead_Staging` AS
WITH prospect_info AS (
 WITH lead_history AS
(
  SELECT leadid,field,oldvalue,newvalue
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('leadConverted','leadMerged')
) ,status_change AS (
     SELECT  
    news.status AS new_status,
    old.status AS old_status,
    old.masterrecordid AS old_masterrecordid ,
    old.id AS old_id,
    news.id AS new_id,
    CASE WHEN news.status = old.status THEN TRUE ELSE FALSE END AS _status_different,
    old.email AS _old_email,news.email AS _new_email,
    news.mql_source__c AS mql_source
FROM `x-marketing.pcs_salesforce.Lead` old
JOIN `x-marketing.pcs_salesforce.Lead` News ON old.masterrecordid=news.id
)
, contact AS
(
  SELECT id AS _contactid,
  segment__c
  FROM `x-marketing.pcs_salesforce.Contact`
)
, leads AS
(
    SELECT
    routable.name AS names, 
    routable.firstname, 
    routable.lastname, 
    routable.id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    routable.title, 
    dd_bd_title_categories__c, 
    routable.phone, 
    CASE WHEN state._code IS NULL THEN IF(UPPER(routable.state) = 'INDIANA', 'IN', UPPER(routable.state)) ELSE UPPER(state._code) END AS _state, 
    routable.email,
    leadsource,
    CASE WHEN status = "Open" THEN 1 
          WHEN status = "Nurture" THEN 6
          WHEN status = "Sales Qualified" THEN 5 
          WHEN status = "Archived" THEN 7
          WHEN status = "Contacted" THEN 3
          WHEN status= "Engaged" THEN 4
          WHEN status = "Unqualified" THEN 8
          WHEN status = "New" THEN 2
          END AS _salesforceLeadStage,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',routable.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    routable.ownerid AS ownerids, 
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c,
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    CASE WHEN routable.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN routable.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
WHEN routable.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c  END AS mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    CAST(converteddate AS DATETIME) AS converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS link_m,
    

    /*
    dd_current_ria_firm_1_crd__c, 
    dd_current_ria_firm_2_crd__c, 
    dd_current_bd_firm_1_crd__c, 
    dd_primary_bd_firm_crd__c, 
    dd_primary_ria_firm_crd__c, 
    dd_primary_firm_crd__c, 
    dd_prior_firm_2_firm_crd__c, 
    dd_prior_firm_1_firm_crd__c, 
    dd_prior_firm_3_firm_crd__c,
    dd_branch_address_id__c,individual_crd__c,
    firm_crd__c,
    dd_home_address_id__c*/
    FROM `x-marketing.pcs_salesforce.Lead` routable
    LEFT JOIN `x-marketing.pcs.db_state_code_lookup` state on CAST(routable.state AS STRING) = CAST(state._state AS STRING) or routable.state = state._code
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = routable.ownerid
    --WHERE id = '00Q5x000021VdQjEAK'
)

SELECT leads.*,
lead_history.*,
segment__c,new_status,mql_source
FROM leads
LEFT JOIN contact ON leads.convertedcontactid = contact._contactid
LEFT JOIN status_change ON leads.id = old_id
LEFT JOIN lead_history ON leads.id = leadid
    
)
SELECT k.* EXCEPT(_sdc_received_at,systemmodstamp, _sdc_batched_at, _sdc_extracted_at,_sdc_table_version,id,isdeleted) ,CASE WHEN action_type__c = "Open" THEN "Opened"
 WHEN action_type__c = "CTA" THEN "Clicked"
  WHEN action_type__c = "Bounce" THEN "Bounced"
   WHEN action_type__c = "Unsubscribe" THEN "Unsubscribe"
    WHEN action_type__c = "Submission" THEN "Downloaded" ELSE action_type__c END AS _engagement ,l.* 
    
FROM `x-marketing.pcs_salesforce.Lead_Score_Staging__c` k
LEFT JOIN prospect_info l ON k.lead_id__c = l.id;
--and lead_name__c = '00Q5x00001wO7uzEAC';
--LEFT JOIN `x-marketing.pcs_sfmc.data_extension_DB_of_Advisors_for_Demand_Gen` l ON k.id= l.id
/*SELECT 
lead_name__c, 
SUM(open_score__c) AS open_score, 
SUM(total_score__c) AS total_score, 
SUM(form_submission_score__c) AS form_submission_score, 
SUM(total_scores__c) AS total_socres,
SUM(cta_score__c) AS cta_score,
--company,
--CONCAT(first_name, ' ', last_name) AS name,
--email_address,
--Lead_scoring.categoryid
FROM `x-marketing.pcs_salesforce.Lead_Score__c` sLead
--LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Lead_Scoring_List` Lead_scoring ON Lead_scoring.lead_id = sLead.lead_name__c
--LEFT JOIN `x-marketing.pcs_sfmc.data_extension_DB_of_Advisors_for_Demand_Gen` l ON Lead_scoring.lead_id = l.id
 GROUP BY lead_name__c
 --, company,
CONCAT(first_name, ' ', last_name),
email_address,Lead_scoring.categoryid*/

CREATE OR REPLACE TABLE `x-marketing.pcs.Lead_Scoring_SFMC` AS
SELECT * EXCEPT(_rownum),SUM(_cta+_open+_download+_unsubscribe+_bounced) AS _total_score,CASE WHEN COUNT(submitted) > 1 THEN 'Multiple Touchpoint' ELSE 'Single Touchpoint' END AS _touchpoint,0 AS _total_score_contact
FROM (
SELECT
CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'DG-EM-01-LP' OR _linked_clicked = 'DG-EM-01-LP2-W2' OR _linked_clicked = 'DG-EM-01-LP-W2' THEN 5
ELSE 0 END AS _cta, 
CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'DG-EM-01-LP' OR _linked_clicked = 'DG-EM-01-LP2-W2' OR _linked_clicked = 'DG-EM-01-LP-W2' THEN 1
ELSE 0 END AS cta,
CASE WHEN _engagement = 'Opened' THEN 1 
ELSE 0 END AS _open, 
CASE WHEN _engagement = 'Opened' THEN 1 
ELSE 0 END AS open, 
CASE WHEN _engagement = 'Downloaded' THEN 100 
ELSE 0 END AS _download,
CASE WHEN _engagement = 'Downloaded' THEN 1
ELSE  0 END AS submitted,
CASE WHEN _engagement = 'Unsubscribe' THEN -100 ELSE 0 END AS _unsubscribe, 
CASE  WHEN _engagement = 'Hard bounce' THEN -100
WHEN _engagement = 'Soft bounce' THEN -100
WHEN _engagement = 'Block bounce' THEN -100  ELSE 0 END AS _bounced , k._prospectID AS lead_id, _categoryID, 
l.firstname, 
l.lastname, 
l.name AS lead_name,
company,
 _customobjectkey,
 email,
 territory__c,
 lastmodifieddate,
 _utm_campaign,createddate,campaignName,
CASE WHEN state = 'Alabama' THEN 'AL'
WHEN state = 'Alaska' THEN 'AK'
WHEN state = 'Arizona' THEN 'AZ'
WHEN state = 'Arkansas' THEN 'AR'
WHEN state = 'California' THEN 'CA'
WHEN state = 'Colorado' THEN 'CO'
WHEN state = 'Connecticut' THEN 'CT'
WHEN state = 'Delaware' THEN 'DE'
WHEN state = 'Florida' THEN 'FL'
WHEN state = 'Georgia' THEN 'GA'
WHEN state = 'Hawaii' THEN 'HI'
WHEN state = 'Idaho' THEN 'ID'
WHEN state = 'Illinois' THEN 'IL'
WHEN state = 'Indiana' THEN 'IN'
WHEN state = 'Iowa' THEN 'IA'
WHEN state = 'Kansas' THEN 'KS'
WHEN state = 'Kentucky' THEN 'KY'
WHEN state = 'Louisiana' THEN 'LA'
WHEN state = 'Maine' THEN 'ME'
WHEN state = 'Maryland' THEN 'MD'
WHEN state = 'Massachusetts' THEN 'MA'
WHEN state = 'Michigan' THEN 'MI'
WHEN state = 'Minnesota' THEN 'MN'
WHEN state = 'Mississippi' THEN 'MS'
WHEN state = 'Missouri' THEN 'MO'
WHEN state = 'Montana' THEN 'MT'
WHEN state = 'Nebraska' THEN 'NE'
WHEN state = 'Nevada' THEN 'NV'
WHEN state = 'New Hampshire' THEN 'NH'
WHEN state = 'New Jersey' THEN 'NJ'
WHEN state = 'New Mexico' THEN 'NM'
WHEN state = 'New York' THEN 'NY'
WHEN state = 'North Carolina' THEN 'NC'
WHEN state = 'North Dakota' THEN 'ND'
WHEN state = 'Ohio' THEN 'OH'
WHEN state = 'Oklahoma' THEN 'OK'
WHEN state = 'Oregon' THEN 'OR'
WHEN state = 'Pennsylvania' THEN 'PA'
WHEN state = 'Rhode Island' THEN 'RI'
WHEN state = 'South Carolina' THEN 'SC'
WHEN state = 'South Dakota' THEN 'SD'
WHEN state = 'Tennessee' THEN 'TN'
WHEN state = 'Texas' THEN 'TX'
WHEN state = 'Utah' THEN 'UT'
WHEN state = 'Vermont' THEN 'VT'
WHEN state = 'Virginia' THEN 'VA'
WHEN state = 'Washington' THEN 'WA'
WHEN state = 'West Virginia' THEN 'WV'
WHEN state = 'Wisconsin' THEN 'WI'
WHEN state = 'Wyoming' THEN 'WY'
ELSE Upper(state) END AS state,_landingPage,
    k.number_of_retirement_plans__c, 
    k.average_retirement_plan_size_aua__c, 
    k.retirement_aum__c, 
    k.of_plans_acquired_per_year__c,
    k.mql_source__c,
    k.convertedcontactid, k.convertedopportunityid, k.isconverted, k.converteddate, k.convertedaccountid, k.isdeleted,k.Salesforce_Link,new_lead_status, new_mql_source,salesforce_mastercontactid,_Salesforceownername,
   ROW_NUMBER() OVER(PARTITION BY _prospectID,_utm_campaign,_engagement,_linked_clicked ORDER BY _timestamp DESC) AS _rownum
  FROM `x-marketing.pcs.db_campaign_analysis`  k
 LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON k._prospectID = l.id
 LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Lead_Scoring_List` m  ON m.lead_id = k._prospectID
)
WHERE _rownum = 1 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39;

UPDATE `x-marketing.pcs.Lead_Scoring_SFMC`  origin
SET _touchpoint = k._touchpoint,_total_score_contact = l
FROM ( 
SELECT * FROM (
SELECT lead_id,firstname,lastname,lead_name,company,_customobjectkey,email,/*CASE WHEN COUNT(DISTINCT campaignName) > 1 THEN 'Multiple Touchpoint' ELSE */'Single Touchpoint' /*END*/ AS _touchpoint,MAX(lastmodifieddate),SUM(_total_score)l
 FROM `x-marketing.pcs.Lead_Scoring_SFMC`
GROUP BY 1,2,3,4,5,6,7 
) )k
WHERE origin.lead_id = k.lead_id;




CREATE OR REPLACE TABLE `x-marketing.pcs.MQL New Status_Backup` AS
WITH owner_change AS (
    SELECT * EXCEPT(_rownum)
  FROM (
  SELECT leadid,field,oldvalue,newvalue, 
  createddate AS _lead_status_change_date_owner,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date_owner,
    CASE WHEN field = 'Owner' THEN createddate END AS owner_change_Date,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY createddate DESC) AS _rownum
   

  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('Owner')
  
  )WHERE _rownum = 1
), task AS(
    SELECT 
    task_type__c, 
    tasksubtype, 
    subject,
    whoid,
     k.ownerid, k.createddate AS _task_created_date,
     j.name as Assign_to
    FROM `x-marketing.pcs_salesforce.Task` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    ---JOIN `x-marketing.pcs_salesforce.Lead` l ON k.whoid = l.id /* AND k.ownerid = l.createdbyid */
   WHERE k.ownerid <>  '00560000001R83WAAS' AND k.status <> 'Not Started'AND isdeleted IS FALSE AND k.createddate >= "2022-08-23" 
)
, _new AS (
 SELECT * EXCEPT (_rownum)
 FROM (
    SELECT 
    k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    l._total_score,
    dd_linkedin__c,
    k.id,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid,
   m._utm_campaign,
   m._timestamp,
   k.leadsource,
   m.campaignName,
   mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
         TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,k.createddate, DAY) AS _duration,
   CAST(NULL AS INT) AS running_total_date_diff_last_activity,
   CAST(NULL AS INT) AS running_total_duration,
 task_type__c, 
    tasksubtype, 
    subject,
    case_safe_id__c,_task_created_date,
    k.mql_source__c,Assign_to,
    k.convertedcontactid, k.convertedopportunityid, k.isconverted, k.converteddate, k.convertedaccountid, k.isdeleted,salesforce_mastercontactid,
    CURRENT_DATE('America/New_York') AS extract_date,
    ROW_NUMBER() OVER(PARTITION BY k.id,k.status,_engagement ORDER BY _timestamp DESC) AS _rownum
   FROM `x-marketing.pcs_salesforce.Lead` k
  LEFT JOIN `x-marketing.pcs.Lead_Scoring_SFMC` l ON l.lead_id = k.id
  LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
   LEFT JOIN task ON  k.id = task.whoid /* AND k.createdbyid = task.ownerid */
  JOIN (SELECT _prospectID,_timestamp,_utm_campaign,campaignName,_engagement FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ("Web",'Downloaded','MQL Score') AND _lead_status IN ( 'New')) m  ON k.id = m._prospectID
WHERE 
    (k.status = 'New' AND k.isdeleted IS NOT TRUE) 
    --AND mql_date__c IS NOT NULL
 )WHERE _rownum = 1
    /*OR 
((j.name = 'Sitecore User' OR j.name LIKE '%Dev User%') AND k.status = 'New' AND isdeleted IS NOT TRUE)*/

), contact AS
(
  SELECT id AS _contactid,
  segment__c,leadsource
  FROM `x-marketing.pcs_salesforce.Contact`
),contacted AS (
 SELECT 
    k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid,
   m._utm_campaign,
   m._timestamp,
    k.leadsource,
   m.campaignName,
    mql_date__c, 
    last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY) AS _duration,
   SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_date_diff_last_activity,
   SUM(TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_duration,
   task_type__c, 
    tasksubtype, 
    subject,
    case_safe_id__c,_task_created_date,
    CASE WHEN k.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN k.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission' ELSE mql_source__c END AS mql_source__c,Assign_to,
    k.convertedcontactid, k.convertedopportunityid, k.isconverted, k.converteddate, k.convertedaccountid, k.isdeleted,salesforce_mastercontactid,
    CURRENT_DATE('America/New_York') AS extract_date,
    --ROW_NUMBER() OVER(PARTITION BY k.id,k.status,subject ORDER BY _timestamp DESC) AS _rownum
    FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    LEFT JOIN task ON  k.id = task.whoid /* AND k.createdbyid = task.ownerid */
    JOIN (SELECT _prospectID,_timestamp,_utm_campaign,campaignName,_engagement,salesforce_mastercontactid FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ("Web",'Downloaded','MQL Score') AND _lead_status IN ( 'Contacted')) m  ON k.id = m._prospectID
    WHERE (
   /* (l._total_score > 75 AND k.status = 'New' AND isdeleted IS NOT TRUE)
    OR 
((j.name = 'Sitecore User' OR j.name LIKE '%Dev User%') AND k.status = 'New' AND isdeleted IS NOT TRUE)
        OR */
    ( k.status = 'Contacted'  AND k.isdeleted IS NOT TRUE) 
    --AND mql_date__c IS NOT NULL
) 
AND k.email not like '%2x.marketing%'
) 
, _combine_all AS (
SELECT * FROM _new 
UNION ALL 
SELECT * FROM contacted
) SELECT 
createddate,
status, 
name,
lastname, 
firstname, 
DATETIME(lastactivitydate,'America/New_York') AS lastactivitydate, 
DATETIME(lastmodifieddate,'America/New_York') AS lastmodifieddate,  
lastvieweddate, 
lastreferenceddate, 
lasttransferdate, 
territory__c,
 email,
  company, 
  state, 
  title,
   pardot_campaign__c,
_total_score,
dd_linkedin__c,
id, 
link, 
ownername, 
 ownerid,
_utm_campaign,
 _timestamp,
 _combine_all.leadsource,
 campaignName,
 mql_date__c, 
 --CASE  WHEN id = '00Q5x000021W0yvEAC'  THEN last_status_change_date__c 
 --WHEN  last_status_change_date__c > owner_change_Date THEN owner_change_Date
 --ELSE  
 --last_status_change_date__c END AS
  last_status_change_date__c,
 _date_diff,
 _date_diff_last_activiy,
 _duration,
 running_total_date_diff_last_activity, running_total_duration, task_type__c, tasksubtype, subject, case_safe_id__c, 
DATETIME(_task_created_date,'America/New_York') AS _task_created_date, 
CASE WHEN mql_source__c IS NULL THEN contact.leadsource ELSE mql_source__c END AS mql_source__c, Assign_to, convertedcontactid, convertedopportunityid, isconverted, converteddate, convertedaccountid, isdeleted, salesforce_mastercontactid,DATETIME(owner_change_Date,'America/New_York') AS owner_change_Date,extract_date 
 FROM _combine_all
 LEFT JOIN owner_change ON _combine_all.id = owner_change.leadid
 LEFT JOIN contact ON _combine_all.convertedcontactid = contact._contactid
 WHERE _combine_all.id  <> '00Q5x00001wM2nbEAC';


CREATE OR REPLACE TABLE `x-marketing.pcs.MQL New Status Report` AS
WITH _MQL AS (
    Select * FROM `x-marketing.pcs.MQL New Status_Backup`
)
,unique_years_involved AS (

    SELECT
        EXTRACT(YEAR FROM  last_status_change_date__c) AS year
    FROM _MQL

    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM _MQL

),

all_holiday_dates AS (
    
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL 

    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 

    UNION ALL

    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     

    UNION ALL

    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [9] Veterans Day (Nov 11)
    -- SELECT 
    --     "Veterans Day" AS holiday_name, 
    --     DATE(year, 11, 11) AS holiday_date 
    -- FROM unique_years_involved 

    -- UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL   

    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 

),

add_filler_info AS (

    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates

),

replacement_holiday_dates AS (

    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info

),

actual_holiday_dates AS (

    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates

),

cross_join_leads_with_holidays AS (

    SELECT 
        main.*,
        side.actual_holiday_date
    FROM _MQL AS main
    CROSS JOIN actual_holiday_dates AS side

),

count_total_days_between_date_range AS (

    SELECT
        *,
        DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), DAY)  AS total_days_last_status_change_date__c,
        DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), DAY)  AS total_days_lastactivitydate,
        DATE_DIFF(extract_date,  CAST(createddate AS DATE), DAY)  AS total_days_created_date,
        DATE_DIFF(extract_date,  CAST( _task_created_date AS DATE), DAY)  AS total_days_task_created_date,
        DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), DAY)  AS total_days_mql_date__c
    FROM cross_join_leads_with_holidays

)
,count_total_weekends_between_date_range AS (

    SELECT
        *, (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  last_status_change_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_last_status_change_date__c,
        (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  lastactivitydate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_lastactivitydate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(createddate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  createddate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_createddate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(_task_created_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _task_created_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_task_created_date,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  mql_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_mql_date__c
    FROM count_total_days_between_date_range

)
,count_total_holidays_between_date_range AS (

    SELECT
        * EXCEPT(actual_holiday_date, in_date_range_last_status_change_date__c,in_date_range_lastactivitydate,in_date_range_createddate,in_date_range_task_created_date,in_date_range_mql_date__c),
        COALESCE(SUM(in_date_range_last_status_change_date__c), 0) AS total_holidays__last_status_change_date__c,
        COALESCE(SUM(in_date_range_lastactivitydate), 0) AS total_holidays_lastactivitydate,
        COALESCE(SUM(in_date_range_createddate), 0) AS total_holidays_createddate,
        COALESCE(SUM(in_date_range_task_created_date), 0) AS total_holidays_task_created_date,
        COALESCE(SUM(in_date_range_mql_date__c), 0) AS total_holidays_mql_date__c,
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(last_status_change_date__c AS DATE) AND extract_date
                THEN 1
            END in_date_range_last_status_change_date__c,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(lastactivitydate AS DATE) AND extract_date
                THEN 1
            END in_date_range_lastactivitydate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(createddate AS DATE) AND extract_date
            THEN 1
            END in_date_range_createddate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(_task_created_date  AS DATE) AND extract_date
            THEN 1
            END in_date_range_task_created_date ,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(mql_date__c AS DATE) AND extract_date
            THEN 1
            END in_date_range_mql_date__c
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59

),

calculate_days_in_new_stage AS (

    SELECT
        *,
        (total_days_last_status_change_date__c - total_weekends_last_status_change_date__c - total_holidays__last_status_change_date__c) AS net_days_last_status_change_date__c,
        (total_days_lastactivitydate - total_weekends_lastactivitydate - total_holidays_lastactivitydate) AS net_days_stage_lastactivitydate,
        (total_days_created_date - total_weekends_createddate - total_holidays_createddate) AS net_days_new_stage_createddate,
        (total_days_task_created_date - total_weekends_task_created_date - total_holidays_task_created_date) AS net_days_new_stage_task_created_date,
        (total_days_mql_date__c - total_weekends_mql_date__c - total_holidays_mql_date__c) AS net_days_new_stage_mql_date__c
    FROM count_total_holidays_between_date_range

)

SELECT * FROM calculate_days_in_new_stage;


CREATE OR REPLACE TABLE `x-marketing.pcs.leadConverted` AS
WITH lead_history AS
(
  SELECT leadid,field,oldvalue,newvalue
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('leadConverted','leadMerged')
) 
, contact AS
(
  SELECT id AS _contactid,
  segment__c
  FROM `x-marketing.pcs_salesforce.Contact`
)
, leads AS
(
    SELECT
    routable.name, 
    routable.firstname, 
    routable.lastname, 
    routable.id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    routable.title, 
    dd_bd_title_categories__c, 
    routable.phone, 
    CASE WHEN state._code IS NULL THEN IF(UPPER(routable.state) = 'INDIANA', 'IN', UPPER(routable.state)) ELSE UPPER(state._code) END AS _state, 
    routable.email,
    leadsource,
    CASE WHEN status = "Open" THEN 1 
          WHEN status = "Nurture" THEN 6
          WHEN status = "Sales Qualified" THEN 5 
          WHEN status = "Archived" THEN 7
          WHEN status = "Contacted" THEN 3
          WHEN status= "Engaged" THEN 4
          WHEN status = "Unqualified" THEN 8
          WHEN status = "New" THEN 2
          END AS _salesforceLeadStage,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',routable.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    routable.ownerid AS ownerid,
    number_of_retirement_plans__c, 
    average_retirement_plan_size_aua__c, 
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    CASE WHEN routable.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN routable.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
WHEN routable.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c  END AS mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    CAST(converteddate AS DATETIME) AS converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid

    /*
    dd_current_ria_firm_1_crd__c, 
    dd_current_ria_firm_2_crd__c, 
    dd_current_bd_firm_1_crd__c, 
    dd_primary_bd_firm_crd__c, 
    dd_primary_ria_firm_crd__c, 
    dd_primary_firm_crd__c, 
    dd_prior_firm_2_firm_crd__c, 
    dd_prior_firm_1_firm_crd__c, 
    dd_prior_firm_3_firm_crd__c,
    dd_branch_address_id__c,individual_crd__c,
    firm_crd__c,
    dd_home_address_id__c*/
    FROM `x-marketing.pcs_salesforce.Lead` routable
    LEFT JOIN `x-marketing.pcs.db_state_code_lookup` state on CAST(routable.state AS STRING) = CAST(state._state AS STRING) or routable.state = state._code
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = routable.ownerid
)

SELECT leads.*,
lead_history.*,
segment__c
FROM leads
JOIN contact ON leads.convertedcontactid = contact._contactid
 JOIN lead_history ON leads.id = leadid;

CREATE OR REPLACE TABLE `x-marketing.pcs.db_campaign_summary` AS
WITH clicks AS (
  SELECT sendid,COUNT( DISTINCT subscriberkey) as clicks_DG,COUNT(id) AS count_dg
FROM(
  SELECT sendid,id, activity.subscriberkey, linkname, linkclick.url,activity.url, emailname, clickdate,
  ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,linkclick.url,id ORDER BY clickdate DESC) AS _rownum
FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks` linkclick
LEFT JOIN `x-marketing.pcs_sfmc.event` activity ON activity.subscriberkey = linkclick.subscriberkey
WHERE eventtype = 'Click' AND linkname = 'DG-EM-01-LP'
ORDER BY subscriberkey ASC)
where _rownum = 1
GROUP BY 1
),click_unique AS (
    SELECT _campaignID,_utm_campaign,SUM(CASE WHEN _engagement = 'Downloaded' THEN 1 END) AS _download,
  SUM(CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'Empty' THEN 1 END ) AS _unsubscribeClicked,
  SUM(CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'PCS' THEN 1 END) AS _linkClicked,
  SUM(CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'DG-EM-01-LP' THEN 1 END) AS _DG_linkedClicked,
  SUM(CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'Privacy Policy' THEN 1 END) AS _privacy,
  SUM(CASE WHEN _engagement = 'Clicked' AND _linked_clicked = 'View as a Web Page' THEN 1 END) AS _webpage,
  SUM(CASE WHEN _engagement = 'Block bounce' THEN 1 END) AS _blockbounced,
  SUM(CASE WHEN _engagement = 'Hard bounce' THEN 1 END) AS _hardbounced,
  SUM(CASE WHEN _engagement = 'Soft bounce' THEN 1 END) AS _softbounced,
  SUM(CASE WHEN _engagement = 'Soft bounce' THEN 1 
  WHEN _engagement = 'Hard bounce' THEN 1
  WHEN _engagement = 'Block bounce' THEN 1  END) AS _bounced,
   SUM(CASE WHEN _engagement = 'Clicked' THEN 1 END) AS _clickss,
  FROM  `x-marketing.pcs.db_campaign_analysis`
  GROUP BY 1,2
)
SELECT 
list.sendid, senddate,  sentdate, 
  name.value.name AS _utm_campaign,   status, 
  emailid, 
  TRIM(subject) AS subject, 
  fromname, 
  TRIM(emailname) AS emailname, 
  --fromaddress, 
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",airtable.createddate) AS createddate, 
  isalwayson , airtable.id,
  CASE WHEN airtable.id =   125883  THEN 'DG_EM1_W1_D1'
  WHEN airtable.id = 125885 THEN 'DG_EM1_W1_D2'
  WHEN airtable.id = 125887 THEN 'DG_EM1_W1_D3'
  WHEN airtable.id = 127013 THEN 'DG_EM1_W1_D4'
  WHEN airtable.id =128177 THEN 'DG_EM1_W1_D5'
  WHEN airtable.id = 128515 THEN 'DG_EM1_W1_D6'
  WHEN airtable.id = 129927 THEN 'DG_EM1_W2_D1'
  WHEN airtable.id = 130701 THEN 'DG_EM1_W2_D2' 
  WHEN airtable.id = 131367 THEN 'DG_EM1_W2_D3'
  WHEN airtable.id = 131369 THEN 'DG_EM1_W2_D4'
  WHEN airtable.id = 135657 THEN 'DG_EM2_W1_D1'
  WHEN airtable.id = 136530 THEN 'DG_EM2_W1_D2'
  WHEN airtable.id = 136532 THEN 'DG_EM2_W1_D3'
  WHEN airtable.id = 138604 THEN 'DG_EM2_W2_D1'
  WHEN airtable.id = 139767 THEN 'DG_EM2_W2_D2'
  WHEN airtable.id = 139769 THEN 'DG_EM2_W2_D3'
  WHEN airtable.id = 143847 THEN 'DG_EM3_W1_D1'
  WHEN airtable.id =143850 THEN 'DG_EM3_W1_D2'
  WHEN airtable.id = 146385 THEN 'DG_EM3_W2_D1'
  WHEN airtable.id = 146387 THEN 'DG_EM3_W2_D2' END AS  _type,numbersent,numberdelivered,
_clickss,uniqueclicks,clicks_DG, count_dg,_unsubscribeClicked,_linkClicked,_DG_linkedClicked,_privacy,_webpage,
hardbounces, 
 
missingaddresses, 
softbounces, existingunsubscribes, otherbounces, uniqueopens, _blockbounced,_hardbounced,_softbounced,_bounced,

listid,  invalidaddresses, existingundeliverables, forwardedemails,_download as _fromsubmission
   FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
LEFT JOIN `x-marketing.pcs_sfmc.list_send` list  ON list.sendid = airtable.id
LEFT JOIN clicks ON airtable.id = clicks.sendid 
LEFT JOIN click_unique ON airtable.id = CAST(click_unique._campaignID AS INT64)
WHERE numbersent > 80
ORDER BY senddate DESC;

CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_Disposition` AS
WITH leadstatus_o AS (
   with owner AS (
     -- SELECT * EXCEPT (_rownum)
    --FROM (
    --SELECT *
    --,
    --ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    --FROM (
        SELECT leadid,
        field,
        j.name as previous_status,
        l.name AS new_status,
        _lead_status_change_date,
        createbyfieldhistory
        FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    createdbyid AS createbyfieldhistory
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE  
  field IN( "Owner") AND 
  isdeleted IS FALSE 
         )k
          JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.previous_status
          JOIN `x-marketing.pcs_salesforce.User` l ON l.id = k.new_status /*AND newvalue = 'Contacted'*/ 
    --)WHERE  previous_status IN( 'New' ,'Open')
    --)WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'
)
,lead_field_change AS (
     -- SELECT * EXCEPT (_rownum)
    --FROM (
    --SELECT *
    --,
    --ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    --FROM (
    SELECT 
    leadid,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'
     WHEN field = 'created' THEN 'Created' ELSE field END AS field,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'
     WHEN field = 'created' THEN 'Created' ELSE oldvalue END AS previous_status,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'  
    WHEN field = 'created' THEN 'Created' ELSE newvalue END AS new_status,
    createddate AS _lead_status_change_date,
    createdbyid AS createbyfieldhistory
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE  
  field IN( 'Status','leadConverted','leadMerged','created') AND 
  isdeleted IS FALSE  /*AND newvalue = 'Contacted'*/ 
    --)WHERE  previous_status IN( 'New' ,'Open')
    --)WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'
), contact_status_change AS (
  WITH status_change AS (
     SELECT  
    old.status AS old_status,
    old.id AS leadid,
    old.email AS _old_email,
    convertedcontactid,
    createdbyid
FROM `x-marketing.pcs_salesforce.Lead` old
WHERE convertedcontactid is NOT NULL AND isdeleted IS FALSE 
  )
SELECT * EXCEPT (rownum)
FROM(
SELECT leadid, 
CASE WHEN field = "contactMerged" THEN "Contact Merged" ELSE field END AS field, CASE WHEN oldvalue IS NULL THEN " Contact Merged " ELSE oldvalue END AS oldvalue, CASE WHEN newvalue IS NULL THEN "Contact Merged " ELSE newvalue END AS newvalue, createddate,history.createdbyid,
ROW_NUMBER() OVER(PARTITION BY leadid,contactid ORDER BY createddate DESC) AS rownum

 FROM `x-marketing.pcs_salesforce.ContactHistory`  history
 JOIN status_change ON history.contactid = status_change.convertedcontactid
 WHERE isdeleted IS FALSE AND field IN ("contactMerged")
) WHERE rownum = 1 
) 
SELECT k.*,LAG(_lead_status_change_date) OVER(PARTITION BY leadid,k.createbyfieldhistory ORDER BY _lead_status_change_date) previous_lead_status_change_date,
CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername_field_history,

FROM (
SELECT * FROM lead_field_change
UNION ALL 
SELECT * FROM contact_status_change
UNION ALL 
SELECT * FROM owner
)k
LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.createbyfieldhistory
--WHERE field IN( 'Status',"Owner",'leadConverted','leadMerged')
)
,all_data AS (

SELECT m.*,
k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid, k.leadsource,mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY) AS _duration,
  SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_date_diff_last_activity,
SUM(TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_duration,
  -- task_type__c, 
  --  tasksubtype, 
  --  subject,
    case_safe_id__c,
    _lead_status_change_date,
    previous_lead_status_change_date,
    --DATE_DIFF(_lead_status_change_date, previous_lead_status_change_date, DAY) _date_diff_task,
    ownername_field_history,
    --ownername_field_history,
    leadstatus.field AS field_change,
    previous_status,
    new_status,
    DATETIME(_lead_status_change_date,'America/New_York'),
    leadstatus.field,previous_lead_status_change_date AS previous_lead_status_change_dates
    
FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    --LEFT JOIN task ON  k.id = task.whoid /* AND k.createdbyid = task.ownerid */
    LEFT JOIN (SELECT *  FROM leadstatus_o ) leadstatus ON k.id = leadstatus.leadid
     JOIN (SELECT * FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ('Web','Downloaded','MQL','MQL Score')) m  ON k.id = m._prospectID
) ,leadstatus AS (
    SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE /*AND newvalue = 'Contacted'*/ 
    )WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'

),task AS(
    SELECT 
    task_type__c, 
    tasksubtype, 
    subject,
    whoid,
     k.ownerid, k.createddate AS _task_created_date,
     j.name as Assign_to,
     LAG(k.createddate) OVER(PARTITION BY whoid,k.ownerid ORDER BY k.createddate) previous_task_date
    FROM `x-marketing.pcs_salesforce.Task` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    ---JOIN `x-marketing.pcs_salesforce.Lead` l ON k.whoid = l.id /* AND k.ownerid = l.createdbyid */
   WHERE k.ownerid NOT IN (  '00560000001R83WAAS','005f2000008Y2yNAAS') AND status <>'Not Started' AND isdeleted IS FALSE AND k.createddate >= "2022-08-23" 
),contact_status AS (
    SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY _lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate ) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE AND newvalue = 'Contacted' 
    )
    --WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1
),engaged_status AS (
  SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY _lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate ) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE AND newvalue = 'Engaged' 
    )
    --WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1
),all_data_task AS (

SELECT m.*,
k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid, k.leadsource,mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY) AS _duration,
  SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_date_diff_last_activity,
  SUM(TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_duration,
  -- task_type__c, 
  --  tasksubtype, 
  --  subject,
    case_safe_id__c,
    _lead_status_change_date ,
    previous_task_date,
    --DATE_DIFF(_task_created_date, previous_task_date, DAY) _date_diff_task,
    Assign_to,
    --ownername_field_history,
    task_type__c AS task_type__c,
    '' AS previous_status,
    subject,
    DATETIME(_task_created_date,'America/New_York') AS _task_created_date,
    tasksubtype,previous_lead_status_change_date AS previous_lead_status_change_dates,
   
    FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    LEFT JOIN task ON  k.id = task.whoid OR k.convertedcontactid = task.whoid /* AND k.createdbyid = task.ownerid */
    LEFT JOIN (SELECT *  FROM leadstatus ) leadstatus ON k.id = leadstatus.leadid
     JOIN (SELECT * FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ('Web','Downloaded','MQL','MQL Score')) m  ON k.id = m._prospectID
) 
SELECT k.*, contact_status.new_status AS contact_status,engaged_status.new_status AS engaged_status,
  COALESCE(TIMESTAMP_DIFF( _task_created_date,previous_lead_status_change_date_net, DAY),0)  AS _date_diff_task
    FROM (
        SELECT * ,  
          SUM(TIMESTAMP_DIFF(_task_created_date,previous_lead_status_change_date_net, DAY)) OVER (PARTITION BY _prospectID ORDER BY _task_created_date) AS running_total_duration_activity,
          DATE_DIFF(_task_created_date,previous_lead_status_change_date_net,DAY) AS new_date_diff
        FROM(
            SELECT *, 
              CASE WHEN previous_task_date IS NULL THEN mql_date__c ELSE previous_task_date END AS previous_lead_status_change_date ,
              DATE_DIFF(_lead_status_change_date,CASE WHEN previous_lead_status_change_dates IS NULL THEN mql_date__c ELSE previous_lead_status_change_dates END, DAY) AS _date_diff_lead_status_change,
              CURRENT_DATE('America/New_York') AS extract_date ,
              LAG(_task_created_date) OVER(PARTITION BY _prospectID ORDER BY _task_created_date ASC) previous_lead_status_change_date_net, 
              FROM (
                      SELECT *,'Open+New' AS news FROM all_data_task 
                      UNION ALL 
                      SELECT *,'All Status' AS news FROM all_data
                  )
--WHERE _prospectID = '00Q6000001CcxyrEAB'
          ORDER BY _prospectID DESC
            )


            ) k
            LEFT JOIN (SELECT *  FROM contact_status ) contact_status ON k._prospectID = contact_status.leadid
            LEFT JOIN (SELECT *  FROM engaged_status ) engaged_status ON k._prospectID = engaged_status.leadid
--WHERE tasksubtype IS NOT NULL
;




CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_Disposition_date` AS
WITH _MQL AS (
    Select * FROM `x-marketing.pcs.MQLs_Disposition`
)
,unique_years_involved AS (

    SELECT
        EXTRACT(YEAR FROM  lastactivitydate) AS year
    FROM _MQL

    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM _MQL
    
    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM _task_created_date) AS year
    FROM _MQL

  UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM previous_task_date) AS year
    FROM _MQL
      UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM previous_lead_status_change_date) AS year
    FROM _MQL
      UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM _lead_status_change_date) AS year
    FROM _MQL
    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM previous_lead_status_change_date_net) AS year
    FROM _MQL

),

all_holiday_dates AS (
    
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL 

    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 

    UNION ALL

    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     

    UNION ALL

    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [9] Veterans Day (Nov 11)
    -- SELECT 
    --     "Veterans Day" AS holiday_name, 
    --     DATE(year, 11, 11) AS holiday_date 
    -- FROM unique_years_involved 

    -- UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL   

    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 

),

add_filler_info AS (

    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates

),

replacement_holiday_dates AS (

    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info

),

actual_holiday_dates AS (

    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates

),

cross_join_leads_with_holidays AS (

    SELECT 
        main.*,
        side.actual_holiday_date
    FROM _MQL AS main
    CROSS JOIN actual_holiday_dates AS side

),

count_total_days_between_date_range AS (

    SELECT
        *,
        DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), DAY)  AS total_days_last_status_change_date__c,
        DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), DAY)  AS total_days_lastactivitydate,
        DATE_DIFF(extract_date,  CAST(createddate AS DATE), DAY)  AS total_days_created_date,
        DATE_DIFF(extract_date,  CAST( _task_created_date AS DATE), DAY)  AS total_days_task_created_date,
        DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), DAY)  AS total_days_mql_date__c,
        DATE_DIFF(extract_date,  CAST( previous_task_date AS DATE), DAY)  AS total_days_previous_task_date,
        DATE_DIFF(extract_date,  CAST( previous_lead_status_change_date AS DATE), DAY)  AS total_days_previous_lead_status_change_date,
        DATE_DIFF(extract_date,  CAST( _lead_status_change_date AS DATE), DAY)  AS total_days_lead_status_change_date,
        DATE_DIFF(extract_date,  CAST( previous_lead_status_change_date_net AS DATE), DAY)  AS total_days_previous_lead_status_change_date_net,
        --DATE_DIFF(CAST(_lead_status_change_date AS DATE),  CAST( previous_lead_status_change_date AS DATE), DAY)  AS total_days_previous_lead_status_change_date,
    FROM cross_join_leads_with_holidays

)
--,count_total_weekends_between_date_range AS (

    SELECT
        *, (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  last_status_change_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_last_status_change_date__c,
        (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  lastactivitydate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_lastactivitydate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(createddate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  createddate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_createddate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(_task_created_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _task_created_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_task_created_date,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  mql_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_mql_date__c,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(previous_task_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  previous_task_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_previous_task_date
        ,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(previous_lead_status_change_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  previous_lead_status_change_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_previous_lead_status_change_date
        ,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(_lead_status_change_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _lead_status_change_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends__lead_status_change_date
        , 
          (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(previous_lead_status_change_date_net AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM previous_lead_status_change_date_net) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_previous_lead_status_change_date_net
    FROM count_total_days_between_date_range;

CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_Disposition_net_new` AS
WITH 
count_total_weekends_between_date_range AS (

    SELECT
        *
    FROM `x-marketing.pcs.MQLs_Disposition_date`

)
,count_total_holidays_between_date_range AS (

    SELECT
        * EXCEPT(actual_holiday_date, in_date_range_last_status_change_date__c,in_date_range_lastactivitydate,in_date_range_createddate,in_date_range_task_created_date,in_date_range_mql_date__c,in_date_range_previous_task_date,in_date_range_previous_lead_status_change_date,in_date_range_lead_status_change_date,in_date_range_previous_lead_status_change_date_net),
        COALESCE(SUM(in_date_range_last_status_change_date__c), 0) AS total_holidays__last_status_change_date__c,
        COALESCE(SUM(in_date_range_lastactivitydate), 0) AS total_holidays_lastactivitydate,
        COALESCE(SUM(in_date_range_createddate), 0) AS total_holidays_createddate,
        COALESCE(SUM(in_date_range_task_created_date), 0) AS total_holidays_task_created_date,
        COALESCE(SUM(in_date_range_mql_date__c), 0) AS total_holidays_mql_date__c,
        COALESCE(SUM(in_date_range_previous_task_date), 0) AS total_holidays_previous_task_date,
        COALESCE(SUM(in_date_range_previous_lead_status_change_date), 0) AS total_holidays_previous_lead_status_change_date,
        COALESCE(SUM(in_date_range_lead_status_change_date), 0) AS total_holidays_lead_status_change_date,
         COALESCE(SUM(in_date_range_previous_lead_status_change_date_net), 0) AS total_holidays_previous_lead_status_change_date_net,
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(last_status_change_date__c AS DATE) AND extract_date
                THEN 1
            END in_date_range_last_status_change_date__c,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(lastactivitydate AS DATE) AND extract_date
                THEN 1
            END in_date_range_lastactivitydate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(createddate AS DATE) AND extract_date
            THEN 1
            END in_date_range_createddate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(_task_created_date  AS DATE) AND extract_date
            THEN 1
            END in_date_range_task_created_date ,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(mql_date__c AS DATE) AND extract_date
            THEN 1
            END in_date_range_mql_date__c,
              CASE
            WHEN actual_holiday_date BETWEEN  CAST(previous_task_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_previous_task_date,
             CASE
            WHEN actual_holiday_date BETWEEN  CAST(previous_lead_status_change_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_previous_lead_status_change_date,
             CASE
            WHEN actual_holiday_date BETWEEN  CAST(_lead_status_change_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_lead_status_change_date,
             CASE
            WHEN actual_holiday_date BETWEEN  CAST(previous_lead_status_change_date_net AS DATE) AND extract_date
            THEN 1
            END in_date_range_previous_lead_status_change_date_net
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197

),

calculate_days_in_new_stage AS (

    SELECT
        *,
        (total_days_last_status_change_date__c - total_weekends_last_status_change_date__c - total_holidays__last_status_change_date__c) AS net_days_last_status_change_date__c,
        (total_days_lastactivitydate - total_weekends_lastactivitydate - total_holidays_lastactivitydate) AS net_days_stage_lastactivitydate,
        (total_days_created_date - total_weekends_createddate - total_holidays_createddate) AS net_days_new_stage_createddate,
        (total_days_task_created_date - total_weekends_task_created_date - total_holidays_task_created_date) AS net_days_new_stage_task_created_date,
        (total_days_mql_date__c - total_weekends_mql_date__c - total_holidays_mql_date__c) AS net_days_new_stage_mql_date__c,
        (total_days_previous_task_date - total_weekends_previous_task_date - total_holidays_previous_task_date) AS net_days_new_stage_previous_task_date,
        (total_days_previous_lead_status_change_date - total_weekends_previous_lead_status_change_date - total_holidays_previous_lead_status_change_date) AS net_days_new_stage_previous_lead_status_change_date,
         (total_days_lead_status_change_date - total_weekends__lead_status_change_date - total_holidays_lead_status_change_date) AS net_days_new_stage__lead_status_change_date,
         (total_days_previous_lead_status_change_date_net - total_weekends_previous_lead_status_change_date_net - total_holidays_previous_lead_status_change_date_net) AS net_days_new_stage_previous_lead_status_change_date_net,

    FROM count_total_holidays_between_date_range

)

SELECT * EXCEPT (_rownum)
FROM(
SELECT *, net_days_new_stage_previous_task_date - net_days_new_stage_task_created_date AS _date_different_task_net,net_days_new_stage_previous_lead_status_change_date-net_days_new_stage__lead_status_change_date AS net_new_data_status, 

COALESCE(net_days_new_stage_previous_lead_status_change_date_net - net_days_new_stage_task_created_date,0) AS _net_day_task_created_date,
ROW_NUMBER() OVER(PARTITION BY _email,_prospectID,_timestamp,_lead_status,_task_created_date,last_status_change_date__c,mql_date__c,_engagement,ownerid,subject,tasksubtype,task_type__c,case_safe_id__c,status,createddate,lastmodifieddate,Assign_to,
leadsource,
mql_date__c,
last_status_change_date__c ORDER BY _task_created_date DESC) AS _rownum  FROM calculate_days_in_new_stage
) 
WHERE 
_rownum = 1

;


CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_activity_history` AS
WITH owner_change AS (
    SELECT * EXCEPT(_rownum)
  FROM (
  SELECT leadid,field,oldvalue,newvalue, 
  createddate AS _lead_status_change_date_owner,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date_owner,
    CASE WHEN field = 'Owner' THEN createddate END AS owner_change_Date,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY createddate DESC) AS _rownum
   

  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('Owner')
  
  )WHERE _rownum = 1
), lead_contact AS (
     SELECT * EXCEPT (_rownum)
    FROM (
    SELECT 
     leadid,
    field,
    previous_status,
    new_status,
    _lead_status_change_date,
    --CASE WHEN _new_status_change_dates IS NULL THEN _lead_status_change_date ELSE _new_status_change_dates END AS 
    _new_status_change_dates,
    previous_lead_status_change_date,_Contacted_status_change_dates,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
        CASE WHEN field = 'Status' AND oldvalue = 'New' THEN createddate END AS _new_status_change_dates,
        CASE WHEN field = 'Status' AND newvalue = 'Contacted' THEN createddate END AS _Contacted_status_change_dates,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE 
  --AND leadid = '00Q5x000021WwViEAK' 
  ORDER BY  createddate  DESC /*AND newvalue = 'Contacted'*/ 
    )WHERE  previous_status IN( 'New' ) AND new_status = 'Contacted'
    )WHERE _rownum = 1 
    --AND leadid = '00Q5x000021XD4kEAG'
) ,leadstatus AS (
    SELECT * EXCEPT (_rownum)
    FROM (
    SELECT 
     leadid,
    field,
    previous_status,
    new_status,
    _lead_status_change_date,
    CASE WHEN _new_status_change_dates IS NULL THEN previous_lead_status_change_date ELSE _new_status_change_dates END AS 
    _new_status_change_dates,
    previous_lead_status_change_date,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
        CASE WHEN field = 'Status' AND newvalue = 'New' THEN createddate END AS _new_status_change_dates,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE 
  --AND leadid =  '00Q5x00001wOO1UEAW'
  ORDER BY  createddate  DESC /*AND newvalue = 'Contacted'*/ 
    )WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VBK6EAO'

),task AS(
    SELECT 
    task_type__c, 
    tasksubtype, 
    subject,
    whoid,
     k.ownerid, k.createddate AS _task_created_date,
     j.name as Assign_to,
     LAG(k.createddate) OVER(PARTITION BY whoid,k.ownerid ORDER BY k.createddate) previous_task_date
    FROM `x-marketing.pcs_salesforce.Task` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    ---JOIN `x-marketing.pcs_salesforce.Lead` l ON k.whoid = l.id /* AND k.ownerid = l.createdbyid */
   WHERE k.ownerid NOT IN (  '00560000001R83WAAS','005f2000008Y2yNAAS') AND status <>'Not Started' AND isdeleted IS FALSE AND k.createddate >= "2022-08-23" 
) ,status AS (
  SELECT leadid,
  max(_leadConverted_dates) AS _leadConverted_dates, 
  max(_new_status_change_date) AS _new_status_change_date,
  MAX(_Contacted_status_change_date) AS _Contacted_status_change_date,
  MAX(_Open_status_change_date) AS _Open_status_change_date,
  MAX(_Engaged_status_change_date) AS _Engaged_status_change_date,
  MAX(_Sales_Qualified_status_change_date) AS _Sales_Qualified_status_change_date,
  MAX(_Nurture_status_change_date) AS _Nurture_status_change_date,
  MAX(_Archived_status_change_date) AS _Archived_status_change_date,
  MAX(_Unqualified_status_change_date) AS _Unqualified_status_change_date,

  FROM (
  SELECT DISTINCT leadid,
    CASE WHEN field = 'leadConverted'  THEN createddate END AS _leadConverted_dates,
    CASE WHEN field = 'Status' AND newvalue = 'New' THEN createddate END AS _new_status_change_date,
    CASE WHEN field = 'Status' AND newvalue = 'Contacted' THEN createddate END AS _Contacted_status_change_date,
    CASE WHEN field = 'Status' AND newvalue = 'Open' THEN createddate END AS _Open_status_change_date,
    CASE WHEN field = 'Status' AND newvalue = 'Engaged' THEN createddate END AS _Engaged_status_change_date,
    CASE WHEN field = 'Status' AND newvalue = 'Sales Qualified' THEN createddate END AS _Sales_Qualified_status_change_date,
     CASE WHEN field = 'Status' AND newvalue = 'Nurture' THEN createddate END AS _Nurture_status_change_date,
     CASE WHEN field = 'Status' AND newvalue = 'Archived' THEN createddate END AS _Archived_status_change_date,
     CASE WHEN field = 'Status' AND newvalue = 'Unqualified' THEN createddate END AS _Unqualified_status_change_date,
    ROW_NUMBER() OVER(PARTITION BY leadid
 ORDER BY createddate DESC) AS _rownum
   

  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('leadConverted','Status') 
  
  
  )
  
  GROUP BY 1
),all_data AS (

SELECT m.*,
k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid, k.leadsource,
    CASE 
     WHEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR) IS NULL THEN k.createddate ELSE  DATE_ADD(mql_date__c, INTERVAL 4 HOUR) END AS mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY) AS _duration,
   SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_date_diff_last_activity,
   SUM(TIMESTAMP_DIFF(k.lastactivitydate,_task_created_date, DAY)) OVER (PARTITION BY k.id ORDER BY _task_created_date) AS running_total_duration,
   task_type__c, 
    tasksubtype, 
    subject,
    case_safe_id__c,
    _task_created_date,
    previous_task_date,
    DATE_DIFF(_task_created_date, previous_task_date, DAY) _date_diff_task,
    Assign_to,
    previous_status,new_status,
    CASE  WHEN k.id =  '00Q5x00001wO9I7EAK' THEN _leadConverted_dates
       WHEN new_status IS NULL AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates 
     WHEN new_status = 'Contacted' AND k.id = "00Q5x000021XD4kEAG" THEN _Contacted_status_change_date 
     WHEN new_status = 'Contacted' AND k.id = '00Q5x00001wNtBbEAK' THEN _lead_status_change_date
     WHEN new_status = 'Contacted' THEN _Contacted_status_change_date 
     WHEN new_status = 'New' AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates
     WHEN new_status = 'Sales Qualified' AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates 
     WHEN _lead_status_change_date IS NULL THEN k.createddate
     ELSE _lead_status_change_date END AS _lead_status_change_date,
    previous_lead_status_change_date AS previous_lead_status_change_dates,
--CASE   

--WHEN  k.territory__c IS NULL AND  owner_change_Date IS NOT NULL  AND previous_lead_status_change_date > owner_change_Date THEN owner_change_Date
--WHEN previous_lead_status_change_date IS NULL OR DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_date THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
--WHEN  previous_lead_status_change_date > owner_change_Date THEN _lead_status_change_date
--WHEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_date THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)

--ELSE
 --previous_lead_status_change_date 
 --END 
 _new_status_change_date AS previous_lead_status_change_date ,

     DATE_DIFF(CASE   WHEN k.id =  '00Q5x00001wO9I7EAK' THEN _leadConverted_dates
      WHEN new_status IS NULL AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates 
     WHEN new_status = 'Contacted' AND k.id = "00Q5x000021XD4kEAG" THEN _Contacted_status_change_date 
     WHEN new_status = 'Contacted' AND k.id = '00Q5x00001wNtBbEAK' THEN _lead_status_change_date
     WHEN new_status = 'Contacted' THEN _Contacted_status_change_date 
     WHEN new_status = 'New' AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates
    WHEN new_status = 'Sales Qualified' AND _leadConverted_dates  IS NOT NULL  THEN _leadConverted_dates 
    WHEN _lead_status_change_date IS NULL THEN k.createddate
    ELSE _lead_status_change_date END, 
CASE  
--WHEN  k.territory__c IS NULL AND  owner_change_Date IS NOT NULL  AND previous_lead_status_change_date > owner_change_Date THEN owner_change_Date
WHEN previous_lead_status_change_date IS NULL OR DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_date THEN DATE_ADD(mql_date__c, INTERVAL 5 HOUR)
--WHEN  previous_lead_status_change_date > owner_change_Date THEN _lead_status_change_date
WHEN previous_lead_status_change_date IS NULL THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
ELSE previous_lead_status_change_date END , DAY) _date_diff_lead_status_change,
     owner_change_Date AS owner_change_Date,
    CURRENT_DATE('America/New_York') AS extract_date,
    k.converteddate AS converteddates,
    _Contacted_status_change_date,
    _new_status_change_date,
    _leadConverted_dates, 
    _Open_status_change_date,
    _Engaged_status_change_date,
    _Sales_Qualified_status_change_date,
    _Nurture_status_change_date,
    _Archived_status_change_date,
    _Unqualified_status_change_date
    FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    LEFT JOIN task ON  k.id = task.whoid OR k.convertedcontactid = task.whoid/* AND k.createdbyid = task.ownerid */
    LEFT JOIN (SELECT *  FROM leadstatus ) leadstatus ON k.id = leadstatus.leadid
    LEFT JOIN (SELECT *  FROM owner_change ) owner_change ON k.id = owner_change.leadid
    LEFT JOIN (SELECT *  FROM status ) status ON k.id = status.leadid
    JOIN (SELECT * FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ('Web','Downloaded','MQL','MQL Score')) m  ON k.id = m._prospectID
)
SELECT * EXCEPT (rownum)
FROM(
SELECT *, 
    ROW_NUMBER() OVER(PARTITION BY _prospectID,subject,tasksubtype,_task_created_date ORDER BY _task_created_date DESC) AS rownum
FROM ( 
SELECT _sdc_sequence,
_campaignID,	
_engagement,	
_email,	
_prospectID,	
_timestamp,
_description,	
_name,	
_phone,	
_title,	
_seniority,	
_segment,	
_persona,	
_tier,	
_company,	
_domain,	
_industry,	
_subIndustry,	
_country,	
_city,	
_revenue,	
_employees,	
_subject,	
_screenshot,	
_landingPage,	
_utm_source,	
_utm_campaign,	
_utm_medium,	
_contentID,	
_contentTitle,	
_storyBrandStage,	
_abstract,	
_salesforceLeadStage,	
_salesforceLastActivity,	
_salesforceCreated,	
_salesforceOpportunityStage,	
_salesforceOpportunityValue,	
_salesforceOpportunityName,	
_salesforceOpportunityCreated,	
_sfdcAccountID,	
_sfdcLeadID,	
_sfdcContactID,	
_sfdcOpportunityID,	
_meetingScheduledDate,	
_salesforceOpportunityCloseDate,	
_state,	
_function,	
_lb_email,	
_utm_content,	
_campaignSentDate,	
_subCampaign,	
_salesforceLeadCreated,	
_salesforceLeadSource,	
_showExport,	
_isBot,	
_campaignStartDate,	
_territory,	
_categoryID,
_campaignSendDate,	
_status,	
_emailID,	
_fromname,	
_campaignCreatedDate,	
firm_crd__c,	
individual_crd__c,	
data_link2__ddl_firmid__c,	
data_link2__ddl_repid__c,	
_whatwedo,	
campaignName,	
_livedate,	
_rootcampaign,	
_pardotid,	
_landingpage_airtable,	
_url_param,	
_campaign_live_date,	
_download_utm_source,	
_download_utm_content,	
_download_utm_medium,	
_download_content_downloaded,	
_type,	
_linked_clicked,	
_lead_status,	
_dropped,	
_leadsource,	
_Salesforceownername,	
_Salesforceownerid,	
_lastday_timestamp,	
_salesforce_lead_status,	
_last_timestamp_second,	
_last_timestamp_sec,	
_target,
_target_per_month,
_target_value,	
number_of_retirement_plans__c,	
average_retirement_plan_size_aua__c,	
retirement_aum__c,	
of_plans_acquired_per_year__c,	
mql_source__c,	
convertedcontactid,	
convertedopportunityid,	
isconverted,	
converteddate,	
convertedaccountid,	
isdeleted,	
leadid,	
field,	
oldvalue,	
newvalue,	
segment__c,	
_email_segment,	
masterrecordid,	
Salesforce_Link,	
new_lead_status,	
new_mql_source,	
salesforce_mastercontactid,	
status,	
createddate,	
name,	
lastname,	
firstname,	
lastactivitydate,	
lastmodifieddate,	
lastvieweddate,	
lastreferenceddate,	
lasttransferdate,	
territory__c,	
email,	
company,	
state,	
title,	
pardot_campaign__c,	
total_lead_score__c,	
dd_linkedin__c,	
id,	
link,	
ownername,	
ownerid,	
leadsource,	
CASE WHEN  id = '00Q5x00001wO4NyEAK'  THEN TIMESTAMP('2022-09-06 00:00:00 UTC') ELSE mql_date__c END AS mql_date__c,		
last_status_change_date__c,	
_date_diff,
_date_diff_last_activiy,
_duration,
running_total_date_diff_last_activity,
running_total_duration,
task_type__c,	
tasksubtype,	
subject,	
case_safe_id__c,	
DATETIME(_task_created_date,'America/New_York') AS _task_created_date,	
previous_task_date,	
_date_diff_task,
Assign_to,	
previous_status,	
new_status,	
DATETIME( 
    --CASE 
--WHEN DATE_ADD(converteddates, INTERVAL 5 HOUR) > _lead_status_change_date  THEN DATE_ADD(converteddates, INTERVAL 5 HOUR)
--WHEN _lead_status_change_date< DATE_ADD(mql_date__c, INTERVAL 4 HOUR) THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR) ELSE
CASE  
WHEN id = '00Q5x00001rJ6oLEAS'  THEN TIMESTAMP('2023-05-11 20:01:36 UTC')
WHEN id = '00Q5x00001wOOBpEAO'  THEN TIMESTAMP('2023-04-27 20:04:49 UTC')
WHEN id = '00Q5x00001wOJ3MEAW'  THEN TIMESTAMP('2023-04-27 20:07:25 UTC')
WHEN id = '00Q5x00001wOXbZEAW'  THEN TIMESTAMP('2023-05-18 14:35:35 UTC')
WHEN id = '00Q5x00001wOO1UEAW'  THEN TIMESTAMP('2023-07-27 15:22:37 UTC')
ELSE 
_lead_status_change_date 
END
,'America/New_York') AS _lead_status_change_date,	
DATETIME( CASE  
WHEN id = '00Q5x00001wOLkhEAG'  THEN DATE_ADD(mql_date__c, INTERVAL 5 HOUR) 
WHEN id = '00Q5x00001wNl2QEAS'  THEN DATE_ADD(mql_date__c, INTERVAL 5 HOUR) 
WHEN id = '00Q5x00001rJ6oLEAS'  THEN DATE_ADD(mql_date__c, INTERVAL 5 HOUR) 
WHEN id = '00Q5x00001zx3mDEAQ'  THEN mql_date__c  
WHEN id = '00Q5x00001wO4NyEAK'  THEN DATE_ADD(TIMESTAMP('2022-09-06 00:00:00 UTC'), INTERVAL 4 HOUR) 
WHEN id = '00Q5x00001wOQA9EAO'  THEN _new_status_change_date
WHEN id = '00Q5x00001zx91xEAA'  THEN owner_change_Date
WHEN id = '00Q5x00001zx8JwEAI'  THEN owner_change_Date
WHEN id = '00Q5x000021W7ztEAC'  THEN owner_change_Date
WHEN id = '00Q5x000021XPCDEA4'  THEN owner_change_Date
WHEN id = '00Q5x000021WAGcEAO'  THEN owner_change_Date
WHEN id = '00Q5x000021WwViEAK'  THEN _new_status_change_date
WHEN  territory__c IS NULL AND  owner_change_Date IS NOT NULL AND _lead_status_change_date > owner_change_Date  THEN owner_change_Date 
WHEN (previous_lead_status_change_date < owner_change_Date ) AND (owner_change_Date < _Contacted_status_change_date) 
THEN owner_change_Date
WHEN previous_status = 'New' AND owner_change_Date < _Contacted_status_change_date THEN _new_status_change_date

--WHEN (_new_status_change_date < owner_change_Date AND owner_change_Date < _Contacted_status_change_date) THEN owner_change_Date
--WHEN _new_status_change_date IS NULL    THEN owner_change_Date
WHEN previous_lead_status_change_date IS NULL THEN mql_date__c ELSE  previous_lead_status_change_date END,'America/New_York') AS previous_lead_status_change_date,	
CASE WHEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR) IS NULL THEN createddate
WHEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_dates THEN DATE_ADD(mql_date__c, INTERVAL 5 HOUR)
--WHEN  k.territory__c IS NULL AND  owner_change_Date IS NOT NULL  AND previous_lead_status_change_date > owner_change_Date THEN owner_change_Date
WHEN previous_lead_status_change_dates IS NULL OR DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_dates THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
WHEN  territory__c IS NULL AND  owner_change_Date IS NOT NULL AND _lead_status_change_date > owner_change_Date  THEN owner_change_Date 
WHEN  previous_lead_status_change_dates > owner_change_Date THEN _lead_status_change_date
WHEN previous_lead_status_change_dates IS NULL THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
ELSE previous_lead_status_change_dates END AS previous_lead_status_change_datess ,
previous_lead_status_change_dates,
DATE_DIFF(_lead_status_change_date, CASE WHEN previous_lead_status_change_date IS NULL OR DATE_ADD(mql_date__c, INTERVAL 4 HOUR) > previous_lead_status_change_date THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
WHEN  _lead_status_change_date > owner_change_Date THEN owner_change_Date
WHEN previous_lead_status_change_date IS NULL THEN DATE_ADD(mql_date__c, INTERVAL 4 HOUR)
ELSE previous_lead_status_change_date END, DAY) AS _date_diff_lead_status_change,
owner_change_Date AS owner_change_Date,
extract_date,
_Contacted_status_change_date,
_new_status_change_date,
 _leadConverted_dates, 
_Open_status_change_date,
_Engaged_status_change_date,
  _Sales_Qualified_status_change_date,
_Nurture_status_change_date,
_Archived_status_change_date,
_Unqualified_status_change_date
 FROM all_data
--WHERE _prospectID = '00Q5x000021ViBIEA0'
  ORDER BY _prospectID DESC)
) WHERE rownum = 1 
--and _prospectID = '00Q5x00001zx7ZiEAI'
;



CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_activity_history_date` AS
WITH _MQL AS (
    Select * FROM `x-marketing.pcs.MQLs_activity_history`
)
,unique_years_involved AS (

    SELECT
        EXTRACT(YEAR FROM  lastactivitydate) AS year
    FROM _MQL

    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM _MQL
    
    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM _task_created_date) AS year
    FROM _MQL

  UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM previous_task_date) AS year
    FROM _MQL
      UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM previous_lead_status_change_date) AS year
    FROM _MQL
      UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM _lead_status_change_date) AS year
    FROM _MQL

),

all_holiday_dates AS (
    
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL 

    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 

    UNION ALL

    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     

    UNION ALL

    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [9] Veterans Day (Nov 11)
    -- SELECT 
    --     "Veterans Day" AS holiday_name, 
    --     DATE(year, 11, 11) AS holiday_date 
    -- FROM unique_years_involved 

    -- UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL   

    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 

),

add_filler_info AS (

    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates

),

replacement_holiday_dates AS (

    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info

),

actual_holiday_dates AS (

    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates

),

cross_join_leads_with_holidays AS (

    SELECT 
        main.*,
        side.actual_holiday_date
    FROM _MQL AS main
    CROSS JOIN actual_holiday_dates AS side

),

count_total_days_between_date_range AS (

    SELECT
        *,
        DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), DAY)  AS total_days_last_status_change_date__c,
        DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), DAY)  AS total_days_lastactivitydate,
        DATE_DIFF(extract_date,  CAST(createddate AS DATE), DAY)  AS total_days_created_date,
        DATE_DIFF(extract_date,  CAST( _task_created_date AS DATE), DAY)  AS total_days_task_created_date,
        DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), DAY)  AS total_days_mql_date__c,
        DATE_DIFF(extract_date,  CAST( previous_task_date AS DATE), DAY)  AS total_days_previous_task_date,
        DATE_DIFF(extract_date,  CAST( previous_lead_status_change_date AS DATE), DAY)  AS total_days_previous_lead_status_change_date,
        DATE_DIFF(extract_date,  CAST( _lead_status_change_date AS DATE), DAY)  AS total_days_lead_status_change_date,
        --DATE_DIFF(CAST(_lead_status_change_date AS DATE),  CAST( previous_lead_status_change_date AS DATE), DAY)  AS total_days_previous_lead_status_change_date,
    FROM cross_join_leads_with_holidays

)
--,count_total_weekends_between_date_range AS (

    SELECT
        *, (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(last_status_change_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  last_status_change_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_last_status_change_date__c,
        (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(lastactivitydate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  lastactivitydate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_lastactivitydate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(createddate AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  createddate) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_createddate,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(_task_created_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _task_created_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_task_created_date,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(mql_date__c AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  mql_date__c) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_mql_date__c,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(previous_task_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  previous_task_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_previous_task_date
        ,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(previous_lead_status_change_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  previous_lead_status_change_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends_previous_lead_status_change_date
        ,
                (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  CAST(_lead_status_change_date AS DATE), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _lead_status_change_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends__lead_status_change_date
    FROM count_total_days_between_date_range;

CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_activity_history_net_new` AS
WITH 
count_total_weekends_between_date_range AS (

    SELECT
        *
    FROM `x-marketing.pcs.MQLs_activity_history_date`

)
,count_total_holidays_between_date_range AS (

    SELECT
        * EXCEPT(actual_holiday_date, in_date_range_last_status_change_date__c,in_date_range_lastactivitydate,in_date_range_createddate,in_date_range_task_created_date,in_date_range_mql_date__c,in_date_range_previous_task_date,in_date_range_previous_lead_status_change_date,in_date_range_lead_status_change_date),
        COALESCE(SUM(in_date_range_last_status_change_date__c), 0) AS total_holidays__last_status_change_date__c,
        COALESCE(SUM(in_date_range_lastactivitydate), 0) AS total_holidays_lastactivitydate,
        COALESCE(SUM(in_date_range_createddate), 0) AS total_holidays_createddate,
        COALESCE(SUM(in_date_range_task_created_date), 0) AS total_holidays_task_created_date,
        COALESCE(SUM(in_date_range_mql_date__c), 0) AS total_holidays_mql_date__c,
        COALESCE(SUM(in_date_range_previous_task_date), 0) AS total_holidays_previous_task_date,
        COALESCE(SUM(in_date_range_previous_lead_status_change_date), 0) AS total_holidays_previous_lead_status_change_date,
        COALESCE(SUM(in_date_range_lead_status_change_date), 0) AS total_holidays_lead_status_change_date,
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(last_status_change_date__c AS DATE) AND extract_date
                THEN 1
            END in_date_range_last_status_change_date__c,
            CASE
                WHEN actual_holiday_date BETWEEN  CAST(lastactivitydate AS DATE) AND extract_date
                THEN 1
            END in_date_range_lastactivitydate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(createddate AS DATE) AND extract_date
            THEN 1
            END in_date_range_createddate,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(_task_created_date  AS DATE) AND extract_date
            THEN 1
            END in_date_range_task_created_date ,
            CASE
            WHEN actual_holiday_date BETWEEN  CAST(mql_date__c AS DATE) AND extract_date
            THEN 1
            END in_date_range_mql_date__c,
              CASE
            WHEN actual_holiday_date BETWEEN  CAST(previous_task_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_previous_task_date,
             CASE
            WHEN actual_holiday_date BETWEEN  CAST(previous_lead_status_change_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_previous_lead_status_change_date,
             CASE
            WHEN actual_holiday_date BETWEEN  CAST(_lead_status_change_date AS DATE) AND extract_date
            THEN 1
            END in_date_range_lead_status_change_date
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,184,185,186,187

),

calculate_days_in_new_stage AS (

    SELECT
        *,
        (total_days_last_status_change_date__c - total_weekends_last_status_change_date__c - total_holidays__last_status_change_date__c) AS net_days_last_status_change_date__c,
        (total_days_lastactivitydate - total_weekends_lastactivitydate - total_holidays_lastactivitydate) AS net_days_stage_lastactivitydate,
        (total_days_created_date - total_weekends_createddate - total_holidays_createddate) AS net_days_new_stage_createddate,
        (total_days_task_created_date - total_weekends_task_created_date - total_holidays_task_created_date) AS net_days_new_stage_task_created_date,
        (total_days_mql_date__c - total_weekends_mql_date__c - total_holidays_mql_date__c) AS net_days_new_stage_mql_date__c,
        (total_days_previous_task_date - total_weekends_previous_task_date - total_holidays_previous_task_date) AS net_days_new_stage_previous_task_date,
        (total_days_previous_lead_status_change_date - total_weekends_previous_lead_status_change_date - total_holidays_previous_lead_status_change_date) AS net_days_new_stage_previous_lead_status_change_date,
         (total_days_lead_status_change_date - total_weekends__lead_status_change_date - total_holidays_lead_status_change_date) AS net_days_new_stage__lead_status_change_date,

    FROM count_total_holidays_between_date_range

)

SELECT * EXCEPT (_rownum)
FROM(
SELECT *, net_days_new_stage_previous_task_date - net_days_new_stage_task_created_date AS _date_different_task_net,net_days_new_stage_previous_lead_status_change_date-net_days_new_stage__lead_status_change_date AS net_new_data_status, ROW_NUMBER() OVER(PARTITION BY _email,_prospectID,_timestamp,_lead_status,_task_created_date,last_status_change_date__c,mql_date__c,_engagement,ownerid,subject,tasksubtype,task_type__c,case_safe_id__c,status,createddate,lastmodifieddate,Assign_to,
leadsource,
mql_date__c,
last_status_change_date__c ORDER BY _task_created_date DESC) AS _rownum  FROM calculate_days_in_new_stage
) 
WHERE 
_rownum = 1

;

CREATE OR REPLACE TABLE `x-marketing.pcs.opportunity_demand_gen` AS
WITH account_info AS (
    SELECT * FROM (
    SELECT DISTINCT email.* EXCEPT(_prospectID,_email),
    CASE WHEN new_lead_status IS NULL THEN _lead_status ELSE new_lead_status END AS _newls,id AS _prospectID , lead.email AS _email 
    FROM `x-marketing.pcs_salesforce.Lead` lead
    JOIN  (SELECT DISTINCT *, case 
    --when _email = 'peterchemidlin@familyinvestors.com' THEN '00Q5x00001zzbzDEAQ'
    when _email = 'phil@cookandassoc.com' THEN '00Q5x000021XdAUEA0' ELSE _prospectID END AS _id
        FROM `x-marketing.pcs.db_campaign_analysis` 
        WHERE 
            _engagement IN ("Web",'Downloaded',"Clicked") 
           ) email ON lead.id = email._id
    ) 
    --WHERE 
    --_newls = 'Sales Qualified' 
    --AND
  
    
),conduct_demo AS (
    SELECT opportunityid, 
        stagename AS conduct_demo_stage, 
        createddate, 
        isdeleted 
        FROM `x-marketing.pcs_salesforce.OpportunityHistory` 
        WHERE stagename = 'Conduct Demo' 
),aggrement_sent AS (
    SELECT opportunityid, 
        stagename AS _aggrement_sent, 
        createddate, 
        isdeleted 
        FROM `x-marketing.pcs_salesforce.OpportunityHistory` 
        WHERE stagename = 'Agreement Sent' 
),closed_won AS (
    SELECT opportunityid, 
stagename AS _closed_won, 
createddate, 
isdeleted 
FROM `x-marketing.pcs_salesforce.OpportunityHistory` 
WHERE stagename = 'Closed Won' 
),closed_lost AS (
    SELECT opportunityid, 
stagename AS _closed_lost, 
createddate, 
isdeleted 
FROM `x-marketing.pcs_salesforce.OpportunityHistory` 
WHERE stagename = 'Closed Lost' 
)
,opps_data AS (
    WITH all_historical_opp_stages AS (
        SELECT
            main.id AS _opportunityID,
            main.name AS _opportunityName,
            main.createddate AS _createTS,
            main.closedate AS _closeTS,
            CAST(main.createddate AS DATE) + 1 AS _createdDate,
            main.accountid AS accountid, 
            main.amount AS _amount, 
           0 AS _acv,
            main.type AS _type_opportunity,
            side.createddate AS opp_hist_createddate,
            main.laststagechangedate AS _oppLastChangeinStage,
            side.stagename AS _currentStage,
            LAG(side.stagename) OVER(
                PARTITION BY main.id 
                ORDER BY side.createddate
            ) AS _previousStage,
            sort.defaultprobability AS _currentStageProbability,
            sub_stage__c,
            laststagechangedate, 
            assets_current__c, 
            tpa__c, 
            client_product__c, 
            plan_id__c, 
            plan_type__c, 
            advisor_fiduciary_status__c, 
            advisor_fee__c, 
            advisor_secondary_contact__c, 
            advisor_fee_schedule__c, 
            application_downloaded__c, 
            ownerid,
            CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
             CASE WHEN main.id = "0065x000029iEMyAAM" THEN "00Q5x00001tbJcaEAE"
             ELSE main.contactid END AS contactid,
              main.recordtypeid,main.recordtype_name__c,lead_id__c,CONCAT("https://pcsretirement.lightning.force.com/lightning/r/Opportunity/",main.id,"/view") AS opportunity_link
        FROM `x-marketing.pcs_salesforce.Opportunity` main
        JOIN `x-marketing.pcs_salesforce.OpportunityHistory` side
        ON main.id = side.opportunityid
        LEFT JOIN `x-marketing.pcs_salesforce.OpportunityStage` sort 
        ON main.stagename = sort.masterlabel
        LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = main.ownerid
        WHERE  --main.isdeleted = false AND
         main.recordtypeid  = '0125x00000071cnAAA' AND main.id  <> '0065x000029iSI5AAM'
        --AND main.type NOT LIKE '%Renewal%'
    )
    ,latest_opps AS (
        SELECT * EXCEPT(rownum, opp_hist_createddate)
        FROM (
            SELECT
                main.*,
                sort.defaultprobability AS _previousStageProbability,
                DATE_DIFF(
                    CURRENT_DATE(), 
                    DATE(main.opp_hist_createddate), 
                    DAY
                ) AS _daysCurrentStage,
                ROW_NUMBER() OVER (
                    PARTITION BY main._opportunityID
                    ORDER BY main.opp_hist_createddate DESC
                ) AS rownum
            FROM all_historical_opp_stages AS main
            LEFT JOIN `x-marketing.pcs_salesforce.OpportunityStage` sort 
            ON main._previousStage = sort.masterlabel
            WHERE main._previousStage IS NULL
            OR main._currentStage != main._previousStage
        )
        WHERE rownum = 1
        ORDER BY _createTS DESC
            )
    SELECT
    TIMESTAMP(CURRENT_DATETIME('America/New_York')) AS extractDate, 
    CURRENT_DATE('America/New_York') AS extract_date,
        latest_opps.*,
        account_info.* 
    FROM  latest_opps
     JOIN account_info   ON  latest_opps.lead_id__c = account_info._prospectid 
), all_data AS (
  SELECT opps_data.*,conduct_demo_stage,_aggrement_sent,_closed_won, _closed_lost, 
CASE WHEN _currentStage IS NULL THEN _newls ELSE _currentStage END AS _stages_sql  
FROM opps_data
LEFT JOIN conduct_demo ON opps_data._opportunityID = conduct_demo.opportunityid
LEFT JOIN aggrement_sent ON opps_data._opportunityID = aggrement_sent.opportunityid
LEFT JOIN closed_won ON opps_data._opportunityID = closed_won.opportunityid
LEFT JOIN closed_lost ON opps_data._opportunityID = closed_lost.opportunityid
ORDER BY _createTS DESC
),

unique_years_involved AS (

    SELECT
        EXTRACT(YEAR FROM _createTS) AS year
    FROM all_data

    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM all_data

),

all_holiday_dates AS (
    
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL 

    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 

    UNION ALL

    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     

    UNION ALL

    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [9] Veterans Day (Nov 11)
    -- SELECT 
    --     "Veterans Day" AS holiday_name, 
    --     DATE(year, 11, 11) AS holiday_date 
    -- FROM unique_years_involved 

    -- UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL   

    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 

),

add_filler_info AS (

    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates

),

replacement_holiday_dates AS (

    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info

),

actual_holiday_dates AS (

    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates

),

cross_join_leads_with_holidays AS (

    SELECT 
        main.*,
        side.actual_holiday_date
    FROM all_data AS main
    CROSS JOIN actual_holiday_dates AS side

),

count_total_days_between_date_range AS (

    SELECT
        *,
        DATE_DIFF(extract_date,  DATE(_createTS), DAY) AS total_days
    FROM cross_join_leads_with_holidays

),

count_total_weekends_between_date_range AS (

    SELECT
        *, (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  DATE(_createTS), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _createTS) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends
    FROM count_total_days_between_date_range

),

count_total_holidays_between_date_range AS (

    SELECT
        * EXCEPT(actual_holiday_date, in_date_range),
        COALESCE(SUM(in_date_range), 0) AS total_holidays
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN DATE( _createTS) AND extract_date
                THEN 1
            END in_date_range
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174
),

calculate_days_in_new_stage AS (

    SELECT
        *,
        (total_days - total_weekends - total_holidays) AS days_in_new_stage
    FROM count_total_holidays_between_date_range

)

SELECT * FROM calculate_days_in_new_stage
WHERE _opportunityID  NOT IN ( '0065x000029iSI5AAM');




CREATE OR REPLACE TABLE pcs.db_account_engagements AS 
WITH tam_contacts AS (
      SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT DISTINCT
        firstname AS _firstname, 
        lastname AS _lastname, 
        title AS _title, 
        COALESCE(null, CAST(NULL AS STRING)) AS _2xseniority,
        email AS _email, 
        convertedaccountid AS _accountid,
        RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
        company AS _accountname, 
        industry AS _industry, 
        COALESCE(null, CAST(NULL AS STRING)) AS _tier,
        status, data_link2__ddl_status__c AS lead_status__c, 
        CAST(NUll AS INTEGER) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY email 
            ORDER BY createddate DESC
        ) _rownum
    FROM 
      `x-marketing.pcs_salesforce.Lead` main
    WHERE 
      NOT REGEXP_CONTAINS(email, 'pcs|2x.marketing') 
  )
  WHERE _rownum = 1
)
,email_engagement AS (
    SELECT * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `pcs.db_campaign_analysis`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|pcs|gmail|yahoo|outlook|hotmail') 
      AND NOT REGEXP_CONTAINS(_contentTitle, 'test')
      AND _domain IS NOT NULL
      --AND _year = 2022
    ORDER BY 1, 3 DESC, 2 DESC
)
/*,web_views AS (
  SELECT 
    email AS _email, 
    RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
    activity_date AS _date, 
    EXTRACT(WEEK FROM activity_date) AS _week,  
    EXTRACT(YEAR FROM activity_date) AS _year, 
    entry_page AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(NULL AS STRING) AS _description
  FROM `impartner.mkto_web_visits` web 
  WHERE NOT REGEXP_CONTAINS(entry_page, 'Unsubscribe')
  OR NOT REGEXP_CONTAINS(LOWER(utm_source__c), 'linkedin|google|email') 
)
,ad_clicks AS (
  SELECT 
    email AS _email, 
    RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
    activity_date, 
    EXTRACT(WEEK FROM activity_date) AS _week,  
    EXTRACT(YEAR FROM activity_date) AS _year, 
    entry_page AS _pageName, 
    "Ad Clicks" AS _engagement, 
    CAST(NULL AS STRING) AS _description
  FROM `impartner.mkto_web_visits` web 
  WHERE  REGEXP_CONTAINS(LOWER(utm_source__c), 'linkedin|google|email') 
)
,content_engagement AS (
  SELECT 
    email AS _email, 
    RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
    activity_date, 
    EXTRACT(WEEK FROM activity_date) AS _week,  
    EXTRACT(YEAR FROM activity_date) AS _year, 
    entry_page AS _pageName, 
    "Content Engagement" AS _engagement, 
    CAST(NULL AS STRING) AS _description
  FROM `impartner.mkto_web_visits` web 
  WHERE  REGEXP_CONTAINS(LOWER(entry_page), 'blog|commid=')
)*/
,form_fills AS (
     SELECT * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `pcs.db_campaign_analysis`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ _engagement IN ('Downloaded')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|pcs|gmail|yahoo|outlook|hotmail') 
      AND _domain IS NOT NULL 
    ORDER BY 1, 3 DESC, 2 DESC
)
,dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
) 
,
#Combining the engagements - Contact based and account based engagements
contact_engagement AS (
#Contact based engagement query
  SELECT 
    DISTINCT 
    tam_contacts._domain, 
    tam_contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    -- CAST(NULL AS INTEGER) AS _avg_bombora_score,
    tam_contacts.*EXCEPT(_domain, _email),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  JOIN
    tam_contacts USING(_email) 
)/*,
account_engagement AS (
#Account based engagement query
   SELECT 
    DISTINCT 
    tam_accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS STRING) AS _id, 
    CAST(NULL AS STRING) AS _contact_type,
    CAST(NULL AS STRING) AS _firstname, 
    CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _2xseniority,
    tam_accounts.*EXCEPT(_domain),
    CAST(engagements._date AS DATETIME)
  FROM 
    dummy_dates
  JOIN (*/
    /* SELECT * FROM intent_score UNION ALL */
    /*SELECT * FROM web_views 
    UNION ALL
    SELECT * FROM ad_clicks UNION ALL
    SELECT * FROM content_engagement
  ) engagements USING(_week, _year)
  JOIN
    (
      SELECT 
        DISTINCT _domain, 
        _accountid, 
        _accountname, 
        _industry, 
        _tier, 
        _annualrevenue 
      FROM 
        tam_contacts
    ) tam_accounts
    USING(_domain)
)*/
,
combined_engagements AS (
  SELECT * FROM contact_engagement
)
SELECT 
  DISTINCT
  _domain,
  _accountid,
  _date,
  SUM(IF(_engagement = 'Email Opened', 1, 0)) AS _emailOpens,
  SUM(IF(_engagement = 'Email Clicked', 1, 0)) AS _emailClicks,
  SUM(IF(_engagement = 'Email Downloaded', 1, 0)) AS _emailDownloads,
  SUM(IF(_engagement = 'Form Filled', 1, 0)) AS _gatedForms,
  SUM(IF(_engagement = 'Web Visit', 1, 0)) AS _webVisits,
  SUM(IF(_engagement = 'Ad Clicks', 1, 0)) AS _adClicks,
FROM 
  combined_engagements
GROUP BY 
  1, 2, 3
ORDER BY _date DESC;
--Limit 1



CREATE OR REPLACE TABLE `x-marketing.pcs.opportunity_demand_gen_status` AS
WITH account_info AS (
SELECT * FROM (
    SELECT DISTINCT 
    CASE WHEN new_lead_status IS NULL THEN _lead_status ELSE new_lead_status END AS _newls,id AS _prospectID , lead.email AS _email,_lead_status,_engagement ,_timestamp,email.mql_source__c,_territory,_email AS _dup_email,_Salesforceownername,_Salesforceownerid
    FROM `x-marketing.pcs_salesforce.Lead` lead
     JOIN  (SELECT DISTINCT *, case 
     --when _email = 'peterchemidlin@familyinvestors.com' THEN '00Q5x00001zzbzDEAQ'
    when _email = 'phil@cookandassoc.com' THEN '00Q5x000021XdAUEA0' ELSE _prospectID END AS _id
        FROM `x-marketing.pcs.db_campaign_analysis` 
        WHERE 
            _engagement IN ('Downloaded',"MQL Score","Web","Clicked",'MQL') 
           ) email ON lead.id = email._id
    ) 
    --WHERE 
    --_newls = 'Sales Qualified' 
    --AND
),opps_data AS (
    WITH all_historical_opp_stages AS (
        SELECT
            main.id AS _opportunityID,
            main.name AS _opportunityName,
            main.createddate AS _createTS,
            main.closedate AS _closeTS,
            main.accountid AS accountid, 
            main.amount AS _amount, 
           0 AS _acv,
            main.type AS _type_opportunity,
            side.createddate AS opp_hist_createddate,
            main.laststagechangedate AS _oppLastChangeinStage,
            side.stagename AS _currentStage,
            LAG(side.stagename) OVER(
                PARTITION BY main.id 
                ORDER BY side.createddate
            ) AS _previousStage,
            sort.defaultprobability AS _currentStageProbability,
            sub_stage__c,
            laststagechangedate, 
            assets_current__c, 
            tpa__c, 
            client_product__c, 
            plan_id__c, 
            plan_type__c, 
            advisor_fiduciary_status__c, 
            advisor_fee__c, 
            advisor_secondary_contact__c, 
            advisor_fee_schedule__c, 
            application_downloaded__c, 
            ownerid,
            CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
             CASE WHEN main.id = "0065x000029iEMyAAM" THEN "00Q5x00001tbJcaEAE"
             ELSE main.contactid END AS contactid,
              main.recordtypeid,main.recordtype_name__c,lead_id__c,CONCAT("https://pcsretirement.lightning.force.com/lightning/r/Opportunity/",main.id,"/view") AS opportunity_link
        FROM `x-marketing.pcs_salesforce.Opportunity` main
        JOIN `x-marketing.pcs_salesforce.OpportunityHistory` side
        ON main.id = side.opportunityid
        LEFT JOIN `x-marketing.pcs_salesforce.OpportunityStage` sort 
        ON main.stagename = sort.masterlabel
        LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = main.ownerid
        WHERE  --main.isdeleted = false AND
         main.recordtypeid  = '0125x00000071cnAAA' AND  main.id <> '0065x000029iSI5AAM'
        --AND main.type NOT LIKE '%Renewal%'
    )
    ,latest_opps AS (
        SELECT * EXCEPT(rownum, opp_hist_createddate)
        FROM (
            SELECT
                main.*,
                sort.defaultprobability AS _previousStageProbability,
                DATE_DIFF(
                    CURRENT_DATE(), 
                    DATE(main.opp_hist_createddate), 
                    DAY
                ) AS _daysCurrentStage,
                ROW_NUMBER() OVER (
                    PARTITION BY main._opportunityID
                    ORDER BY main.opp_hist_createddate DESC
                ) AS rownum
            FROM all_historical_opp_stages AS main
            LEFT JOIN `x-marketing.pcs_salesforce.OpportunityStage` sort 
            ON main._previousStage = sort.masterlabel
            WHERE main._previousStage IS NULL
            OR main._currentStage != main._previousStage
        )
        WHERE rownum = 1
        ORDER BY _createTS DESC
            )
    SELECT
    TIMESTAMP(CURRENT_DATETIME('America/New_York')) AS extractDate, 
        latest_opps.*,
        account_info.* 
    FROM  account_info  
    LEFT JOIN latest_opps  ON   account_info._prospectid = latest_opps.lead_id__c 
) SELECT opps_data.*,CASE WHEN _currentStage IS NULL THEN _newls ELSE _currentStage END AS new_lead_status,
CASE WHEN _createTS IS NULL THEN _timestamp ELSE _createTS END _time_date
FROM opps_data

;

INSERT INTO `x-marketing.pcs.opportunity_demand_gen_status` (
new_lead_status,
_time_date,
_engagement,
_email
)
WITH all_dates AS(
  SELECT DISTINCT LAST_DAY(CAST(_time_date AS DATE), MONTH) AS dates

  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
),
all_leadstage AS (
  SELECT DISTINCT _newls
  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
)   SELECT _newls,CAST(dates AS TIMESTAMP),'MQL',"@gopeanut.com"
 FROM all_leadstage
CROSS JOIN all_dates;

INSERT INTO `x-marketing.pcs.opportunity_demand_gen_status` (
new_lead_status,
_time_date,
_engagement,
_email
)
WITH all_dates AS(
  SELECT DISTINCT LAST_DAY(CAST(_createTS AS DATE), MONTH) AS dates

  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
),
all_leadstage AS (
  SELECT DISTINCT "Agreement Sent" As Stage
  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
)   SELECT Stage,CAST(dates AS TIMESTAMP),'MQL',"@gopeanut.com"
 FROM all_leadstage
CROSS JOIN all_dates;

INSERT INTO `x-marketing.pcs.opportunity_demand_gen_status` (
new_lead_status,
_time_date,
_engagement,
_email
)
WITH all_dates AS(
  SELECT DISTINCT LAST_DAY(CAST(_createTS AS DATE), MONTH) AS dates

  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
),
all_leadstage AS (
  SELECT DISTINCT "Closed Won" As Stage
  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
)   SELECT Stage,CAST(dates AS TIMESTAMP),'MQL',"@gopeanut.com"
 FROM all_leadstage
CROSS JOIN all_dates;

INSERT INTO `x-marketing.pcs.opportunity_demand_gen_status` (
new_lead_status,
_time_date,
_engagement,
_email
)
WITH all_dates AS(
  SELECT DISTINCT LAST_DAY(CAST(_createTS AS DATE), MONTH) AS dates

  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
),
all_leadstage AS (
  SELECT DISTINCT "Closed Lost" As Stage
  FROM `x-marketing.pcs.opportunity_demand_gen_status` 
)   SELECT Stage,CAST(dates AS TIMESTAMP),'MQL',"@gopeanut.com"
 FROM all_leadstage
CROSS JOIN all_dates;


CREATE OR REPLACE TABLE `x-marketing.pcs.MQLs_Disposition_all_lead` AS
WITH leadstatus_o AS (
   with owner AS (
     -- SELECT * EXCEPT (_rownum)
    --FROM (
    --SELECT *
    --,
    --ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    --FROM (
        SELECT leadid,
        field,
        j.name as previous_status,
        l.name AS new_status,
        _lead_status_change_date,
        createbyfieldhistory
        FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    createdbyid AS createbyfieldhistory
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE  
  field IN( "Owner") AND 
  isdeleted IS FALSE 
         )k
          JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.previous_status
          JOIN `x-marketing.pcs_salesforce.User` l ON l.id = k.new_status /*AND newvalue = 'Contacted'*/ 
    --)WHERE  previous_status IN( 'New' ,'Open')
    --)WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'
)
,lead_field_change AS (
     -- SELECT * EXCEPT (_rownum)
    --FROM (
    --SELECT *
    --,
    --ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    --FROM (
    SELECT 
    leadid,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'
     WHEN field = 'created' THEN 'Created' ELSE field END AS field,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'
     WHEN field = 'created' THEN 'Created' ELSE oldvalue END AS previous_status,
    CASE WHEN field = 'leadConverted' THEN 'Lead Converted'
    WHEN field = 'leadMerged' THEN 'Lead Merged'  
    WHEN field = 'created' THEN 'Created' ELSE newvalue END AS new_status,
    createddate AS _lead_status_change_date,
    createdbyid AS createbyfieldhistory
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE  
  field IN( 'Status','leadConverted','leadMerged','created') AND 
  isdeleted IS FALSE  /*AND newvalue = 'Contacted'*/ 
    --)WHERE  previous_status IN( 'New' ,'Open')
    --)WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'
), contact_status_change AS (
  WITH status_change AS (
     SELECT  
    old.status AS old_status,
    old.id AS leadid,
    old.email AS _old_email,
    convertedcontactid,
    createdbyid
FROM `x-marketing.pcs_salesforce.Lead` old
WHERE convertedcontactid is NOT NULL AND isdeleted IS FALSE 
  )
SELECT * EXCEPT (rownum)
FROM(
SELECT leadid, 
CASE WHEN field = "contactMerged" THEN "Contact Merged" ELSE field END AS field, CASE WHEN oldvalue IS NULL THEN " Contact Merged " ELSE oldvalue END AS oldvalue, CASE WHEN newvalue IS NULL THEN "Contact Merged " ELSE newvalue END AS newvalue, createddate,history.createdbyid,
ROW_NUMBER() OVER(PARTITION BY leadid,contactid ORDER BY createddate DESC) AS rownum

 FROM `x-marketing.pcs_salesforce.ContactHistory`  history
 JOIN status_change ON history.contactid = status_change.convertedcontactid
 WHERE isdeleted IS FALSE AND field IN ("contactMerged")
) WHERE rownum = 1 
) 
SELECT k.*,LAG(_lead_status_change_date) OVER(PARTITION BY leadid,k.createbyfieldhistory ORDER BY _lead_status_change_date) previous_lead_status_change_date,
CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername_field_history,

FROM (
SELECT * FROM lead_field_change
UNION ALL 
SELECT * FROM contact_status_change
UNION ALL 
SELECT * FROM owner
)k
LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.createbyfieldhistory
--WHERE field IN( 'Status',"Owner",'leadConverted','leadMerged')
)
,all_data AS (

SELECT m.*,
k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid, k.leadsource,mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY) AS _duration,
  SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_date_diff_last_activity,
SUM(TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_duration,
  -- task_type__c, 
  --  tasksubtype, 
  --  subject,
    case_safe_id__c,
    _lead_status_change_date,
    previous_lead_status_change_date,
    --DATE_DIFF(_lead_status_change_date, previous_lead_status_change_date, DAY) _date_diff_task,
    ownername_field_history,
    --ownername_field_history,
    leadstatus.field AS field_change,
    previous_status,
    new_status,
    DATETIME(_lead_status_change_date,'America/New_York'),
    leadstatus.field,previous_lead_status_change_date AS previous_lead_status_change_dates
    
FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    --LEFT JOIN task ON  k.id = task.whoid /* AND k.createdbyid = task.ownerid */
    LEFT JOIN (SELECT *  FROM leadstatus_o ) leadstatus ON k.id = leadstatus.leadid
     JOIN (SELECT * FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ('Web','Downloaded','MQL','MQL Score')) m  ON k.id = m._prospectID
) ,leadstatus AS (
    SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY previous_lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE /*AND newvalue = 'Contacted'*/ 
    )WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1 
    --AND leadid = '00Q5x000021VOPaEAO'

),task AS(
    SELECT 
    task_type__c, 
    tasksubtype, 
    subject,
    whoid,
     k.ownerid, k.createddate AS _task_created_date,
     j.name as Assign_to,
     LAG(k.createddate) OVER(PARTITION BY whoid,k.ownerid ORDER BY k.createddate) previous_task_date
    FROM `x-marketing.pcs_salesforce.Task` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    ---JOIN `x-marketing.pcs_salesforce.Lead` l ON k.whoid = l.id /* AND k.ownerid = l.createdbyid */
   WHERE k.ownerid NOT IN (  '00560000001R83WAAS','005f2000008Y2yNAAS') AND status <>'Not Started' AND isdeleted IS FALSE AND k.createddate >= "2022-08-23" 
),contact_status AS (
    SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY _lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate ) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE AND newvalue = 'Contacted' 
    )
    --WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1
),engaged_status AS (
  SELECT * EXCEPT (_rownum)
    FROM (
    SELECT *
    ,
    ROW_NUMBER() OVER(PARTITION BY leadid ORDER BY _lead_status_change_date DESC) AS _rownum
    FROM (
    SELECT 
    leadid,
    field,
    oldvalue AS previous_status,
    newvalue AS new_status,
    createddate AS _lead_status_change_date,
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate ) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field = 'Status' AND isdeleted IS FALSE AND newvalue = 'Engaged' 
    )
    --WHERE  previous_status IN( 'New' ,'Open')
    )WHERE _rownum = 1
), total_leads AS ( 
  WITH sent  AS (
SELECT g.*
FROM `x-marketing.pcs_salesforce.Lead` l
 JOIN (SELECT * FROM  `x-marketing.pcs.db_campaign_analysis`
WHERE _engagement IN( 'Sent')) g on l.id = g._prospectID
),mqls AS (
  SELECT DISTINCT l.id 
FROM `x-marketing.pcs_salesforce.Lead` l
 JOIN (SELECT DISTINCT _prospectID FROM  `x-marketing.pcs.db_campaign_analysis`
WHERE _engagement IN ('Downloaded','MQL Score')) g on l.id = g._prospectID
) SELECT sent.* FROM sent
 --LEFT JOIN mqls ON sent._prospectID = mqls.id 
--WHERE mqls.id IS NULL
)
,all_leads AS (

SELECT m.*,
k.status, 
    k.createddate, 
    k.name, 
    k.lastname, 
    k.firstname, 
    k.lastactivitydate, 
    k.lastmodifieddate, 
    k.lastvieweddate, 
    k.lastreferenceddate, 
    k.lasttransferdate,
    k.territory__c,
    k.email,
    k.company,
    k.state,
    k.title,
    k.pardot_campaign__c,
    total_lead_score__c,
        dd_linkedin__c,
    k.id,
        CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',k.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    k.ownerid, k.leadsource,mql_date__c, 
   last_status_change_date__c,
   TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(last_status_change_date__c AS DATETIME), DAY) AS _date_diff,
      TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY) AS _date_diff_last_activiy,
   TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY) AS _duration,
  SUM(TIMESTAMP_DIFF(CURRENT_DATETIME('America/New_York'), CAST(k.lastactivitydate AS DATETIME), DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_date_diff_last_activity,
SUM(TIMESTAMP_DIFF(k.lastactivitydate,_lead_status_change_date, DAY)) OVER (PARTITION BY k.id ORDER BY _lead_status_change_date) AS running_total_duration,
  -- task_type__c, 
  --  tasksubtype, 
  --  subject,
    case_safe_id__c,
    _lead_status_change_date,
    previous_lead_status_change_date,
    --DATE_DIFF(_lead_status_change_date, previous_lead_status_change_date, DAY) _date_diff_task,
    ownername_field_history,
    --ownername_field_history,
    leadstatus.field AS field_change,
    previous_status,
    new_status,
    DATETIME(_lead_status_change_date,'America/New_York') AS _lead_status_change_date_datetime,
    previous_lead_status_change_date AS previous_lead_status_change_dates,
    dd_branch_address_id__c
    
FROM `x-marketing.pcs_salesforce.Lead` k
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.ownerid
    --LEFT JOIN task ON  k.id = task.whoid /* AND k.createdbyid = task.ownerid */
    LEFT JOIN (SELECT *  FROM leadstatus_o ) leadstatus ON k.id = leadstatus.leadid
     JOIN total_leads m  ON k.id = m._prospectID
) 
SELECT k.* 
        FROM(
              SELECT *,'All_leads' AS news FROM all_leads

            ) k
            LEFT JOIN (SELECT *  FROM contact_status ) contact_status ON k._prospectID = contact_status.leadid
            LEFT JOIN (SELECT *  FROM engaged_status ) engaged_status ON k._prospectID = engaged_status.leadid
--WHERE tasksubtype IS NOT NULL
;

CREATE OR REPLACE TABLE  `x-marketing.pcs.opportunity_lead` AS 
WITH new_status_leads AS (

    SELECT DISTINCT

        lead.name AS lead_name,
        lead.email AS lead_email,
        CONCAT(
            "https://pcsretirement.lightning.force.com/lightning/r/Opportunity/",
           _opportunityID ,
            '/view'
        ) AS salesforce_link,
        _opportunityID,
        _opportunityName,
        company,
            _territory
 AS territory__c,
             CAST(_createdDate  AS TIMESTAMP) AS  _createTS,
            stagename, 
            sub_stage__c,
            recordtypeid,
            recordtype_name__c,
        --_opportunityName,
     owner.name AS lead_owner_name,
 owner.email  AS lead_owner_email,
        DATE(laststagechangedate) AS last_status_change_date,
        CURRENT_DATE('America/New_York') AS extract_date,
        TIMESTAMP(CURRENT_DATETIME('Asia/Kuala_Lumpur')) AS run_date,
        

    FROM (

        SELECT
            lead_id__c,
            main.ownerid 
            AS ownerid,
            lead.name AS name,
            lead.email AS email,
           lead.company AS company,
            lead.territory__c AS _territory
,
            laststagechangedate,
            _newls,
            lead.isdeleted,
            main.id AS _opportunityID,
            main.name AS _opportunityName,
            main.createddate  AS _createTS,
            CAST(main.createddate AS DATE) + 1 AS _createdDate,
            main.closedate AS _closeTS,
            main.accountid AS accountid,stagename, 
            sub_stage__c,
            main.recordtypeid,
            main.recordtype_name__c
        FROM `x-marketing.pcs_salesforce.Opportunity` main
         
     LEFT JOIN `x-marketing.pcs_salesforce.Contact` c  ON main.contactid = c.id
      LEFT JOIN (SELECT email.*,CASE WHEN new_lead_status IS NULL THEN _lead_status ELSE new_lead_status END AS _newls,lead.email,lead.name,lead.company,lead.territory__c,lead.ownerid,lead.id
 FROM `x-marketing.pcs_salesforce.Lead` lead
   LEFT JOIN  (SELECT *,
        FROM `x-marketing.pcs.db_campaign_analysis` 
        WHERE 
            _engagement IN ("Web",'Downloaded') 
           ) email ON lead.id = email._prospectID ) lead ON main.lead_id__c = lead.id
        WHERE main.recordtypeid  = '0125x00000071cnAAA' 
        --AND main.id NOT IN ( '0065x000029iSI5AAM')
     ORDER BY  main.createddate DESC
        --WHERE main.accountid  = '001f2000023tCQpAAM'

    ) lead

    LEFT JOIN (

        SELECT
            id,
            name,
            email
        FROM `x-marketing.pcs_salesforce.User`

    ) owner 

    ON lead.ownerid = owner.id

    --WHERE NOT REGEXP_CONTAINS(lead.email, '@test.com|@pcsretirement.com') 
    --AND _opportunityID = '0123I000000MIm1QAG'
),

unique_years_involved AS (

    SELECT
        EXTRACT(YEAR FROM _createTS) AS year
    FROM new_status_leads

    UNION DISTINCT

    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM new_status_leads

),

all_holiday_dates AS (
    
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL

    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved

    UNION ALL 

    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 

    UNION ALL

    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     

    UNION ALL

    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved

    UNION ALL

    -- [9] Veterans Day (Nov 11)
    SELECT 
        "Veterans Day" AS holiday_name, 
        DATE(year, 11, 11) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 

    UNION ALL   

    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 

),

add_filler_info AS (

    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates

),

replacement_holiday_dates AS (

    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info

),

actual_holiday_dates AS (

    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates

),

cross_join_leads_with_holidays AS (

    SELECT 
        main.*,
        side.actual_holiday_date
    FROM new_status_leads AS main
    CROSS JOIN actual_holiday_dates AS side

),

count_total_days_between_date_range AS (

    SELECT
        *,
        DATE_DIFF(extract_date,  DATE(_createTS), DAY) + 1 AS total_days
    FROM cross_join_leads_with_holidays

),

count_total_weekends_between_date_range AS (

    SELECT
        *, (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date,  DATE(_createTS), WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM  _createTS) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends
    FROM count_total_days_between_date_range

),

count_total_holidays_between_date_range AS (

    SELECT
        * EXCEPT(actual_holiday_date, in_date_range),
        COALESCE(SUM(in_date_range), 0) AS total_holidays
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN DATE( _createTS) AND extract_date
                THEN 1
            END in_date_range
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13,14,15,16,17,18,19

),

calculate_days_in_new_stage AS (

    SELECT
        *,
        (total_days - total_weekends - total_holidays) AS days_in_new_stage
    FROM count_total_holidays_between_date_range

)

SELECT * FROM calculate_days_in_new_stage
WHERE _opportunityID  NOT IN ( '0065x000029iSI5AAM');



CREATE OR REPLACE TABLE `x-marketing.pcs.plead_lead_sql_mql` AS
WITH prospect_info AS (
  WITH  plan AS (
  SELECT p.id, p.name AS Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.Plan__c`  p
JOIN `x-marketing.pcs_salesforce.PlanLead__c`  l ON p.id = l.Converted_to_New_Plan__c

 ),planlead AS (
  SELECT name AS plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, forecast_amount_of_assets__c, participants__c,PROP_Total_Participants__c,StageName__c,WIN_E_mail_Date__c,	Converted_to_New_Plan__c,c.id AS plan_id, advisor__c,Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.PlanLead__c` c
LEFT JOIN plan on c.converted_to_new_plan__c = plan.id
 )
 , opps_id  AS  (
    SELECT l.id AS ops_lead_id,g.id as  opsid
FROM `x-marketing.pcs_salesforce.Lead` l
JOIN `x-marketing.pcs_salesforce.Opportunity` g ON l.id = g.lead_id__c
WHERE  g.recordtypeid  = '0125x00000071cnAAA'
 ), lead_history AS
(
  SELECT leadid,field,oldvalue,newvalue
  FROM `x-marketing.pcs_salesforce.LeadHistory` 
  WHERE field IN ('leadConverted','leadMerged')
) ,status_change AS (
     SELECT  
    news.status AS new_status,
    old.status AS old_status,
    old.masterrecordid AS old_masterrecordid ,
    old.id AS old_id,
    news.id AS new_id,
    CASE WHEN news.status = old.status THEN TRUE ELSE FALSE END AS _status_different,
    old.email AS _old_email,news.email AS _new_email,
    news.mql_source__c AS mql_source,
    news.mql_date__c
FROM `x-marketing.pcs_salesforce.Lead` old
JOIN `x-marketing.pcs_salesforce.Lead` News ON old.masterrecordid=news.id
)
, contact AS
(
  SELECT id AS _contactid,
  segment__c,leadsource
  FROM `x-marketing.pcs_salesforce.Contact`
)
, leads AS
(
    SELECT
    routable.name, 
    routable.firstname, 
    routable.lastname, 
    routable.id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    routable.title, 
    dd_bd_title_categories__c, 
    routable.phone, 
    CASE WHEN state._code IS NULL THEN IF(UPPER(routable.state) = 'INDIANA', 'IN', UPPER(routable.state)) ELSE UPPER(state._code) END AS _state, 
    routable.email,
    leadsource,
    CASE WHEN status = "Open" THEN 1 
          WHEN status = "Nurture" THEN 6
          WHEN status = "Sales Qualified" THEN 5 
          WHEN status = "Archived" THEN 7
          WHEN status = "Contacted" THEN 3
          WHEN status= "Engaged" THEN 4
          WHEN status = "Unqualified" THEN 8
          WHEN status = "New" THEN 2
          END AS _salesforceLeadStage,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',routable.id,'/view') AS link,
    CASE WHEN j.name LIKE '%Dev User%' THEN 'Dev User' ELSE j.name END AS ownername,
    routable.ownerid AS ownerid, 
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c,
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
   CASE WHEN routable.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
   WHEN routable.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
WHEN routable.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
      ELSE mql_source__c END AS mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    CAST(converteddate AS DATETIME) AS converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS link_m,
    total_lead_score__c,
    CASE WHEN mql_date__c IS NULL THEN routable.createddate ELSE mql_date__c END AS _mql_date,
    routable.createddate
    

    /*
    dd_current_ria_firm_1_crd__c, 
    dd_current_ria_firm_2_crd__c, 
    dd_current_bd_firm_1_crd__c, 
    dd_primary_bd_firm_crd__c, 
    dd_primary_ria_firm_crd__c, 
    dd_primary_firm_crd__c, 
    dd_prior_firm_2_firm_crd__c, 
    dd_prior_firm_1_firm_crd__c, 
    dd_prior_firm_3_firm_crd__c,
    dd_branch_address_id__c,individual_crd__c,
    firm_crd__c,
    dd_home_address_id__c*/
    FROM `x-marketing.pcs_salesforce.Lead` routable
    LEFT JOIN `x-marketing.pcs.db_state_code_lookup` state on CAST(routable.state AS STRING) = CAST(state._state AS STRING) or routable.state = state._code
    LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = routable.ownerid
    --WHERE id = '00Q5x000021VdQjEAK'
)
SELECT * EXCEPT(_rownum,createddate)
FROM (
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY email,id ORDER BY createddate DESC) AS _rownum
FROM (
SELECT name, 
    firstname, 
    lastname, 
    id, 
    status, 
    state__c, 
    state, 
    territory__c, 
    firm_crd__c, 
    individual_crd__c,
    data_link2__ddl_firmid__c,
    data_link2__ddl_repid__c,
    industry, 
    title, 
    dd_bd_title_categories__c, 
    phone, 
_state, 
    email,
    leads.leadsource,
 _salesforceLeadStage,
    link,
 ownername,
 ownerid, 
    average_retirement_plan_size_aua__c, 
    number_of_retirement_plans__c,
    retirement_aum__c, 
    of_plans_acquired_per_year__c,
    mql_source__c ,
    convertedcontactid,
    opsid AS  convertedopportunityid,
    isconverted,
converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
 link_m,
lead_history.*,
segment__c,new_status,mql_source,total_lead_score__c,_mql_date,
createddate,
plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, forecast_amount_of_assets__c, participants__c,PROP_Total_Participants__c,StageName__c,WIN_E_mail_Date__c,	Converted_to_New_Plan__c,Converted_to_New_Plan__c_name,plan_id, 
FROM leads
LEFT JOIN contact ON leads.convertedcontactid = contact._contactid
LEFT JOIN status_change ON leads.id = old_id
LEFT JOIN lead_history ON leads.id = leadid
LEFT JOIN opps_id  ON leads.id = opps_id.ops_lead_id
LEFT JOIN planlead  ON leads.convertedcontactid = planlead.advisor__c
) 
)
WHERE _rownum = 1
), plan AS (
  SELECT p.id, p.name AS Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.Plan__c`  p
JOIN `x-marketing.pcs_salesforce.PlanLead__c`  l ON p.id = l.Converted_to_New_Plan__c

 ),planlead AS (
  SELECT c.name AS plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, forecast_amount_of_assets__c, participants__c,PROP_Total_Participants__c,StageName__c,WIN_E_mail_Date__c,	Converted_to_New_Plan__c,c.id AS plan_id, c.advisor__c,plan.Converted_to_New_Plan__c_name
FROM `x-marketing.pcs_salesforce.PlanLead__c` c
LEFT JOIN plan on c.converted_to_new_plan__c = plan.id
JOIN `x-marketing.pcs_salesforce.Contact` j ON j.id = c.advisor__c
 ),opps_id  AS  (
    SELECT l.id AS ops_lead_id,g.id as  opsid,g.ownerid AS opps_owner,j.name AS opportunity_owner_name, l.name as opportunityname
FROM `x-marketing.pcs_salesforce.Lead` l
JOIN `x-marketing.pcs_salesforce.Opportunity` g ON l.id = g.lead_id__c
LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = g.ownerid
WHERE  g.recordtypeid  = '0125x00000071cnAAA'
 )
 SELECT * 
 FROM planlead k
JOIN (SELECT case 
     --when _email = 'peterchemidlin@familyinvestors.com' THEN '00Q5x00001zzbzDEAQ'
    when _email = 'phil@cookandassoc.com' THEN '00Q5x000021XdAUEA0' ELSE _prospectID END AS _prospectID,_timestamp,_utm_campaign,campaignName,_engagement,convertedcontactid,_mql_dates, _lead_score, new_mql_source, salesforce_mastercontactid, new_lead_status, Salesforce_Link, masterrecordid, _email_segment, segment__c, newvalue, oldvalue, field, leadid, convertedaccountid, converteddate, isconverted, convertedopportunityid,  mql_source__c, of_plans_acquired_per_year__c, retirement_aum__c, average_retirement_plan_size_aua__c, number_of_retirement_plans__c, _target_value, _leadsource, _lead_status, _Salesforceownername, _Salesforceownerid, _salesforce_lead_status 
FROM `x-marketing.pcs.db_campaign_analysis` WHERE _engagement IN ("Web",'Downloaded','MQL Score')) m  ON k.advisor__c= m.convertedcontactid
LEFT JOIN opps_id ON opps_id.ops_lead_id = m._prospectID;



