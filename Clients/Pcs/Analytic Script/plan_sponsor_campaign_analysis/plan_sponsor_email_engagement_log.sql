TRUNCATE TABLE `x-marketing.pcs.plan_sponsor_email_performance`;

INSERT INTO `x-marketing.pcs.plan_sponsor_email_performance` (

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
   _employees
  --, _salesforce_lead_status,_last_timestamp_sec
  )
WITH 
prospect_info AS (
  with 
  plan AS (
    SELECT 
    p.id, 
    p.name AS Converted_to_New_Plan__c_name
    FROM `x-marketing.pcs_salesforce.Plan__c`  p
    JOIN `x-marketing.pcs_salesforce.PlanLead__c`  l ON p.id = l.Converted_to_New_Plan__c
  )
  , planlead AS (
    SELECT 
    name AS plan_lead_name, 
    planname__c , 
    excl_pcs_revenue__c, 
    forecast_amount_of_assets__c,
    participants__c,
    PROP_Total_Participants__c,
    StageName__c,
    WIN_E_mail_Date__c,
    Converted_to_New_Plan__c,c.id AS plan_id,
    advisor__c,
    Converted_to_New_Plan__c_name
    FROM `x-marketing.pcs_salesforce.PlanLead__c` c
    LEFT JOIN plan on c.converted_to_new_plan__c = plan.id
  )
  , opps_id AS (
    SELECT 
    l.id AS ops_lead_id,
    g.id as  opsid
    FROM `x-marketing.pcs_salesforce.Lead` l
    JOIN `x-marketing.pcs_salesforce.Opportunity` g ON l.id = g.lead_id__c
    WHERE  g.recordtypeid  = '0125x00000071cnAAA'
  )
  , lead_history AS (
    SELECT 
    leadid,
    field,
    oldvalue,
    newvalue
    FROM `x-marketing.pcs_salesforce.LeadHistory` 
    WHERE field IN ('leadConverted','leadMerged')
  )
  , status_change AS (
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
  , contact AS (
    SELECT 
    id AS _contactid,
    segment__c,
    leadsource
    FROM `x-marketing.pcs_salesforce.Contact`
  )
  , leads AS (
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
    CASE 
    WHEN state._code IS NULL THEN IF(UPPER(routable.state) = 'INDIANA', 'IN', UPPER(routable.state))
    ELSE UPPER(state._code) END AS _state, 
    routable.email,
    leadsource,
    CASE 
    WHEN status = "Open" THEN 1 
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
    CASE 
    WHEN routable.id = '00Q5x00001wOJ3MEAW' THEN 'Form Submission' 
    WHEN routable.id = '00Q5x00001wOOBpEAO' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wNmV6EAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNwUcEAK' THEN 'Video'
    WHEN routable.id = '00Q5x00001wNoiJEAS' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOMIHEA4' THEN 'Form Submission'
    WHEN routable.id = '00Q5x00001wOR57EAG' THEN 'Form Submission'
    ELSE mql_source__c 
    END AS mql_source__c,
    convertedcontactid,
    convertedopportunityid,
    isconverted,
    CAST(converteddate AS DATETIME) AS converteddate,
    convertedaccountid,
    isdeleted,
    masterrecordid,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS link_m,
    total_lead_score__c,
    CASE 
    WHEN mql_date__c IS NULL THEN routable.createddate 
    ELSE mql_date__c 
    END AS _mql_date,
   routable.createddate,
   CAST(numberofemployees AS STRING) AS _employees
    

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
    SELECT * 
    EXCEPT(_rownum,createddate)
    FROM (
      SELECT * ,
      ROW_NUMBER() OVER(PARTITION BY email,id ORDER BY createddate DESC) AS _rownum
      FROM (
        SELECT 
        name, 
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
        mql_source,
        total_lead_score__c,
        _mql_date,
        createddate,
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
        plan_id, 
        _employees
        FROM leads
        LEFT JOIN contact ON leads.convertedcontactid = contact._contactid
        LEFT JOIN status_change ON leads.id = old_id
        LEFT JOIN lead_history ON leads.id = leadid
        LEFT JOIN opps_id  ON leads.id = opps_id.ops_lead_id
        LEFT JOIN planlead  ON leads.convertedcontactid = planlead.advisor__c
        )
        )
        WHERE _rownum = 1
)
,email_campaign AS (
    SELECT * FROM (
    SELECT *,  ROW_NUMBER() OVER(PARTITION BY id,_code ORDER BY _livedate DESC) AS _rownum
    FROM (
    SELECT DISTINCT 
  _notes, 
  _status, 
  _trimcode, 
  _screenshot, 
  _assettitle, 
  _subject, 
  _whatwedo, 
  _campaignid, 
  _utm_campaign, 
  _preview, 
  _code, 
  _journeyname, 
  _emailsegment AS _campaignname, 
  _formsubmission, 
  _id, _livedate, 
  _utm_source, 
  _emailname,
   _assignee, 
   _utm_medium, 
   _landingpage,
  _campaignid AS id,
  _emailsequence AS _email_segment

  FROM `x-marketing.pcs_mysql.db_airtable_email_participant_engagement` 
  WHERE _rootcampaign = 'Plan Sponsor Demand Gen' AND  _campaignID  <> 'Obtain from DE>Campaign>Email JobID' 
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
  _code,
  _trimcode, 
  _screenshot, 
  _assettitle, 
  _subject, 
  _preview AS _whatwedo, 
  _campaignname AS campaignName, 
  _id, 
  safe.timestamp(_livedate) AS _livedate, 
  _utm_source, 
  _utm_medium, 
  _landingpage,
  _journeyname,
  _email_segment,
_code AS _type,
  ROW_NUMBER() OVER(PARTITION BY emailname,airtable.id,emailid,_email_segment ORDER BY senddate DESC) AS _rownum
  FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
  JOIN  email_campaign ON airtable.id  = SAFE_CAST(email_campaign.id AS INT64)
  ---WHERE airtable.id = 224947
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
   --CASE WHEN email_address = 'timothy.maher@raymondjames.com' THEN '00Q5x00001wOAH7EAO' ELSE 
   --lead_id END  AS 
   lead_id,
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
   
    FROM `x-marketing.pcs_sfmc.data_extension_Plan_Sponsor_Lead_Gen_VA`  activity
    --WHERE email_address NOT LIKE "%2x.marketing%" 
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
     JOIN (SELECT *,    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _campaign,
    
    REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(url, STRPOS(url, 'content_downloaded=') + 19), '&')[ORDINAL(1)], '%EM_', ' '), '%EM_',':') AS _content_downloaded
     FROM `x-marketing.pcs_sfmc.event`
     --WHERE sendid = 224947 AND subscriberkey = '00Q5x00001wNfQbEAK'
      ) campaignn ON campaignn.subscriberkey = activity.lead_id 
            AND content_downloaded = _content_downloaded
    LEFT JOIN `x-marketing.pcs_salesforce.Lead` l ON activity.lead_id= l.id /*or activity.subscriberkey = contactid*/
    LEFT JOIN `x-marketing.pcs_sfmc.send` campaign ON TRIM(emailname) = utm_campaign
    WHERE eventtype = 'Click' 
    AND 
    url LIKE '%PCSRetirement.accountsvc.com%'
        ))WHERE _rownum = 1
    
  
) 
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
    _email_segment, masterrecordid,prospect_info.link,new_status,mql_source,link_m,total_lead_score__c,_mql_date,
    plan_lead_name, 
  planname__c , 
  excl_pcs_revenue__c, 
  forecast_amount_of_assets__c, 
  participants__c,
  PROP_Total_Participants__c,
  StageName__c,
  WIN_E_mail_Date__c,	
  Converted_to_New_Plan__c,
  Converted_to_New_Plan__c_name,plan_id,
  _employees


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
    _email_segment, masterrecordid,prospect_info.link,new_status,mql_source,link_m,total_lead_score__c,_mql_date,
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
  plan_id,
  _employees


  FROM (
  SELECT * FROM lead_form_download

  ) engagements
LEFT JOIN airtable ON CAST(engagements._campaignID AS INT64) = airtable.id
LEFT JOIN prospect_info ON  engagements._prospectID = prospect_info.id;

--- Set Show Export
UPDATE `x-marketing.pcs.plan_sponsor_email_performance` origin
SET origin._showExport = 'Yes'
FROM (
    WITH focused_engagement AS (
        SELECT 
            _email, 
            _engagement, 
            _utm_campaign,
            CASE WHEN _engagement = 'Opened' THEN 1
                WHEN _engagement = 'Clicked' THEN 2
                WHEN _engagement = 'Downloaded' THEN 3
            END AS _priority
        FROM `x-marketing.pcs.plan_sponsor_email_performance`
        WHERE _engagement IN('Opened', 'Clicked', 'Downloaded')
        ORDER BY 1, 3 DESC 
    ),
    final_engagement AS (
        SELECT * EXCEPT(_priority, _rownum)
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY _email, _utm_campaign ORDER BY _priority DESC) AS _rownum
            FROM focused_engagement
        )
        WHERE _rownum = 1
    )    
    SELECT * FROM final_engagement 
) AS final
WHERE origin._email = final._email
AND origin._engagement = final._engagement
AND origin._utm_campaign = final._utm_campaign;

UPDATE `x-marketing.pcs.plan_sponsor_email_performance` origin
SET origin._dropped = 'True'
FROM (
    SELECT 
        _campaignID, 
        _email,_prospectID
    FROM (
        SELECT 
            _campaignID, 
            _email,_prospectID,
            SUM(CASE WHEN _engagement = 'Opened' THEN 1 END) AS _hasOpened,
            SUM(CASE WHEN _engagement = 'Clicked' THEN 1 END) AS _hasClicked,
            SUM(CASE WHEN _engagement IN( 'Soft bounce','Hard bounce','Block bounce') THEN 1 END) AS _hasBounced,
        FROM 
            `x-marketing.pcs.plan_sponsor_email_performance`
        WHERE
            _engagement IN ('Opened', 'Clicked', 'Soft bounce','Hard bounce','Block bounce')
        GROUP BY
            1, 2,3
    )
    WHERE 
    (_hasClicked IS NOT NULL
    AND _hasBounced IS NOT NULL) OR (_hasOpened IS NOT NULL
    AND _hasBounced IS NOT NULL)
    ) scenario
WHERE 
    origin._email = scenario._email
AND origin._campaignID = scenario._campaignID
AND origin._prospectID = scenario._prospectID
AND origin._engagement IN('Soft bounce','Hard bounce','Block bounce');

UPDATE `x-marketing.pcs.plan_sponsor_email_performance`  origin 
SET origin._isBot = 'True'
FROM (
SELECT _campaignID, _email, _prospectID
FROM (
SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linked_clicked LIKE "%DG-EM%" THEN 1
WHEN _linked_clicked = "Content_downloaded" THEN 1  END) AS _content_donwloaded,
SUM(CASE WHEN _linked_clicked = "Bot"  THEN 1 END) AS _bot
FROM `x-marketing.pcs.plan_sponsor_email_performance`
WHERE _engagement = 'Clicked' 
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) scenario
WHERE origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linked_clicked LIKE  "%DG-EM%";