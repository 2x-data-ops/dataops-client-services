TRUNCATE TABLE `x-marketing.pcs.dg_email_performance`;

INSERT INTO `x-marketing.pcs.dg_email_performance` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  _email,
  _timestamp,
  _name,
  _region,
  _description,
  _utm_source, 
   _utm_content,
   _utm_medium,
  _abstract,
  _linkid,


  _utm_campaign,
  _subject,
  _salesforceCreated,
  _contentTitle,
  _screenshot, 
  _campaignSentDate,
  _storyBrandStage, 
 _subCampaign, 
  _landingpage,
  _segment,
  _links,
  _rootcampaign,

  _title,
  _phone,

  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  _salesforce_link
)
WITH open_event AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Opened' AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,subscriberkey ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Open' 
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1

), 
click_event AS(
  --SELECT * EXCEPT(rownum)
 -- FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      --account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
     SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_source=') + 11), '&')[ORDINAL(1)]  AS utm_source ,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_content=') + 12), '&')[ORDINAL(1)]  AS utm_content, 
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_medium=') + 11), '&')[ORDINAL(1)] AS utm_medium,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'content_downloaded=') + 19), '&')[ORDINAL(1)]  AS content_downloaded,
  CASE WHEN activity.url LIKE '%link=botclick%' THEN 'Bot'
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
  WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
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
     -- ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  --)
 -- WHERE rownum = 1

), unique_click AS (
   SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Unique' AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_source=') + 11), '&')[ORDINAL(1)]  AS utm_source ,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_content=') + 12), '&')[ORDINAL(1)]  AS utm_content, 
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_medium=') + 11), '&')[ORDINAL(1)] AS utm_medium,
  SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'content_downloaded=') + 19), '&')[ORDINAL(1)]  AS content_downloaded,
  CASE WHEN activity.url LIKE '%link=botclick%' THEN 'Bot'
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
  WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
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
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1

 ),
sent_event AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Sent' AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Sent'
    
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1

  
), 
hard_bounce AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      eventtype AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
    --  account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype IN ('HardBounce','OtherBounce','SoftBounce') 
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1

), 
unsubscribe AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Unsubscribe' AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
    --  account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Lead`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Unsubscribe'
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
)
, 
email_campaign AS (
  SELECT * EXCEPT (rownum)
  FROM (
    SELECT 
      *,  
      ROW_NUMBER() OVER(
          PARTITION BY id,_code 
          ORDER BY _livedate DESC
      ) AS rownum
    FROM (
      SELECT DISTINCT 
        _notes, 
        _status, 
        _trimcode, 
        _screenshot, 
        _assettitle, 
        _subject, 
        _whatwedo, 
        _campaignid AS id, 
        _utm_campaign, 
        _preview, 
        _code, 
        _journeyname,
        _campaignname, 
        _formsubmission, 
        _id, 
        _livedate, 
        _utm_source, 
        _emailname, 
        _assignee, 
        _utm_medium, 
        _landingpage,
        _emailsequence AS _segment,
        _link,
        _rootcampaign
      FROM `x-marketing.pcs_mysql.db_airtable_email_participant_engagement` 
      WHERE _rootcampaign = 'Demand Generation'
      ORDER BY _code
      )
  ) 
  WHERE rownum = 1
  AND id != ''
  AND id IS NOT NULL
),
airtable AS (
  SELECT 
    * EXCEPT(rownum)
  FROM (
    SELECT 
        -- sentdate, 
        name.value.name AS _utm_campaign, 
        -- senddate, 
        -- status, 
        -- airtable.emailid, 
        TRIM(subject) AS subject, 
        -- fromname, 
        -- TRIM(airtable.emailname) AS emailname, 
        --fromaddress, 
        PARSE_DATETIME("%Y-%m-%dT%H:%M:%SZ",airtable.createddate) AS createddate, 
        -- isalwayson,
        airtable.id,
        _code,
        -- _trimcode, 
        _screenshot, 
        -- _assettitle, 
        -- _subject, 
        -- _preview AS _whatwedo, 
        -- _campaignname AS campaignName, 
        -- _id, 
        safe.timestamp(_livedate) AS _livedate, 
        _utm_source, 
        _utm_medium, 
        _landingpage,
        -- _journeyname,
        _segment,
        _link,
        -- _code AS _type,
        -- airtable.id AS sendID,
        -- email_campaign.id AS airtableID,
        _rootcampaign,
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = CAST(email_campaign.id AS INT)
  )
  WHERE rownum = 1
  AND _code IS NOT NULL
),
Contact AS (
  SELECT
    acc.id,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    title AS _title,
    -- email AS _email,
    phone AS _phone,
    --email_domain__c AS _domain,
    --mailingcity AS _city,
   -- mailingcountry AS _country,
    --mailingstate AS _state,
   fin.name, total_market_value_amt__c,contributing__c,

    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link
  FROM `x-marketing.pcs_salesforce.Lead` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c 
  WHERE acc.isdeleted IS FALSE 
  --AND 
  --acc.email = 'enrique.urbina@westernu.edu'
)
SELECT * EXCEPT (rownum)
FROM (
SELECT * ,  
ROW_NUMBER() OVER(
            PARTITION BY _scd_sequence,_prospectID,_campaignID,_event_type,url
            ORDER BY _timestamp
 DESC
        ) AS rownum
        FROM (

SELECT 
  engagements.*,
  airtable.* EXCEPT(id),
  contact.* EXCEPT(id),
FROM (
 SELECT * FROM open_event
  UNION ALL 
  SELECT * FROM click_event
  UNION ALL
  SELECT * FROM sent_event
  UNION ALL
  SELECT * FROM hard_bounce
  UNION ALL 
  SELECT * FROM unsubscribe
  UNION ALL 
  SELECT * FROM unique_click
) engagements 
JOIN airtable ON CAST(engagements._campaignID AS INT64) = airtable.id
LEFT JOIN contact ON engagements._prospectID = contact.id
--WHERE _campaignID = '85793'
        )
) WHERE rownum = 1;

UPDATE `x-marketing.pcs.dg_email_performance`  origin 
SET origin._remove_bot = 'True'
FROM (
SELECT _campaignID, _email, _prospectID
FROM (
SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linkid LIKE  "%DG%" THEN 1 END) AS _content_donwloaded,
SUM(CASE WHEN _linkid = 'Bot'  THEN 1 END) AS _bot
FROM `x-marketing.pcs.dg_email_performance` 
WHERE _engagement = 'Clicked' 
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) scenario
WHERE origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid LIKE  "%DG%";

UPDATE `x-marketing.pcs.dg_email_performance` origin
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
            SUM(CASE WHEN _engagement IN( 'SoftBounce','HardBounce','Block bounce',"OtherBounce") THEN 1 END) AS _hasBounced,
        FROM 
            `x-marketing.pcs.dg_email_performance`
        WHERE
            _engagement IN ('Opened', 'Clicked', 'SoftBounce','HardBounce','Block bounce',"OtherBounce")
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
AND origin._engagement IN('SoftBounce','HardBounce','Block bounce',"OtherBounce");