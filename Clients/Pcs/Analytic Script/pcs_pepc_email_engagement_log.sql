TRUNCATE TABLE `x-marketing.pcs.pepc_email_performance`;

INSERT INTO `x-marketing.pcs.pepc_email_performance` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  _email,
  _timestamp,
  _name,
  _company,
  _region,
  _description,
  _linkid,

  _utm_campaign,
  _subject,
  _salesforceCreated,
  _contentTitle,
  _screenshot, 
  _campaignSentDate,
  _utm_source, 
  _utm_medium, 
  _landingpage,
  _segment,
  _links,
  _rootcampaign,
  _senddate, _sentdate, _email_status, emailid, fromname, emailname, fromaddress, isalwayson, _trimcode, _assettitle, _subject_email, _whatwedo, campaignName, _id, _journeyname, _notes,_emailsegment,_preview,

  _title,
  _phone,
  _domain,
  _city,
  _country,
  _state,
  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  _salesforce_link,
  _contact_sdc_sequence
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,

     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,subscriberkey ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Open' 
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
  UNION ALL 
  SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
    WHERE 
   eventtype = 'Open' 
    AND 
    sendid IN (83490,84359)
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,url ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
    WHERE eventtype = 'Open' 
    AND  sendid = 137426

    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
), 
click_event AS(
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      eventdate AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = '141606' THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = '194651' THEN "Click after 2 weeks"
      WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
      WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%" OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?%" OR url LIKE "%https://image.accountsvc.com/lib/%" THEN "Content_downloaded"
      WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
      WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
      WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM (
      SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
      FROM `x-marketing.pcs_sfmc.event`
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type 
      FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks`
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
  UNION ALL 
  SELECT * EXCEPT(rownum)
  FROM (
SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      eventdate AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = '141606' THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = '194651' THEN "Click after 2 weeks"
      WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM (SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
     FROM 
      `x-marketing.pcs_sfmc.event`
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
    WHERE 
   eventtype = 'Click' 
    AND 
    sendid IN ('83490','84359')
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
       c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      eventdate AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = '141606' THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = '194651' THEN "Click after 2 weeks"
      WHEN url LIKE "%link=botclick%" THEN "Bot Click"
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM (SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
     FROM 
      `x-marketing.pcs_sfmc.event`
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
    WHERE eventtype = 'Click'
    AND  sendid = '137426'
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = 141606 THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = 194651 THEN "Click after 2 weeks"
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
  UNION ALL 
  SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Unique" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
    WHERE 
   eventtype = 'Click' 
    AND 
    sendid IN (83490,84359)
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Unique" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
    WHERE eventtype = 'Click'
    AND  sendid = 137426

    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Sent'
    
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
  UNION ALL 
   SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
    WHERE 
   eventtype = 'Sent'
    AND 
    sendid IN (83490,84359)
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,url ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
    WHERE eventtype = 'Sent'
    AND  sendid = 137426

    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
), 
hard_bounce AS (
 SELECT activity.* EXCEPT(rownum) FROM (
 SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c,  
      --categoryid, 
      --segment,
      activity.url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Other Bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event` ) activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
     WHERE  
    eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
    --AND sendid = 189711
 ) activity
  UNION ALL 
  SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
    CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Other Bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event` )  activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
   LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
    WHERE 
    eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
    AND 
    sendid IN (83490,84359)
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
    CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Other Bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,url ORDER BY eventdate DESC) AS rownum
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Soft bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event` )  activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
  LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
    WHERE  eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
    AND  sendid = 137426

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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Unsubscribe'
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
  WHERE rownum = 1
  UNION ALL 
  SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      l.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
   LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON activity.subscriberkey = l.email or activity.subscriberkey = id
    WHERE 
   eventtype = 'Unsubscribe'
    AND 
    sendid IN (83490,84359)
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
  )
WHERE rownum = 1
UNION ALL 
 SELECT * EXCEPT(rownum)
  FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      c.id AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      CASE WHEN eventtype = 'Sent' THEN "Sent"
      WHEN eventtype = 'Open' THEN "Opened" 
      WHEN eventtype = 'Click' THEN "Clicked" 
      WHEN eventtype = 'SoftBounce' THEN "SoftBounce" 
      WHEN eventtype = 'OtherBounce' THEN "OtherBounce" 
      WHEN eventtype = 'Unsubscribe' THEN "Unsubscribe" 
      WHEN eventtype = 'HardBounce' THEN "HardBounce" END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     CASE WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,url ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
  JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` l ON activity.subscriberkey = l.x18_digit_case_safe_id__c
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  c ON  --l.account_holder_email__c = c.email or 
   l.account_holder__c  = c.id
    WHERE eventtype = 'Unsubscribe'
    AND  sendid = 137426

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
  SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  CASE
    WHEN _email_segment LIKE "%PEPC 2022 Email 10 %" THEN "PEPC 2022 Email 10"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Early)%" THEN "PEPC3Early 2023 Email 11"
    WHEN _email_segment LIKE "%PEPC3 EM1 (End)%" THEN "PEPC3End 2023 Email 12"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Mid)%" THEN "PEPC3Mid 2023 Email 13"
    ELSE _email_segment
END
  AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Participant Engagement' 
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
         CASE WHEN CAST(airtable.id AS STRING) = '208483' THEN 'PEPC 2023 Email 10' ELSE _emailsegment END AS _emailsegment,
        _preview ,
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = CAST(email_campaign._campaignid AS INT)
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
    email_domain__c AS _domain,
    mailingcity AS _city,
    mailingcountry AS _country,
    mailingstate AS _state,
   fin.name, total_market_value_amt__c,contributing__c,

    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
    CAST(acc._sdc_sequence AS STRING)
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c 
  WHERE acc.isdeleted IS FALSE 
  --AND 
  --acc.email = 'enrique.urbina@westernu.edu'
)
--, combine_all AS (
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

UPDATE `x-marketing.pcs.pepc_email_performance`  origin 
SET origin._remove_bot = 'True'
FROM (
SELECT _campaignID,_contact_sdc_sequence, _prospectID
FROM (
SELECT _campaignID, _contact_sdc_sequence, _prospectID,SUM(CASE WHEN _linkid = "Content_downloaded" THEN 1 END) AS _content_donwloaded,
SUM(CASE WHEN _linkid = "Bot Click"  THEN 1 END) AS _bot
FROM `x-marketing.pcs.pepc_email_performance` 
WHERE _engagement = 'Clicked' 
--AND _prospectID IN ( '0035x000031gkEYAAY', '0035x000031fumrAAA')
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) scenario
WHERE 
origin._contact_sdc_sequence = scenario._contact_sdc_sequence 
AND 
origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid = "Content_downloaded";


UPDATE `x-marketing.pcs.pepc_email_performance` origin
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
            SUM(CASE WHEN _engagement IN( 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") THEN 1 END) AS _hasBounced,
        FROM 
            `x-marketing.pcs.pepc_email_performance`
        WHERE
            _engagement IN ('Opened', 'Clicked', 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") 
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
AND origin._engagement IN('Soft bounce','Hard bounce','Block bounce',"Other Bounce");


CREATE OR REPLACE TABLE `x-marketing.pcs.PEPC_Contribution_report_2024` AS
WITH  Channel_Partner AS (
  SELECT DISTINCT id,name FROM `x-marketing.pcs_salesforce.Account` 
) , tpa AS (
  SELECT DISTINCT id,name FROM `x-marketing.pcs_salesforce.Account` 
  ) ,sponsor AS (
  SELECT DISTINCT id,name FROM `x-marketing.pcs_salesforce.Account` 
) ,plan AS (
SELECT plan_territory__c, plan.service_territory__c, relius_reviewed_updated__c, recent_issues_stat__c, billing_contact__c, close_date__c, accountid__c, common_remitter__c, contribution_types__c, conversion_file_tested__c, plan_sponsor_call_date__c, loan_interest_rate__c, business_line__c, effective_date_of_agreement__c, primaryid__c, age__c, plan.id, plan_name__c, rtq_status__c, investment_types__c, trust_agreement__c, top_heavy__c, stagename__c, plan.name, type__c, loans__c,advisory_firm_2__c,primary_is_advisor__c hide_advisor__c, plan_advisor_id__c,program2__c, plan.program__c,plan_id__c,go_live_date__c,Service_Type__c,Regional_Sales_Person__c,acc.name as Regional_Sales_Person__c_name,inside.name AS Inside_Sales_Contact__c_name,plan.Inside_Sales_Contact__c,Sales_Agent_Lookup__c,cc.name AS Sales_Agent_Lookup__c_name,plan.Advisor__c, accs.name AS Advisor__c_name,Channel_Partner__c,accss.name AS Channel_Partner__c_name,plan.plan_sponsor__c AS Plan_Sponsor,plan.tpa__c AS TPA
FROM `x-marketing.pcs_salesforce.Plan__c` plan
LEFT JOIN `x-marketing.pcs_salesforce.User` acc ON Regional_Sales_Person__c = acc.id
LEFT JOIN `x-marketing.pcs_salesforce.User` inside ON Inside_Sales_Contact__c = inside.id
LEFT JOIN `x-marketing.pcs_salesforce.Contact` cc ON Sales_Agent_Lookup__c = cc.id
LEFT JOIN (SELECT DISTINCT id,name FROM `x-marketing.pcs_salesforce.Account` ) accs ON plan.Advisor__c = accs.id
LEFT JOIN Channel_Partner accss ON plan.Channel_Partner__c = accss.id
--LEFT JOIN sponsor  acc_sponsor ON plan.plan_sponsor__c = accss.id
--LEFT JOIN tpa acc_tpa ON plan.tpa__c= accss.id
--WHERE plan.id = 'a475x000001M9moAAC'

), fin AS (
  SELECT fin.createddate ,fin.lastmodifieddate,fin.name, total_market_value_amt__c, plan_entry_date__c, account_holder__c,acc.name AS account_holdername, account_holder_email__c, financial_account_id__c, financial_account_number__c, planid__c, new_plan__c,  planid_ssn_unique__c, ee_planstat_cdtxt__c, hire_date__c, rehire_date__c, projected_eligibility_date__c, rtq_status__c,ee_jobstatcd_txt__c,post_tax_percentage__c,roth_percentage__c,owner_percentage__c,Deferred_Pct__c,Defered_Cont_Rate_Amt__c,	Deferred_Rate_Eff_Date__c,contributing__c,roth_cont_rate_amt__c,  roth_rate_eff_date__c, roth_auto_enroll_cd__c,employer_contributions__c, employee_contributions__c, equity_allocation__c
FROM `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin
LEFT JOIN `x-marketing.pcs_salesforce.Contact` acc ON fin.account_holder__c = acc.id
), google_Sheet_fin_account AS (
  WITH email_campaign AS (
 SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  CASE
    WHEN _email_segment LIKE "%PEPC 2022 Email 10 %" THEN "PEPC 2022 Email 10"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Early)%" THEN "PEPC3Early 2023 Email 11"
    WHEN _email_segment LIKE "%PEPC3 EM1 (End)%" THEN "PEPC3End 2023 Email 12"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Mid)%" THEN "PEPC3Mid 2023 Email 13"
    WHEN CAST(_campaign_id AS STRING) = '208483' THEN 'PEPC 2023 Email 10'
    ELSE _email_segment
END
  AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Participant Engagement' 
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
)
,
airtable AS (
  SELECT 
   * EXCEPT(rownum)
  FROM (
     SELECT 
        senddate,CAST(createddate AS TIMESTAMP) AS createddate,
        airtable.id,
        _livedate AS _livedate, 
        _segment,
        ROW_NUMBER() OVER(
            PARTITION BY _segment
            ORDER BY createddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
   LEFT JOIN  email_campaign ON CAST(airtable.id AS STRING)  = email_campaign._campaignid
 )
 WHERE rownum = 1 
 --AND _segment = 'PEPC 2024 Email 1'
 AND _segment IS NOT NULL
)
--google_Sheet_fin_account AS (
  SELECT 
  _financialaccountfinancialaccountname AS financial_account__financial_account_name, 
  CAST(_emailcampaign AS NUMERIC) AS email_campaign, 
  _validation AS validation, 
  CAST(_campaignyear AS NUMERIC) AS year,
  _campaignname AS campaign_name,
  _livedate,
  financial._segment AS segment
  FROM `x-marketing.pcs_mysql.db_pepc_contribution_list` financial 
  LEFT JOIN airtable on financial._campaignname = airtable._segment
--)
),email_action AS (
    WITH email_engagement AS (
  SELECT  
  _prospectID, 
  _email, 
  _campaignID,
  --_description, 
  _contentTitle,
  _segment,
  fin.name AS Financial_Account_Financial_Account_Name ,
  total_market_value_amt__c AS  amt,
  fin.contributing__c AS c,
  CAST(SPLIT(_segment, ' ')[OFFSET(1)] AS NUMERIC) as _year,
  CAST(SPLIT(_segment, ' ')[OFFSET(3)] AS INT64) as _emails,
  COUNT(CASE WHEN _engagement = "Sent" THEN 1 END) AS _sent, 
  COUNT(CASE WHEN _engagement = "Clicked" AND _linkid = "Content_downloaded" 
  AND _remove_bot = 'True'
  THEN 1 END) AS _clicked, 

  FROM (SELECT * FROM `x-marketing.pcs.pepc_email_performance` email
  
   )email
  LEFT JOIN fin ON email._prospectID = fin.account_holder__c 
  GROUP BY 1,2,3,4,5,6,7,8,9,10
) SELECT * ,CASE 
--WHEN _prospectID = "0035x000034BprEAAS" AND _emails = 8 AND _year = 2023 Then 'Contributions'
WHEN (_emails >= 1 AND _year >= 2023 )  AND (_sent = 1 AND _clicked >= 1 ) Then 'Click-to-contributes'
WHEN (_emails >= 7 AND _year >= 2022 ) AND (_sent = 1 AND _clicked >= 1 )   Then 'Click-to-contributes' 
 WHEN (_emails < 7  AND _year = 2022) AND (_sent = 1 )   Then 'Contributions'
ELSE 'Contributions' END AS _click_to_contributes
FROM email_engagement 
--WHERE _email = "rsmiley@nooksports.com"
)
SELECT 
fin.createddate AS Financial_Account_Created_Date,
fin.lastmodifieddate AS Financial_Account_Last_Modified_Date,
fin.name AS Financial_Account_Financial_Account_Name, 
total_market_value_amt__c AS Total_Market_Value_Amt, 
CASE WHEN total_market_value_amt__c > 0 AND contributing__c = 'Yes' Then "Started Contributing"
WHEN (total_market_value_amt__c <= 0 AND contributing__c = 'Yes') OR (total_market_value_amt__c IS NULL AND contributing__c = 'Yes' ) Then "Intent to Contribute"
WHEN total_market_value_amt__c > 0 AND contributing__c = 'No' Then "Stopped Contributing"
WHEN (total_market_value_amt__c <= 0 AND  contributing__c = 'No') OR  (total_market_value_amt__c IS NULL AND contributing__c = 'No') Then "Never Contributed" End AS participant_contribution,
fin.plan_entry_date__c AS Plan_Entry_Date, 
account_holder__c,
account_holdername AS Account_Holder, 
account_holder_email__c AS Account_Holder_Email,
program__c AS Plan_Program,
stagename__c AS Plan_Stage,
plan_id__c AS Plan_id,
go_live_date__c AS Plan_go_live_date,
hire_date__c AS Plan_hire_dates,
projected_eligibility_date__c AS Projected_Eligibility_Date,
ee_jobstatcd_txt__c AS Employee_Job_Status_Code,
fin.ee_planstat_cdtxt__c AS Employee_Plan_Status_Code,
contributing__c AS Contributing,
Deferred_Pct__c AS Deferral_Percentage,
Defered_Cont_Rate_Amt__c AS Deferral_Contribution_Rate_Amount,	
Deferred_Rate_Eff_Date__c,
service_territory__c AS Plan_Plan_Territory,
Business_Line__c,
Type__c,
Service_Type__c,
Regional_Sales_Person__c_name,
Inside_Sales_Contact__c_name,
Sales_Agent_Lookup__c,
Sales_Agent_Lookup__c_name,
Advisor__c_name,
Channel_Partner__c_name,
fin.roth_cont_rate_amt__c, fin.roth_percentage__c, fin.roth_rate_eff_date__c, fin.roth_auto_enroll_cd__c,
google_Sheet_fin_account.validation,
google_Sheet_fin_account.email_campaign,
google_Sheet_fin_account.year,
google_Sheet_fin_account._livedate,
campaign_name,
email_action._prospectID,
email_action._email,
email_action._campaignID,
email_action._contentTitle,
email_action._segment,
email_action._year,
email_action._emails,
email_action._sent,
email_action._clicked,
CASE 
WHEN account_holder__c = "0035x00003PznhnAAB" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 9 THEN 'Click-to-contributes' 
WHEN email_action._email = "travisfdoss@gmail.com" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 8 THEN 'Contributions' 
WHEN account_holder__c = "0035x000034BprEAAS" AND email_action._year = 2022 AND google_Sheet_fin_account.email_campaign= 12 THEN 'Click-to-contributes' 
WHEN account_holder__c = "0035x00003WvHCXAA3" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 5 THEN 'Contributions'  
WHEN account_holder__c = "0035x000031ey1LAAQ" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 5 THEN 'Contributions' 
WHEN account_holder__c = "0035x00003VTTItAAP" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 5 THEN 'Contributions' 
WHEN account_holder__c = "0035x00003VSqQtAAL" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 5 THEN 'Contributions' 
WHEN account_holder__c = "0035x00003S0OZHAA3" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 5 THEN 'Contributions' 
WHEN account_holder__c = "0035x000031eyAoAAI" AND email_action._year = 2023 AND google_Sheet_fin_account.email_campaign= 6 THEN 'Contributions' 
WHEN account_holder__c = "0035x00003VSpQBAA1" AND email_action._year = 2024 AND google_Sheet_fin_account.email_campaign= 1 THEN 'Contributions' 
WHEN email_action._click_to_contributes IS NULL THEN 'Contributions' ELSE email_action._click_to_contributes END AS _click_to_contributes,
Plan_Sponsor,TPA,
acc_sponsor.name AS Plan_Sponsor_name,
tpa.name AS TPA_name,
employer_contributions__c, employee_contributions__c, equity_allocation__c,segment
-- EXCEPT(Financial_Account_Financial_Account_Name
--,amt,c)
FROM fin 
LEFT JOIN plan ON plan.id = fin.new_plan__c
JOIN google_Sheet_fin_account ON fin.name= google_Sheet_fin_account.financial_account__financial_account_name
LEFT JOIN email_action ON email_action._prospectID = fin.account_holder__c AND Financial_Account_Financial_Account_Name = google_Sheet_fin_account.financial_account__financial_account_name AND google_Sheet_fin_account.email_campaign = email_action._emails
 AND email_action._year = google_Sheet_fin_account.year
LEFT JOIN sponsor  acc_sponsor ON Plan_Sponsor = acc_sponsor.id
LEFT JOIN tpa  ON plan.TPA = tpa.id
--WHERE email_action._emails = "travisfdoss@gmail.com"
/*WHERE 
account_holder__c IN ('0035x00003PznhnAAB',
'0035x00003PKW7OAAX',
'0035x00003Q0JoIAAV',
'0035x00003RzPo9AAF',
'0035x000031fXhXAAU',
'0035x0000312ZelAAE',
'0035x00003TndOCAAZ',
'0035x00003S2hZIAAZ',
'0035x00003OqpAsAAJ',
'0035x00003QbgzOAAR',
'0035x00003QbfmfAAB'
) 
--AND
--google_Sheet_fin_account.email_campaign = 10*/
--WHERE google_Sheet_fin_account.email_campaign = 9 AND
--google_Sheet_fin_account.year  = 2022 
ORDER BY fin.createddate DESC;


Create OR REPLACE TABLE pcs.recoring_revenue_pepc AS
SELECT _financialaccountname AS Financial_Account_Financial_Account_Name, _campaignname AS campaign_name, CAST(_contributionyear AS NUMERIC) AS _year ,  _contributiondate AS _date FROM `x-marketing.pcs_mysql.db_pepc_reoccurring_annual_revenue`;


TRUNCATE TABLE `x-marketing.pcs.pesc_email_performance`;

INSERT INTO `x-marketing.pcs.pesc_email_performance` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  _email,
  _timestamp,
  _name,
  _company,
  _region,
  _description,
  _ga_utm_content, _ga_utm_source, _ga_utm_medium, _ga_content_downloaded ,
  _linkid,


  _utm_campaign,
  _subject,
  _salesforceCreated,
  _contentTitle,
  _screenshot, 
  _campaignSentDate,
  _utm_source, 
  _utm_medium, 
  _landingpage,
  _segment,
  _links,
  _rootcampaign,
  _senddate, _sentdate, _email_status, emailid, fromname, emailname, fromaddress, isalwayson, _trimcode, _assettitle, _subject_email, _whatwedo, campaignName, _id, _journeyname, _notes,_emailsegment,_preview,

  _title,
  _phone,
  _domain,
  _city,
  _country,
  _state,
  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  roth_cont_rate_amt__c, roth_percentage__c, roth_rate_eff_date__c, roth_auto_enroll_cd__c,defered_cont_rate_amt__c, deferred_pct__c, deferred_rate_eff_date__c,program__c,financial_account_id,
  ownerid,ownername,
  _salesforce_link,
  _contact_sdc_sequence
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
      account_name__c AS _companyname, 
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
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
SELECT * EXCEPT(rownum)
 FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      eventdate AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = '141606' THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = '194651' THEN "Click after 2 weeks"
      WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
       WHEN url LIKE "%https://pcsretirement.accountsvc.com/blog%"  OR url LIKE "%https://pcsretirement.accountsvc.com/wlblog%" OR url LIKE "%invlink.com%" OR url LIKE "%https://www.pcsretirement.com/login%" OR url LIKE "%image.accountsvc.com/lib/fe33117171640479771276/m/1/PCS-Five-Tips-To-Protect-Your-Personal-Online-Data-Asset.pdf%" OR url LIKE "%image.accountsvc.com/lib/fe33117171640479771276/m/1/Participant+FAQs+Beneficiaries-PCS.pdf?%" THEN "Content_downloaded"
       
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM (SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
     FROM 
      `x-marketing.pcs_sfmc.event`
      
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
      UNION ALL
      SELECT _sdc_sequence,_subscriberkey, _campaignid,
 PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', _clickthroughtime)  AS click_through_time, _linkclicked  AS link_clicked, 'Click' AS _event_type  FROM `x-marketing.pcs_mysql.db_pcs_campaign_clicks_patches`
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click' 
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
 )
WHERE rownum = 1

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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = 141606 THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = 194651 THEN "Click after 2 weeks"
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      account_name__c AS _companyname, 
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
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
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
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Other Bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event`
   )  activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
    WHERE eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
  
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
      account_name__c AS _companyname, 
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
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
,web AS (
  SELECT 
    SAFE_CAST(stream_id AS INT64) AS  _scd_sequence,
    user.value.string_value AS _prospectID,
    SPLIT(SUBSTR(traffic_source.name, STRPOS(traffic_source.name, '?j=') + 3), '&')[ORDINAL(1)] AS _campaignid,
    INITCAP(platform) AS _event_type,
    email AS _email,
    PARSE_TIMESTAMP('%Y%m%d',event_date) AS _timestamp , 
    CONCAT(firstname, ' ', lastname ) AS _name, 
    account_name__c AS _companyname, 
    territory__c, 
    --categoryid, 
    --segment,
    max((SELECT value.string_value FROM unnest(event_params) WHERE  key = 'page_location')) AS   url,
    traffic_source.source AS utm_source,
    collected_traffic_source.manual_content AS utm_content, 
    traffic_source.medium AS utm_medium, 
    REGEXP_REPLACE(collected_traffic_source.manual_campaign_name, r'\?j=\d+', '') AS content_downloaded,
    max((SELECT value.string_value FROM unnest(event_params) WHERE  key = 'page_title')) AS _linked_clicked,
    FROM `x-marketing.analytics_411351491.events_*` activity,UNNEST(event_params) event,UNNEST (user_properties) user
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ user.value.string_value = id
    GROUP BY 1,2,3,4,5,6,7,8,9
    ,11,12,13,14
)
, 
email_campaign AS (
  SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  CASE
    WHEN _email_segment LIKE "%PEPC 2022 Email 10 %" THEN "PEPC 2022 Email 10"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Early)%" THEN "PEPC3Early 2023 Email 11"
    WHEN _email_segment LIKE "%PEPC3 EM1 (End)%" THEN "PEPC3End 2023 Email 12"
    WHEN _email_segment LIKE "%PEPC3 EM1 (Mid)%" THEN "PEPC3Mid 2023 Email 13"
    WHEN CAST(_campaign_id AS STRING) = '208483' THEN 'PEPC 2023 Email 10'
    ELSE _email_segment
END
  AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign IN( 'Participant Education Series','Adhoc - Cantor')
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
      _emailsegment,_preview, 
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = SAFE_CAST(_campaignid AS INT)
  )
  WHERE rownum = 1
  AND _code IS NOT NULL
),
Contact AS (
  SELECT
    acc.id,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    acc.title AS _title,
    -- email AS _email,
    acc.phone AS _phone,
    email_domain__c AS _domain,
    mailingcity AS _city,
    mailingcountry AS _country,
    mailingstate AS _state,
   fin.name, 
   total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,
   fin.program__c,
   fin.id AS financial_account_id,fin.ownerid,j.name,
  CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
  CAST(acc._sdc_sequence AS STRING)
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN (SELECT * EXCEPT (rownum)
FROM (
SELECT fin.program__c,fin.name,fin.id,
   fin.id AS financial_account_id,fin.ownerid,total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,account_holder__c,
    ROW_NUMBER() OVER(
            PARTITION BY account_holder__c
            ORDER BY lastmodifieddate DESC
        ) AS rownum

FROM `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin
WHERE isdeleted IS FALSE
) WHERE rownum = 1 ) fin ON acc.id =  fin.account_holder__c
  LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = fin.ownerid 
  WHERE acc.isdeleted IS FALSE 
  --AND acc.id = '0035x00003PznPLAAZ'
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
) WHERE rownum = 1
UNION ALL 
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
 SELECT * FROM web 
) engagements 
LEFT JOIN airtable ON CAST(engagements._campaignID AS STRING) = CAST(airtable.id AS STRING)
LEFT JOIN contact ON engagements._prospectID = contact.id
--WHERE _campaignID = '85793'
        )
) WHERE rownum = 1;


UPDATE `x-marketing.pcs.pesc_email_performance`  origin 
SET origin._remove_bot = 'True'
FROM (
SELECT _campaignID,_contact_sdc_sequence, _prospectID
FROM (
SELECT _campaignID, _contact_sdc_sequence, _prospectID,SUM(CASE WHEN _linkid = "Content_downloaded" THEN 1 END) AS _content_donwloaded,
SUM(CASE WHEN _linkid = "Bot Click"  THEN 1 END) AS _bot
FROM `x-marketing.pcs.pesc_email_performance` 
WHERE _engagement = 'Clicked' 
--AND _prospectID IN ( '0035x00003T8boPAAR', '0035x000031fsaIAAQ')
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) scenario
WHERE 
origin._contact_sdc_sequence = scenario._contact_sdc_sequence 
AND 
origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid = "Content_downloaded";

UPDATE `x-marketing.pcs.pesc_email_performance` origin
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
            SUM(CASE WHEN _engagement IN( 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") THEN 1 END) AS _hasBounced,
        FROM 
            `x-marketing.pcs.pesc_email_performance`
        WHERE
            _engagement IN ('Opened', 'Clicked', 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") 
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
AND origin._engagement IN('Soft bounce','Hard bounce','Block bounce',"Other Bounce");


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
   _senddate, 
   _sentdate, 
   _email_status, 
   emailid, 
   fromname, 
   emailname, 
   fromaddress, 
   isalwayson, 
   _trimcode, 
   _assettitle, 
   _subject_email, 
   _whatwedo, 
   campaignName, 
   _id, 
   _journeyname, 
   _notes,
   _emailsegment,
   _preview,

  _title,
  _phone,

  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  _salesforce_link,
  _remove_bot
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
    JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`) l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
    WITH smfc_event AS (
    SELECT 
    _sdc_sequence,
    subscriberkey,
    CAST(sendid AS STRING) AS sendid,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, 
    url,
    eventtype
    FROM 
    `x-marketing.pcs_sfmc.event`
    UNION ALL
    SELECT 
    _sdc_sequence, 
    subscriberkey, 
    jobid,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),
    url , 
    'Click' AS _event_type
    FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
    ), 
    linked_click AS (
    
    WITH email_campaign AS (
   SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment 
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Demand Generation'
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _code,_trimcode, _screenshot, _assettitle, _subject, _preview AS _whatwedo, _campaignname AS campaignName, 
        _id, 
        _livedate AS _livedate, 
        _utm_source, _utm_medium, _landingpage,_journeyname,_email_segment,
        _code AS _type,
        ROW_NUMBER() OVER(PARTITION BY emailname,airtable.id,emailid,_email_segment ORDER BY senddate DESC) AS _rownum
        FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
        JOIN  email_campaign ON airtable.id  = SAFE_CAST(_campaignid AS INT64)
        )
        WHERE _rownum = 1
        )SELECT l._sdc_sequence,subscriberkey ,CAST(k.id AS STRING) AS sendid,clickdate,url,
        'Click' AS eventtype,
        --ROW_NUMBER() OVER(PARTITION BY subscriberkey, l.emailname, url,linkname,k.id ORDER BY clickdate DESC ) AS rownum
        FROM (

        SELECT *
        FROM (
          SELECT  _sdc_sequence,subscriberkey, emailname, url, sf, linkname,id, PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate) AS clickdate,
          CASE WHEN url LIKE '%Job_ID=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'Job_ID=') + 7), '&')[ORDINAL(1)] 
          WHEN   url LIKE '%j=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'j=') + 3), '&')[ORDINAL(1)] END  AS sendid
          FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks` 
          UNION ALL 
          SELECT  _sdc_sequence,subscriberkey, emailname, url, sf, linkname,id,PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),
          CASE WHEN url LIKE '%Job_ID=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'Job_ID=') + 7), '&')[ORDINAL(1)] 
          WHEN   url LIKE '%j=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'j=') + 3), '&')[ORDINAL(1)] END  AS sendid 
          FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks___Nurture___Mod_Hot` 
          UNION ALL 
          SELECT  _sdc_sequence,subscriberkey, emailname, url, sf, linkname,id, PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),
          CASE WHEN url LIKE '%Job_ID=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'Job_ID=') + 7), '&')[ORDINAL(1)] 
          WHEN   url LIKE '%j=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'j=') + 3), '&')[ORDINAL(1)] END  AS sendid
          FROM `x-marketing.pcs_sfmc.data_extension_Link_Clicks___Nurture`
          UNION ALL 
          SELECT  _sdc_sequence,lead_id AS subscriberkey,email_name AS emailname, url, sf, link_name AS linkname,CONCAT(lead_id,link_name,click_date) AS id, PARSE_TIMESTAMP('%m/%d/%Y %T %p',click_date),
          CASE WHEN url LIKE '%Job_ID=%' THEN SPLIT(SUBSTR(url, STRPOS(url, 'Job_ID=') + 7), '&')[ORDINAL(1)] ELSE  
          SPLIT(SUBSTR(url, STRPOS(url, 'j=') + 3), '&')[ORDINAL(1)] END  AS sendid 
          FROM `x-marketing.pcs_sfmc.data_extension_DG_Nurture_Email_Link_Clicks`
          ) 
          ) l
          LEFT JOIN 
          airtable k ON sendid = CAST(k.id AS STRING)
    ),all_combine AS (
        SELECT * FROM smfc_event
    UNION ALL
    SELECT * FROM linked_click
)
 SELECT * 

  FROM (   SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      email AS _email,
      eventdate AS _timestamp,
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
 WHEN activity.url LIKE  "%click.accountsvc.com/unsub_center.aspx?%" OR  activity.url LIKE  "%https://PCSRetirement.accountsvc.com/2clickunsub?%"THEN "Unsubscribe"
 WHEN activity.url LIKE    "%click.accountsvc.com/subscription_center.aspx?%" THEN "Subscription Center"
  
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy'
  WHEN activity.url LIKE "%utm_content=cta_button01%" THEN "DG-EM-01-LP-A (cta_button01)"
  WHEN activity.url LIKE "%utm_content=cta_img%" THEN "DG-EM-01-LP-B (cta_img)"
  WHEN activity.url LIKE "%utm_content=cta_text01%" THEN "DG-EM-01-LP-C (cta_text01)"
  WHEN activity.url LIKE "%utm_content=cta_button02%" THEN "DG-EM-01-LP-D (cta_button02)"
   WHEN activity.url LIKE "%utm_content=cta_text%" THEN "DG-EM-01-LP-E (cta_text)"
    WHEN activity.url LIKE "%utm_term=TOFU-E1A-LP-Logo&utm_content=50462%" OR activity.url LIKE "%utm_term=&utm_content=50462%" OR activity.url LIKE "%utm_term=TOFU-E1B-LP-Logo&utm_content=50463%" OR activity.url LIKE "%utm_term=&utm_content=50463&utm_id=b9ec0583-ee17-4fe2-9644-6f473be85519%"OR activity.url LIKE "%utm_content=50464%" OR activity.url LIKE  "%utm_term=&utm_content=50464&utm_id=b9ec0583-ee17-4fe2-9644-6f473be85519%" OR activity.url LIKE  "%utm_term=TOFU-E2A-LP-Logo&utm_content=50464%"
OR activity.url LIKE  "%utm_term=TOFU-E2B-LP-Logo&utm_content=50466%"
OR activity.url LIKE  "%utm_term=&utm_content=50466&utm_id=b9ec0583-ee17-4fe2-9644-6f473be85519%"
OR activity.url LIKE  "%utm_term=MOFU-E1-LP-Logo&utm_content=50465%"
OR activity.url LIKE  "%utm_term=&utm_content=50465&utm_id=5898f2f4-1995-45a0-9ef0-58675650920c%"
OR activity.url LIKE  "%tm_term=MOFU-E2-LP-Logo&utm_content=50467%"
OR activity.url LIKE  "%utm_content=50467&utm_id=5898f2f4-1995-45a0-9ef0-58675650920c%"
OR activity.url LIKE  "%utm_term=MOFU-E3-LP-Logo&utm_content=50547%"
OR activity.url LIKE  "%utm_term=&utm_content=50547&utm_id=5898f2f4-1995-45a0-9ef0-58675650920c%" THEN "DG-EM-01-LP-E (cta)"
  WHEN activity.url LIKE "%content_downloaded=%" THEN "Content_downloaded"
  WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  --WHEN linkclick.url LIKE '%PCSRetirement.accountsvc.com%' OR linkclick.url LIKE '%pcsretirement.accountsvc.com%' THEN 'DG-EM-01-LP'
   ELSE 'Empty' END AS _linked_clicked,
  FROM all_combine activity
  JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`) l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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

  ORDER BY email asc
  ) 
  --WHERE 
  --_linked_clicked = "DG-EM-01-LP-B (cta_img)" AND
   --_rownum = 1
  ORDER BY _timestamp DESC

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
  
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy'
  WHEN activity.url LIKE "%utm_content=cta_button01%" THEN "DG-EM-01-LP-A (cta_button01)"
  WHEN activity.url LIKE "%utm_content=cta_img%" THEN "DG-EM-01-LP-B (cta_img)"
  WHEN activity.url LIKE "%utm_content=cta_text01%" THEN "DG-EM-01-LP-C (cta_text01)"
  WHEN activity.url LIKE "%utm_content=cta_button02%" THEN "DG-EM-01-LP-D (cta_button02)"
   WHEN activity.url LIKE "%utm_content=cta_text%" THEN "DG-EM-01-LP-E (cta_text)"
  WHEN activity.url LIKE "%content_downloaded=%" THEN "Content_downloaded"
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  --WHEN linkclick.url LIKE '%PCSRetirement.accountsvc.com%' OR linkclick.url LIKE '%pcsretirement.accountsvc.com%' THEN 'DG-EM-01-LP'
   ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`)  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
     JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`) l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  ELSE bounce_category END AS _event_type,
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
    FROM  `x-marketing.pcs_sfmc.event`  activity
    JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`)    l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    JOIN (
     SELECT bounce_category, lead_id, bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name  FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce`
    UNION ALL 
    SELECT bounce_category, lead_id, bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name  FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture___Mod_Hot`
    UNION ALl 
    SELECT bounce_category, lead_id, bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name  FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture`
    UNION ALL 
    SELECT bounce_category, lead_id, bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name  FROM `x-marketing.pcs_sfmc.data_extension_DG_Nurture_Email_Bounces`
    UNION ALL 
    SELECT bounce_category, lead_id, bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name  FROM `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` 
    )  Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
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
    JOIN (SELECT id,email,firstname,lastname, company,territory__c, state 
FROM `x-marketing.pcs_salesforce.Lead`
UNION ALL 
SELECT id,email,firstname,lastname, account_name__c,territory__c, dd_home_state__c
FROM `x-marketing.pcs_salesforce.Contact`)  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  _email_segment AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Demand Generation'
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
        _emailsegment,_preview, 
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = CAST(_campaignid AS INT)
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
  UNION ALL 
  SELECT
    acc.id,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    title AS _title,
    -- email AS _email,
    phone AS _phone,
    -- email_domain__c AS _domain,
    -- mailingcity AS _city,
    -- mailingcountry AS _country,
    -- mailingstate AS _state,
   fin.name, 
   total_market_value_amt__c,
   contributing__c,

    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,

  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c 
  WHERE acc.isdeleted IS FALSE
), combine_all AS (
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
) WHERE rownum = 1
) ,_isBot AS ( 
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
 SELECT origin.*,_isBot
FROM combine_all origin 
LEFT JOIN _isBot scenarios ON ((origin._email = scenarios._email 
AND origin._campaignID = scenarios._campaignID 
AND origin._prospectID = scenarios._prospectID
AND origin._event_type = "Clicked" AND _linked_clicked LIKE  "%DG-EM%") OR (origin._email = scenarios._email 
AND origin._campaignID = scenarios._campaignID 
AND origin._prospectID = scenarios._prospectID
AND origin._event_type = "Clicked" AND _linked_clicked LIKE  "Content_downloaded"));


TRUNCATE TABLE `x-marketing.pcs.selling_season_email_performance`;

INSERT INTO `x-marketing.pcs.selling_season_email_performance` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  _email,
  _timestamp,
  _name,
  _company,
  _region,
  _description,
  _linkid,


  _utm_campaign,
  _subject,
  _salesforceCreated,
  _contentTitle,
  _screenshot, 
  _campaignSentDate,
  _utm_source, 
  _utm_medium, 
  _landingpage,
  _segment,
  _links,
  _rootcampaign,
   _senddate, 
   _sentdate, 
   _email_status, 
   emailid, 
   fromname, 
   emailname, 
   fromaddress, 
   isalwayson, 
   _trimcode, 
   _assettitle, 
   _subject_email, 
   _whatwedo, 
   campaignName, 
   _id, 
   _journeyname, 
   _notes,
   _emailsegment,
   _preview,

  _title,
  _phone,
  _domain,
  _city,
  _country,
  _state,
  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  roth_cont_rate_amt__c, 
  roth_percentage__c, 
  roth_rate_eff_date__c, 
  roth_auto_enroll_cd__c,
  defered_cont_rate_amt__c, 
  deferred_pct__c, 
  deferred_rate_eff_date__c,
  program__c,
  financial_account_id,
  ownerid,ownername,
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source ,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,subscriberkey ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      eventdate AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = '141606' THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = '194651' THEN "Click after 2 weeks"
      WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
       WHEN url LIKE "%www.pcsretirement.com/about-us/sales-team%" OR url LIKE "%image.accountsvc.com/lib/fe33117171640479771276/m/1/Retirement-Plan-Tax-Credits-for-Small-Employers.pdf?%" 
       OR url LIKE "%www.pcsretirement.com/about-us/sales-team#team=employer&region=midatlantic?%" 
       OR url LIKE "%www.pcsretirement.com/about-us/sales-team#team=employer&region=northeast%"  
       OR url LIKE "%about-us/sales-team#%"  THEN "Content_downloaded"
       
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      --ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
    FROM (SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
     FROM 
      `x-marketing.pcs_sfmc.event`
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
    ORDER BY email asc
 -- )
  --WHERE rownum = 1

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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = 141606 THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = 194651 THEN "Click after 2 weeks"
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY email,sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    LEFT JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      CONCAT(firstname, ' ', lastname ) AS _name, 
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
      '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Other Bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event` ) activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
   LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
     WHERE  
    eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
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
      account_name__c AS _companyname, 
      territory__c, 
      --categoryid, 
      --segment,
      url,
      -- '' AS utm_source,
      -- '' AS utm_content, 
      -- '' AS utm_medium, 
      -- '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    JOIN `x-marketing.pcs_salesforce.Contact`  l ON /*activity.subscriberkey = l.email or*/ activity.subscriberkey = id
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
  SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  _email_segment AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Selling Season'
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
        _emailsegment,_preview, 
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = CAST(_campaignid AS INT)
  )
  WHERE rownum = 1
  AND _code IS NOT NULL
),
Contact AS (
  SELECT
    acc.id,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    acc.title AS _title,
    -- email AS _email,
    acc.phone AS _phone,
    email_domain__c AS _domain,
    mailingcity AS _city,
    mailingcountry AS _country,
    mailingstate AS _state,
   fin.name, 
   total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,
   fin.program__c,
   fin.id AS financial_account_id,fin.ownerid,j.name,
  CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c
  LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = fin.ownerid 
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


UPDATE `x-marketing.pcs.selling_season_email_performance`  origin 
SET origin._remove_bot = 'True'
FROM (
SELECT _campaignID, _email, _prospectID
FROM (
SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linkid <> "Bot Click"  THEN 1 END) AS _content_donwloaded,
SUM(CASE WHEN _linkid = "Bot Click"  THEN 1 END) AS _bot
FROM `x-marketing.pcs.selling_season_email_performance` 
WHERE _engagement = 'Clicked' 
GROUP BY 1,2,3
) WHERE _bot IS NULL 
--AND _content_donwloaded IS NOT NULL
) scenario
WHERE origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid =  "Content_downloaded";

TRUNCATE TABLE  `x-marketing.pcs.plan_sponsor_email_performance_log`;
INSERT INTO `x-marketing.pcs.plan_sponsor_email_performance_log` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  
  _timestamp,
  
  
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
   _senddate, 
   _sentdate, 
   _email_status, 
   emailid, 
   fromname, 
   emailname, 
   fromaddress, 
   isalwayson, 
   _trimcode, 
   _assettitle, 
   _subject_email, 
   _whatwedo, 
   campaignName, 
   _id, 
   _journeyname, 
   _notes,
   _emailsegment,
   _preview,

  _title,
  _phone,

  Financial_Account_Financial_Account_Name,
  Total_Market_Value_Amt,
  contributing__c,
  _salesforce_link,_employees, _industry,
  _email,
  _name,
  _region,
  _remove_bot
)
WITH open_event AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Opened' AS _event_type,
      --email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      --CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      --territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY sendid,subscriberkey ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Open' 
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/

  )
  WHERE rownum = 1

)
, click_event AS(
  SELECT 
    * EXCEPT(rownum)
    FROM (
      SELECT
        activity._sdc_sequence AS _scd_sequence,
        subscriberkey AS _prospectID,
        CAST(sendid AS STRING) AS _campaignID,
        'Clicked' AS _event_type,
        --email AS _email,
        eventdate AS _timestamp,
        --CONCAT(firstname, ' ', lastname ) AS _name, 
        --account_name__c AS _companyname, 
        --territory__c, 
        --categoryid, 
        --segment,
        url,
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
        ROW_NUMBER() OVER(PARTITION BY sendid,activity.subscriberkey,url ORDER BY eventdate DESC ) AS rownum
        FROM (
          SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
          FROM `x-marketing.pcs_sfmc.event`
          UNION ALl
          SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type 
          FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
          UNION ALL 
          SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
          FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
          UNION ALl
          SELECT _sdc_sequence, lead_id AS subscriberkey, REGEXP_EXTRACT(url,r'Job_ID=([0-9]+)') AS job_i,PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
          FROM `x-marketing.pcs_sfmc.data_extension_Plan_Sponsor_Email_Link_Clicks` 
          ) activity
          WHERE eventtype = 'Click'
  )
 WHERE rownum = 1

), unique_click AS (
   SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Unique' AS _event_type,
      --email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      --CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      --territory__c, 
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
      ROW_NUMBER() OVER(PARTITION BY sendid,activity.subscriberkey ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Click'
    --AND sendid = 189711
    --AND ( subscriberkey  <> "0035x000031fkI1AAI" AND sendid <> 141606)
    /*AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
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
      --email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      --CONCAT(firstname, ' ', lastname ) AS _name, 
     -- account_name__c AS _companyname, 
      --territory__c, 
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
    WHERE eventtype = 'Sent'
    
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
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
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  ELSE bounce_category END AS _event_type,
      --email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      --CONCAT(firstname, ' ', lastname ) AS _name, 
    --  account_name__c AS _companyname, 
      --territory__c, 
      --categoryid, 
      --segment,
      url,
     '' AS utm_source ,
       '' AS utm_content, 
      '' AS utm_medium, 
       '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM  `x-marketing.pcs_sfmc.event`  activity
    
    JOIN (
    SELECT bounce_category, lead_id,  bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name, _customobjectkey FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce`
    UNION ALL 
    SELECT bounce_category, lead_id,  bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name, _customobjectkey FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture___Mod_Hot`
    UNION ALl 
    SELECT bounce_category, lead_id,  bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name, _customobjectkey FROM  `x-marketing.pcs_sfmc.data_extension_Email_Bounce___Nurture`
    UNION ALl 
    SELECT bounce_category, lead_id,  bounce_subcategory, bounce_date, bounce_category_id, bounce_subcategory_id, categoryid, email_name, _customobjectkey FROM `x-marketing.pcs_sfmc.data_extension_Plan_Sponsor_Email_Bounces` 
    )  Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
  WHERE eventtype IN ('HardBounce','OtherBounce','SoftBounce') 
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
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
      ---email AS _email,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      --CONCAT(firstname, ' ', lastname ) AS _name, 
    --  account_name__c AS _companyname, 
      --territory__c, 
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
    WHERE eventtype = 'Unsubscribe'
    --AND sendid = 7899
    /*AND subscriberkey NOT LIKE '%pcsretirement.com%' 
    AND subscriberkey NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%pcsretirement.com%' 
    AND email NOT LIKE '%2x.marketing%'*/
  )
  WHERE rownum = 1
)
, 
email_campaign AS (
  SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  _email_segment AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = "Plan Sponsor Demand Gen"
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
         _emailsegment,_preview, 
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = SAFE_CAST(_campaignid AS INT)
    --WHERE airtable.id = 224947
  )
  WHERE rownum = 1
  --AND _code IS NOT NULL
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
   fin.name, 
   total_market_value_amt__c,
   contributing__c,
    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
    CAST(numberofemployees AS STRING), 
    industry,
    email AS _email,
    CONCAT(firstname, ' ', lastname ) AS _name,
     territory__c, 
  FROM `x-marketing.pcs_salesforce.Lead` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c 
  WHERE acc.isdeleted IS FALSE 

  UNION ALL 
  SELECT
    acc.id,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    title AS _title,
    -- email AS _email,
    phone AS _phone,
    -- email_domain__c AS _domain,
    -- mailingcity AS _city,
    -- mailingcountry AS _country,
    -- mailingstate AS _state,
   fin.name, 
   total_market_value_amt__c,
   contributing__c,

    CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
    '' AS numberofemployees,
    '' industry,
    email AS _email,
    CONCAT(firstname, ' ', lastname ) AS _name,
     territory__c, 
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c 
  WHERE acc.isdeleted IS FALSE
  --AND 
  --acc.email = 'enrique.urbina@westernu.edu'
), combine_all AS (
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
) WHERE rownum = 1
),_remove_bot AS (
   SELECT _campaignID, _email, _prospectID,'True' AS _remove_bot  
FROM (
SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linked_clicked
 LIKE "%DG-EM%" THEN 1
WHEN _linked_clicked
 = "Content_downloaded" THEN 1  END) AS _content_donwloaded,
SUM(CASE WHEN _linked_clicked
 = "Bot"  THEN 1 END) AS _bot
FROM combine_all 
WHERE _event_type = 'Clicked' 
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) select origin.* ,_remove_bot 
FROM combine_all origin
LEFT JOIN _remove_bot scenario ON (origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._event_type = "Clicked" AND _linked_clicked LIKE "%DG-EM%") OR (origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._event_type = "Clicked" AND _linked_clicked LIKE "Content_downloaded")
;

TRUNCATE TABLE `x-marketing.pcs.primerica_email_performance`;
INSERT INTO `x-marketing.pcs.primerica_email_performance` (  
  _sdc_sequence,
  _prospectID,
  _campaignID,
  _engagement,
  
  _timestamp,

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
   _senddate, 
   _sentdate, 
   _email_status, 
   emailid, 
   fromname, 
   emailname, 
   fromaddress, 
   isalwayson, 
   _trimcode, 
   _assettitle, 
   _subject_email, 
   _whatwedo, 
   campaignName, 
   _id, 
   _journeyname, 
   _notes,
   _emailsegment,
   _preview,

  _title,
  _email,
  _phone,
  _name,
  _company,
  _region,
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
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
     '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Open' 
  )
  WHERE rownum = 1

), 
click_event AS(
SELECT * EXCEPT(rownum)
 FROM (
   SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Clicked' AS _event_type,
      eventdate AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
      CASE 
      WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
      WHEN activity.url LIKE  "%click.accountsvc.com/unsub_center.aspx?%" THEN "Unsubscribe"
 WHEN activity.url LIKE    "%click.accountsvc.com/subscription_center.aspx?%" THEN "Subscription Center"
  WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  --WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy'
  WHEN activity.url LIKE "%utm_content=cta_button01%" THEN "DG-EM-01-LP-A (cta_button01)"
  WHEN activity.url LIKE "%utm_content=cta_img%" THEN "DG-EM-01-LP-B (cta_img)"
  WHEN activity.url LIKE "%utm_content=cta_text01%" THEN "DG-EM-01-LP-C (cta_text01)"
  WHEN activity.url LIKE "%utm_content=cta_button02%" THEN "DG-EM-01-LP-D (cta_button02)"
   WHEN activity.url LIKE "%utm_content=cta_text%" THEN "DG-EM-01-LP-E (cta_text)"
    WHEN activity.url LIKE "%utm_content=50462%" OR activity.url LIKE "%utm_content=50463%" OR activity.url LIKE "%utm_content=50464%" OR activity.url LIKE "%utm_content=50465%"OR activity.url LIKE "%utm_content=50467%" OR activity.url LIKE "%utm_content=50547%" OR activity.url LIKE "%utm_content=504%" THEN "DG-EM-01-LP-E"
  WHEN activity.url LIKE "%content_downloaded=%"  THEN "Content_downloaded"
       
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid,url ORDER BY eventdate DESC ) AS rownum
    FROM (SELECT _sdc_sequence,subscriberkey,CAST(sendid AS STRING) AS sendid,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS eventdate, url,eventtype
     FROM 
      `x-marketing.pcs_sfmc.event`
      
      UNION ALL
      SELECT _sdc_sequence, subscriberkey, jobid,  PARSE_TIMESTAMP('%m/%d/%Y %T %p',clickdate),url , 'Click' AS _event_type FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks` 
      UNION ALL
      SELECT _sdc_sequence,_subscriberkey, _campaignid,
 PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', _clickthroughtime)  AS click_through_time, _linkclicked  AS link_clicked, 'Click' AS _event_type  FROM `x-marketing.pcs_mysql.db_pcs_campaign_clicks_patches`
      UNION ALL 
      SELECT _sdc_sequence, lead_id AS subscriberkey, CAST(job_id AS STRING),PARSE_TIMESTAMP('%m/%d/%Y %T %p',	click_date),url , 'Click' AS _event_type
      FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`  
      
      
       )  activity
    WHERE eventtype = 'Click' 
 )
WHERE rownum = 1

), unique_click AS (
   SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _scd_sequence,
      subscriberkey AS _prospectID,
      CAST(sendid AS STRING) AS _campaignID,
      'Unique' AS _event_type,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
      CASE WHEN subscriberkey  = "0035x000031fkI1AAI" AND sendid = 141606 THEN "Click after 2 weeks"
      WHEN subscriberkey  = "0035x00003TnI00AAF" AND sendid = 194651 THEN "Click after 2 weeks"
       WHEN url LIKE "%https://www.pcsretirement.com/login?content_downloaded=%"  OR url LIKE "%https://www.pcsretirement.com/login/Register/Search?content_downloaded=%" OR url LIKE "%http://www.pcsretirement.com/login?content_downloaded%"THEN "Content_downloaded"
       WHEN url LIKE "%link=botclick%" THEN "Bot Click" 
         WHEN activity.url LIKE '%view.accountsvc.com%' THEN 'View as a Web Page'
  WHEN activity.url LIKE '%pcsretirement.com%' THEN 'PCS'
  WHEN activity.url LIKE '%pcsretirement-delivery%' THEN 'Privacy Policy' ELSE 'Empty' END AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY activity.subscriberkey,sendid ORDER BY eventdate DESC ) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Click'

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
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Sent'
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
      CASE WHEN bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
  WHEN bounce_category = 'Unknown bounce' THEN 'Soft bounce' 
  WHEN bounce_category IS NULL THEN eventtype
  ELSE bounce_category END AS _event_type,
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
      '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM (SELECT * EXCEPT (eventtype), 
    CASE WHEN eventtype = 'HardBounce' THEN "Hard bounce"
   WHEN eventtype = 'OtherBounce' THEN "Other Bounce" 
   WHEN eventtype = 'SoftBounce' THEN "Soft bounce"  END AS eventtype FROM  `x-marketing.pcs_sfmc.event`
   )  activity
    LEFT JOIN `x-marketing.pcs_sfmc.data_extension_Email_Bounce___All_Campaigns` Bounce ON activity.subscriberkey = Bounce.lead_id AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p' ,Bounce.bounce_date )AS DATE)
    WHERE eventtype IN ("Hard bounce","Other Bounce" ,"Soft bounce") 
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
      PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ",eventdate) AS _timestamp,
      url,
       '' AS utm_source ,
       '' AS utm_content, 
       '' AS utm_medium, 
      '' AS content_downloaded,
       '' AS _linked_clicked,
      ROW_NUMBER() OVER(PARTITION BY subscriberkey,sendid ORDER BY eventdate DESC) AS rownum
    FROM `x-marketing.pcs_sfmc.event` activity
    WHERE eventtype = 'Unsubscribe'
  )
  WHERE rownum = 1
) 
, 
email_campaign AS (
  SELECT
  DISTINCT _preview AS _notes,
  _status AS _status,
  _campaign_code AS _trimcode,
  _ad_visual AS _screenshot,
  _form_submission AS _assettitle,
  _subject_line AS _subject,
  _email_name AS _whatwedo,
  _campaign_id AS _campaignid,
  _campaign_name AS _utm_campaign,
  _preview,
  _campaign_code AS _code,
  _campaign AS _journeyname,
  _email_segment AS _campaignname,
  _form_submission AS _formsubmission,
  _email_id AS _id,
  PARSE_TIMESTAMP('%m/%d/%Y', _live_date) AS _livedate,
  "" AS _utm_source,
  _email_name AS _emailname,
  '' AS _assignee,
  '' AS _utm_medium,
  _landing_page_url AS _landingpage,
  _email_segment AS _segment,
  _asset_url AS _link,
  _campaign AS _rootcampaign,
  _email_segment AS _emailsegment
FROM
  `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
WHERE
  _campaign = 'Primerica'
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _campaign_id, _code ORDER BY _livedate DESC) = 1
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
        _livedate AS _livedate, 
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
        sentdate, 
        senddate,
        status, 
        CAST(emailid  AS STRING), 
        fromname, 
        TRIM(airtable.emailname) AS emailname, 
        fromaddress, 
        CAST(isalwayson AS STRING),
        _trimcode, 
        _assettitle,
        _subject, 
        _whatwedo, 
         _campaignname AS campaignName, 
        CAST(_id AS STRING), 
        _journeyname,
        _notes,
      _emailsegment,_preview, 
        ROW_NUMBER() OVER(
            PARTITION BY emailname,airtable.id,emailid,_segment 
            ORDER BY senddate DESC
        ) AS rownum
    FROM `x-marketing.pcs_sfmc.send` airtable, unnest (partnerproperties) name
    LEFT JOIN  email_campaign ON airtable.id  = CAST(_campaignid AS INT)
  )
  WHERE rownum = 1
  AND _code IS NOT NULL
),
Contact AS (
  SELECT
    acc.id,
    title AS _title,
    email AS _email,
    phone AS _phone,
   CONCAT(firstname, ' ', lastname ) AS _name, 
   company AS _companyname, 
   territory__c, 
   CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link
  FROM `x-marketing.pcs_salesforce.Lead` acc
  --WHERE acc.isdeleted IS FALSE
  UNION ALL 
  SELECT
    acc.id,
    title AS _title,
    email AS _email,
    phone AS _phone,
   CONCAT(firstname, ' ', lastname ) AS _name, 
   account_name__c AS _companyname, 
   territory__c, 
   CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link
  FROM `x-marketing.pcs_salesforce.Contact` acc
  --WHERE acc.isdeleted IS FALSE 
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
) 
--WHERE rownum = 1
;



UPDATE `x-marketing.pcs.primerica_email_performance`  origin 
SET origin._remove_bot = 'True'
FROM (
SELECT _campaignID, _email, _prospectID
FROM (
SELECT _campaignID, _email, _prospectID,SUM(CASE WHEN _linkid LIKE "%DG-EM%" THEN 1
WHEN _linkid = "Content_downloaded" THEN 1  END) AS _content_donwloaded,
SUM(CASE WHEN _linkid = "Bot"  THEN 1 END) AS _bot
FROM `x-marketing.pcs.primerica_email_performance`
WHERE _engagement = 'Clicked' 
GROUP BY 1,2,3
) WHERE _bot IS NULL AND _content_donwloaded IS NOT NULL
) scenario
WHERE 

(origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid LIKE "%DG-EM%") OR (origin._email = scenario._email 
AND origin._campaignID = scenario._campaignID 
AND origin._prospectID = origin._prospectID
AND origin._engagement = "Clicked" AND _linkid LIKE "Content_downloaded");

TRUNCATE TABLE `x-marketing.pcs.pcs_email_engagement_log`; 
INSERT INTO  `x-marketing.pcs.pcs_email_engagement_log` 
SELECT * EXCEPT (roth_cont_rate_amt__c, roth_percentage__c, roth_rate_eff_date__c, roth_auto_enroll_cd__c,defered_cont_rate_amt__c, deferred_pct__c, deferred_rate_eff_date__c,program__c,financial_account_id,ownerid,ownername,_contact_sdc_sequence , _ga_utm_content, _ga_utm_source, _ga_utm_medium, _ga_content_downloaded)
FROM `x-marketing.pcs.pesc_email_performance` 
UNION ALL 
SELECT * FROM `x-marketing.pcs.dg_email_performance`
UNION ALL 
SELECT * FROM  `x-marketing.pcs.plan_sponsor_email_performance_log`
UNION ALL 
SELECT * FROM  `x-marketing.pcs.primerica_email_performance`
UNION ALL 
SELECT * EXCEPT( _contact_sdc_sequence) FROM `x-marketing.pcs.pepc_email_performance`
UNION ALL 
SELECT * EXCEPT (roth_cont_rate_amt__c, roth_percentage__c, roth_rate_eff_date__c, roth_auto_enroll_cd__c,defered_cont_rate_amt__c, deferred_pct__c, deferred_rate_eff_date__c,program__c,financial_account_id,ownerid,ownername)
FROM `x-marketing.pcs.selling_season_email_performance` ;


UPDATE `x-marketing.pcs.pcs_email_engagement_log` origin
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
            SUM(CASE WHEN _engagement IN( 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") THEN 1 END) AS _hasBounced,
        FROM 
            `x-marketing.pcs.pcs_email_engagement_log`
        WHERE
            _engagement IN ('Opened', 'Clicked', 'Soft bounce','Hard bounce','Block bounce',"Other Bounce") 
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
AND origin._engagement IN('Soft bounce','Hard bounce','Block bounce',"Other Bounce");



CREATE OR REPLACE TABLE `x-marketing.pcs.pesc_financial_account_history` as 
 WITH financial_account_history AS (
 SELECT k.id,
        field,
        Original_Value,
        New_Value,
        _date,
        user,
        j.name
        FROM (
    SELECT 
    parentid AS id,
    field,
    CASE WHEN CAST(oldvalue__fl AS STRING) IS NULL THEN oldvalue__st ELSE  CAST(oldvalue__fl AS STRING)  END AS Original_Value,
    CASE WHEN CAST(newvalue__fl AS STRING) IS NULL THEN newvalue__st ELSE CAST(newvalue__fl AS STRING) END AS New_Value,
    createddate AS _date,
    createdbyid AS user
    --LAG(createddate) OVER(PARTITION BY leadid ORDER BY createddate) previous_lead_status_change_date,
  FROM `x-marketing.pcs_salesforce.NEW_Financial_Account__History` history
  JOIN (SELECT DISTINCT financial_account_id FROM `x-marketing.pcs.pesc_email_performance` WHERE _engagement IN ("Clicked","Opened")) engagement ON history.parentid = engagement.financial_account_id
  WHERE  
  --parentid = 'a3g5x000002FqNIAA0' AND 
  ---field IN( "Owner") AND 
  isdeleted IS FALSE 
         )k
          JOIN `x-marketing.pcs_salesforce.User` j ON j.id = k.user
          --JOIN `x-marketing.pcs_salesforce.User` l ON l.id = k.new_status 
 ), email_engagement AS (
       SELECT financial_account_id, CONCAT("Email Engagement - ", _engagement), _engagement, _contentTitle, _timestamp,ownerid,ownername 
       FROM `x-marketing.pcs.pesc_email_performance` WHERE _engagement IN ("Clicked","Opened")
       
 ), combine AS (
 SELECT * FROM 
 (
 SELECT * FROM financial_account_history
 UNION ALL 
 SELECT * FROM email_engagement
 ) 
 ) ,
Contact AS (
  SELECT
    acc.id AS _prospectID,
    -- accountid AS _sfdcAccountID,
    -- CONCAT(firstname, ' ', lastname) AS _name,
    acc.title AS _title,
    -- email AS _email,
    acc.phone AS _phone,
    email_domain__c AS _domain,
    mailingcity AS _city,
    mailingcountry AS _country,
    mailingstate AS _state,
   fin.name AS financial_account_name, 
   total_market_value_amt__c,
   contributing__c,
   roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,
   fin.program__c,
   fin.id AS financial_account_id,fin.ownerid,j.name AS ownername,
  CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin ON acc.id =  fin.account_holder__c
  LEFT JOIN `x-marketing.pcs_salesforce.User` j ON j.id = fin.ownerid 
  WHERE acc.isdeleted IS FALSE 
 --WHERE id = 'a3g5x000001QULYAA4'
), open_click_engagement AS (
  SELECT *,
CONCAT("Email Engagement - ", _engagement) _engagements
       FROM `x-marketing.pcs.pesc_email_performance` WHERE _engagement IN ("Clicked","Opened")
), financial_account AS (
  SELECT * FROM combine 
LEFT JOIN Contact ON combine.id = contact.financial_account_id
) SELECT open_click_engagement.* EXCEPT (financial_account_id,_prospectID,_title,_phone,_domain,_city,_country,_state,contributing__c,roth_cont_rate_amt__c, 
   roth_percentage__c, 
   roth_rate_eff_date__c, 
   roth_auto_enroll_cd__c, 
   defered_cont_rate_amt__c, 
   deferred_pct__c, 
   deferred_rate_eff_date__c,
   program__c,ownerid,ownername,_salesforce_link),
financial_account.* FROM open_click_engagement 
LEFT JOIN financial_account ON open_click_engagement._prospectID = financial_account._prospectID;
