------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------ Email Engagement Log --------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Marketo
  Data type: Email Engagement
  Depedency Table: 
  Target table: db_email_engagements_log
*/

TRUNCATE TABLE `x-marketing.hyland.db_email_engagements_log`;
INSERT INTO `x-marketing.hyland.db_email_engagements_log` (
  _sdc_sequence,
  _campaignID,
  _utmcampaign,
  _subject,
  _timestamp,
  _engagement,
  _description,
  _utm_source,  
  _utm_medium, 
  _utm_content,
  _prospectID,
  _email,
  _name,
  _domain,
  _title,
  _function,
  _seniority,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _persona,
  _lifecycleStage,
  _leadsourcedetail,
  _mostrecentleadsource,
  _mostrecentleadsourcedetail,
  _programname,
  _programchannel,
  _abm_category__c_lead,
  _abm_category__c, 
  _lead_grade__c, 
  _id18__c, 
  _id_18__c,
  _campaignSentDate,
  EMEAcampaign,
  airtableSegment,
  _in_airtable,
  _program_campaignid,
  _campaignname_standardize,
  _program,
  _campaignname,
  _hive9owner,
  _campaignowner,
  _campaignstartdate,
  _campaignenddate,
  region__c,
  sub_region__c,
  description,
  _sfdccampaignid,
  _campaignType
)
WITH merged_id AS (
  SELECT 
    a.leadid, 
    m.value, 
    l.email
  FROM `x-marketing.epam_marketo.activities_merge_leads` a
  JOIN UNNEST(a.merge_ids) AS m
  JOIN `x-marketing.epam_marketo.leads` l 
    ON m.value = l.id
) ,
  prospect_info AS (
    SELECT * EXCEPT(rownum)
    FROM (
        SELECT DISTINCT 
            CAST(marketo.id AS STRING) AS _id,
            marketo.email AS _email,
            CONCAT(firstname,' ', lastname) AS _name,
            RIGHT(marketo.email, LENGTH(marketo.email) - STRPOS(marketo.email, '@')) AS _domain, 
            title AS _jobtitle,
            job_function__c AS _function,
            CASE 
              WHEN title LIKE '%Senior Counsel%' THEN "VP"
              WHEN title LIKE '%Assistant General Counsel%' THEN "VP" 
              WHEN title LIKE '%General Counsel%' THEN "C-Level" 
              WHEN title LIKE '%Founder%' THEN "C-Level" 
              WHEN title LIKE '%C-Level%' THEN "C-Level" 
              WHEN title LIKE '%CDO%' THEN "C-Level" 
              WHEN title LIKE '%CIO%' THEN "C-Level"
              WHEN title LIKE '%CMO%' THEN "C-Level"
              WHEN title LIKE '%CFO%' THEN "C-Level" 
              WHEN title LIKE '%CEO%' THEN "C-Level"
              WHEN title LIKE '%Chief%' THEN "C-Level" 
              WHEN title LIKE '%coordinator%' THEN "Non-Manager"
              WHEN title LIKE '%COO%' THEN "C-Level" 
              WHEN title LIKE '%Sr. V.P.%' THEN "Senior VP"
              WHEN title LIKE '%Sr.VP%' THEN "Senior VP"  
              WHEN title LIKE '%Senior-Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%srvp%' THEN "Senior VP" 
              WHEN title LIKE '%Senior VP%' THEN "Senior VP" 
              WHEN title LIKE '%SR VP%' THEN "Senior VP"  
              WHEN title LIKE '%Sr Vice Pres%' THEN "Senior VP" 
              WHEN title LIKE '%Sr. VP%' THEN "Senior VP" 
              WHEN title LIKE '%Sr. Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%S.V.P%' THEN "Senior VP" 
              WHEN title LIKE '%Senior Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%Exec Vice Pres%' THEN "Senior VP" 
              WHEN title LIKE '%Exec Vp%' THEN "Senior VP"  
              WHEN title LIKE '%Executive VP%' THEN "Senior VP" 
              WHEN title LIKE '%Exec VP%' THEN "Senior VP"  
              WHEN title LIKE '%Executive Vice President%' THEN "Senior VP" 
              WHEN title LIKE '%EVP%' THEN "Senior VP"  
              WHEN title LIKE '%E.V.P%' THEN "Senior VP" 
              WHEN title LIKE '%SVP%' THEN "Senior VP" 
              WHEN title LIKE '%V.P%' THEN "VP" 
              WHEN title LIKE '%VP%' THEN "VP" 
              WHEN title LIKE '%Vice Pres%' THEN "VP"
              WHEN title LIKE '%V P%' THEN "VP"
              WHEN title LIKE '%President%' THEN "C-Level"
              WHEN title LIKE '%Director%' THEN "Director"
              WHEN title LIKE '%CTO%' THEN "C-Level"
              WHEN title LIKE '%Dir%' THEN "Director"
              WHEN title LIKE '%MDR%' THEN "Non-Manager"
              WHEN title LIKE '%MD%' THEN "Director"
              WHEN title LIKE '%GM%' THEN "Director"
              WHEN title LIKE '%Head%' THEN "VP"
              WHEN title LIKE '%Manager%' THEN "Manager"
              WHEN title LIKE '%escrow%' THEN "Non-Manager"
              WHEN title LIKE '%cross%' THEN "Non-Manager"
              WHEN title LIKE '%crosse%' THEN "Non-Manager"
              WHEN title LIKE '%Assistant%' THEN "Non-Manager"
              WHEN title LIKE '%Partner%' THEN "C-Level"
              WHEN title LIKE '%CRO%' THEN "C-Level"
              WHEN title LIKE '%Chairman%' THEN "C-Level"
              WHEN title LIKE '%Owner%' THEN "C-Level"
            END AS _seniority,
            phone AS _phone,
            company AS _company,
            CAST(annualrevenue AS STRING) AS _revenue,
            industry AS _industry,
            city AS _city,
            state AS _state, 
            country AS _country,
            "" AS _persona,
            lead_lifecycle_stage__c AS _lifecycleStage,
            leadsourcedetail,
            mostrecentleadsource,
            mostrecentleadsourcedetail,
            programs.name,
            programs.channel,
            abm_category__c_lead , 
            abm_category__c , 
            lead_grade__c , 
            id18__c, id_18__c ,
            ROW_NUMBER() OVER(
              PARTITION BY marketo.email
              ORDER BY marketo.id DESC
            ) AS rownum
        FROM `x-marketing.hyland_marketo.leads` marketo
        LEFT JOIN
          `x-marketing.hyland_marketo.programs` programs
        ON
          marketo.acquisitionprogramid = CAST(programs.id AS STRING)
          LEFT JOIN merged_id AS merged  --this table is to get the email for the merge id 
    ON marketo.id = merged.leadid
        WHERE
            marketo.email IS NOT NULL
            AND marketo.email NOT LIKE '%2x.marketing%'
            AND marketo.email NOT LIKE '%hyland.com%'
    )
    WHERE rownum = 1
  ) , deleted_leads AS (
     SELECT 
    CAST(leadid AS STRING) AS leadid,
  FROM `x-marketing.epam_marketo.activities_delete_lead` 
  ) ,
  airtable_emea AS (
    SELECT
      _assetid AS id,
      CASE
        WHEN _senddate = '' THEN CAST(null AS TIMESTAMP)
        ELSE CAST(_senddate AS TIMESTAMP)
      END AS _campaignSentDate,
      CASE
        WHEN _assetid IS NOT NULL
        THEN 'Yes'
        ELSE 'No'
      END AS EMEAcampaign,
      'EMEA' AS airtable_segment,
      _sfcampaignid
    FROM
      `x-marketing.hyland_mysql.db_airtable_email_emea` 
  
  ),
  airtable_customermarketing AS (
  SELECT
      _assetid AS id,
      CASE
        WHEN _senddate = '' THEN CAST(null AS TIMESTAMP)
        ELSE CAST(_senddate AS TIMESTAMP)
      END AS _campaignSentDate,
      CASE
        WHEN _assetid IS NOT NULL
        THEN 'Yes'
        ELSE 'No'
      END AS EMEAcampaign,
      'Customer Marketing' AS airtable_segment,
      '' AS _sfcampaignid
    FROM
      `x-marketing.hyland_mysql.db_airtable_email_customermarketing` 
  ),
  airtable_info AS (
  SELECT * FROM airtable_emea
  UNION ALL
  SELECT* FROM airtable_customermarketing
),
  email_sent AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'sent' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_send_email`
    )
    WHERE
      _rownum = 1 
  ),
  email_delivered AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'delivered' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_delivered` 
    )
    WHERE
      _rownum = 1 
  ),
  email_open AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'opened' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_open_email`
    )
    WHERE
      _rownum = 1 
  ),
  email_click AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'clicked' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_click_email`
    )
    WHERE
      _rownum = 1 
  ),
  unique_click AS (
    SELECT
      DISTINCT
      email_click.*
    FROM email_click
    JOIN email_open ON email_open._leadid = email_click._leadid
  ),
  email_hard_bounce AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'hard_bounced' AS _engagement,
        details AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_bounced`
    )
    WHERE
      _rownum = 1 
  ),
  email_soft_bounce AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'soft_bounced' AS _engagement,
        details AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_bounced_soft`
    )
    WHERE
      _rownum = 1  
  ),
  email_download AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'downloaded' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_fill_out_form`
      WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
      AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
    )
    WHERE
      _rownum = 1 
  ),
  email_unsubscribed AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'unsubscribed' AS _engagement,
        '' AS _description,
        campaignid,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_unsubscribe_email`
    )
    WHERE
      _rownum = 1 
  ),
  engagements_combined AS (
    SELECT * FROM email_sent
    UNION ALL
    SELECT * FROM email_delivered
    UNION ALL
    SELECT * FROM email_open
    -- UNION ALL
    -- SELECT * FROM email_click
    UNION ALL
    SELECT * FROM unique_click
    UNION ALL
    SELECT * FROM email_hard_bounce
    UNION ALL
    SELECT * FROM email_soft_bounce
    UNION ALL
    SELECT * FROM email_unsubscribed
  ),program AS (
    SELECT programname,
    programid,
    campaign.id AS campaignid,
    campaign.name AS _campaignname
    FROM `x-marketing.hyland_marketo.campaigns` campaign
    LEFT JOIN `x-marketing.hyland_marketo.programs` program ON campaign.programid = program.id
  ) , 
  campaign_standardize AS (
    SELECT DISTINCT

        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        
      FROM `x-marketing.hyland_marketo.activities_send_email`
     QUALIFY DENSE_RANK() OVER(
          PARTITION BY primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) = 1 
  )
  ,
  _all AS (
SELECT
  engagements.* EXCEPT( _leadid,_email, campaignid),
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.*,
  CAST(airtable_info._campaignSentDate AS TIMESTAMP) AS _campaignSentDate,
  airtable_info.EMEAcampaign,
  airtable_info.airtable_segment,
   CASE WHEN airtable_info.id IS NULL THEN 'Not Airtable' ELSE 'Airtable' END as _airtable_segment,
   CAST(engagements.campaignid AS STRING) AS campaignids,
   _campaignname,
   programname,
   campaign._campaign AS _campaignname_standardize,
  airtable_info._sfcampaignid
  
  FROM 
  engagements_combined AS engagements
  
  LEFT JOIN
  prospect_info
  ON
  engagements._leadid = prospect_info._id
  LEFT JOIN
  airtable_info
  ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
  
  LEFT JOIN program ON engagements.campaignid = program.campaignid
  LEFT JOIN campaign_standardize campaign ON engagements._campaignID = campaign._campaignID
  LEFT JOIN deleted_leads d  ON d.leadid = engagements._leadid

  WHERE d.leadid IS NULL
  )
  SELECT 
  _all.* EXCEPT (_sfcampaignid),
  hive9_owner__c AS _hive9owner,
  user.name AS _campaignowner,
  startdate AS _campaignstartdate,
  enddate AS _campaignenddate,
  region__c,
  sub_region__c,
  description,
  sfcampaign.id AS _sfdccampaignid,
  sfcampaign.type AS _campaignType
FROM _all
LEFT JOIN `x-marketing.hyland_salesforce.Campaign` sfcampaign ON sfcampaign.id = _all._sfcampaignid
LEFT JOIN (SELECT name, id FROM `x-marketing.hyland_salesforce.User`) user ON user.id = sfcampaign.ownerid;


CREATE OR REPLACE TABLE `x-marketing.hyland.db_email_aggregate` AS
SELECT   
  _campaignname_standardize,
  _program,
  _campaignID,
  _campaignname,
  _utmcampaign,
  _sfdccampaignid,
  _programname,
  _programchannel,
  SUM(CASE WHEN _engagement = 'sent' THEN 1 ELSE 0 END) AS _sent,
  SUM(CASE WHEN _engagement = 'delivered' THEN 1 ELSE 0 END) AS _delivered,
  SUM(CASE WHEN _engagement = 'soft_bounced' THEN 1 ELSE 0 END) AS _soft_bounced,
  SUM(CASE WHEN _engagement = 'hard_bounced' THEN 1 ELSE 0 END) AS _hard_bounced,
  SUM(CASE WHEN _engagement = 'opened' THEN 1 ELSE 0 END) AS _opened,
  SUM(CASE WHEN _engagement = 'clicked' THEN 1 ELSE 0 END) AS _clicked,
  SUM(CASE WHEN _engagement = 'downloaded' THEN 1 ELSE 0 END) AS _downloaded,
  SUM(CASE WHEN _engagement = 'unsubscribed' THEN 1 ELSE 0 END) AS _unsubscribed,
FROM `x-marketing.hyland.db_email_engagements_log` email
GROUP BY 1,2,3,4,5,6,7,8;





----------------------------------------------------------Email Campaign Timeline---------------------------------------------------------
CREATE OR REPLACE TABLE `x-marketing.hyland.db_email_details_aggregate` AS
WITH campaign_aggregate AS (
SELECT _campaignname, 
      hive9_owner__c AS _hive9owner,
      user.name AS _campaignowner,
      _assetid AS _marketo_id,
      --CASE WHEN _senddate = "" THEN NULL ELSE
      --PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez",_senddate) END AS _senddate,
      _sfcampaignid,
      startdate AS _campaignstartdate,
      enddate AS _campaignenddate,
      region__c,
      sub_region__c,
      description,
      type AS _campaigntype
FROM `x-marketing.hyland_mysql.db_airtable_email_emea` airtable
LEFT JOIN `x-marketing.hyland_salesforce.Campaign` campaign ON campaign.id = airtable._sfcampaignid
LEFT JOIN (SELECT name, id FROM `x-marketing.hyland_salesforce.User`) user ON user.id = campaign.ownerid
),

email_aggregate AS(
SELECT _campaignID,
       _utmcampaign,
       _sfdccampaignid,
       SUM(CASE WHEN _engagement = 'sent' THEN 1 ELSE 0 END) AS Sent,
       SUM(CASE WHEN _engagement = 'delivered' THEN 1 ELSE 0 END) AS Delivered,
       SUM(CASE WHEN _engagement = 'soft_bounced' THEN 1 ELSE 0 END) AS Soft_Bounced,
       SUM(CASE WHEN _engagement = 'hard_bounced' THEN 1 ELSE 0 END) AS Hard_Bounced
FROM `x-marketing.hyland.db_email_engagements_log` email
WHERE _in_airtable = 'Airtable'
GROUP BY 1,2,3
)

SELECT campaign_aggregate.*, email_aggregate.* EXCEPT(_campaignID,_utmcampaign,_sfdccampaignid)
FROM campaign_aggregate
LEFT JOIN email_aggregate ON email_aggregate._sfdccampaignid = campaign_aggregate._sfcampaignid
WHERE _campaignstartdate IS NOT NULL