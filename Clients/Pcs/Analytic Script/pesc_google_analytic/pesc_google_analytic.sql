CREATE OR REPLACE TABLE `x-marketing.pcs.googleanalytic_contact` AS 
WITH contact AS (
  SELECT
    acc.id,
    acc.accountid AS _sfdcAccountID,
    CONCAT(acc.firstname, ' ', acc.lastname) AS _name,
    acc.title AS _title,
    acc.email AS _email,
    acc.phone AS _phone,
    email_domain__c AS _domain,
    account_name__c AS _companyname, 
    territory__c, 
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
   fin.id AS financial_account_id,
   fin.ownerid,
   j.name AS owner_name,
   birthdate,
   discovery_date_of_birth_full__c,
   DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) AS age,
   CASE WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) <= 39 THEN "Early"
   WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) >= 40 AND DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) <= 59 THEN "Mid"
   WHEN DATE_DIFF(CURRENT_DATE(), DATE(birthdate), YEAR) >= 60 THEN "End" END AS _age_segment,
  CONCAT('https://pcsretirement.lightning.force.com/lightning/r/Lead/',masterrecordid,'/view') AS _salesforce_link,
  CAST(acc._sdc_sequence AS STRING) AS _sdc_sequence
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN (SELECT * EXCEPT (rownum)
FROM (
  
  SELECT 
  fin.program__c,fin.name,fin.id,
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
), prep AS (
  select
  event_date, 
  event.value.string_value AS page_title,
  user_id, 
  user.value.string_value AS ids,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  (max(event_timestamp)-min(event_timestamp))/1000000 as session_length_in_seconds,
  from
  -- change this to your google analytics 4 export location in bigquery
  `x-marketing.analytics_411351491.events_*` ,UNNEST(event_params) event,UNNEST (user_properties) user
  group by
  event_date,event.value.string_value, user_id,user.value.string_value,
 session_id
), avg AS (

select
event_date, page_title,user_id,ids,

  -- average session duration (metric | the average duration (in seconds) of users' sessions)
  sum(session_length_in_seconds) / count(distinct session_id) as average_session_duration_seconds
from
  prep
  group by
  event_date,
  page_title,user_id,ids
 ) , ga AS ( 
 SELECT *
  FROM avg
 ),email_campaign AS (
  SELECT * EXCEPT (rownum)
  FROM (
    SELECT 
      *,  
      ROW_NUMBER() OVER(
          PARTITION BY airtable_id,_code 
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
        _campaignid AS airtable_id, 
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
        _rootcampaign,
        _emailsegment
      FROM `x-marketing.pcs_mysql.db_airtable_email_participant_engagement` 
      WHERE _rootcampaign = 'Participant Education Series'
      )
  ) 
  WHERE rownum = 1
  AND airtable_id != ''
  AND airtable_id IS NOT NULL 
  --AND id = "221593"
),get_campaignid AS (
  SELECT 
  CONCAT(event_date,event_timestamp,event_name)AS _id,
  CASE WHEN event.key= 'page_location' THEN REGEXP_EXTRACT(event.value.string_value, r'[\?&]j=([^&]*)') END AS campaign_id,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))  AS ga_id
  FROM `x-marketing.analytics_411351491.events_*`, UNNEST (event_params) event
)
,campaign_ids AS (
  SELECT 
  DISTINCT 
  _id,
  campaign_id,
  ga_id 
  FROM get_campaignid 
  WHERE campaign_id IS NOT NULL
) 
, google_analytic_activity AS (
  SELECT activity.*,
  SPLIT(SUBSTR(traffic_source.name, STRPOS(traffic_source.name, '?j=') + 3), '&')[ORDINAL(1)] AS _campaignids,
  PARSE_TIMESTAMP('%Y%m%d',activity.event_date) AS _timestamp,
  COALESCE(event.value.string_value,CAST(event.value.int_value AS STRING), CAST(event.value.float_value AS STRING), CAST(event.value.double_value AS STRING),CAST(TIMESTAMP_MICROS(event_previous_timestamp) AS STRING)) AS events,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  FROM `x-marketing.analytics_411351491.events_*`  activity ,UNNEST(event_params) event,UNNEST (user_properties) user
) 
, all_data AS (
  SELECT 
  activity.*,
  l.*,
  c.campaign_id
  
  FROM google_analytic_activity activity, UNNEST (user_properties) user
  LEFT JOIN Contact  l ON /*activity.subscriberkey = l.email or*/ user.value.string_value = id
  LEFT JOIN ga on user.value.string_value = ids and ga.event_date = activity.event_date
  LEFT JOIN campaign_ids c ON session_id = ga_id
) 
SELECT 
datas.*,
email_campaign.*
--CASE WHEN event.key= 'page_location' THEN REGEXP_EXTRACT(event.value.string_value, r'[?&]j=([^&]+)') END AS _campaignid
FROM all_data AS datas,UNNEST(event_params) event
LEFT JOIN email_campaign ON campaign_id = email_campaign.airtable_id;
