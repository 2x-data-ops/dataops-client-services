------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

#This is the script to power the Account Health reporting page
-- CREATE OR REPLACE TABLE terrasmart.db_consolidated_engagements_log AS
TRUNCATE TABLE `terrasmart.db_consolidated_engagements_log`;
INSERT INTO `terrasmart.db_consolidated_engagements_log`
--CREATE OR REPLACE TABLE terrasmart.db_consolidated_engagements_log AS

WITH 
#Query to pull all the contacts in the leads table from Marketo
  contacts AS (
SELECT * EXCEPT (rownum)
FROM (
SELECT *,
ROW_NUMBER() OVER(
            PARTITION BY _email
            ORDER BY _id DESC
          ) 
          AS rownum
FROM (
SELECT 
 contacts.*,
 _ebos, 
 _utilityprojects, 
 _tracker, 
 COALESCE(_linkedinurl,_accountlinkedin) AS _acc_linkedinurl, 
 _canopy, 
 _prospect, 
 _fixedtilt, 
 COALESCE(_account,_company) AS _account, 
 acc._persona AS _acc_persona, 
 _rep, 
 _midwest,
 _otherdomain,
 COALESCE(acc._domain,contacts._key_account_domain) AS _domain,
 CASE WHEN acc._domain IS NOT NULL THEN "Key Account" ELSE "Other Account" END AS _account_segment,
 _type
 

 FROM 
 (
 WITH icp_contact AS (SELECT   _id,
      _email,
      _name,
      _domain AS _key_account_domain, 
      _jobtitle,
      _seniority,
      _function,
      _phone,
      _company,
      _revenue,
      _industry,
      _city, 
      _state,
      _country,
      _persona,
      _lifecycleStage,
      _sfdcaccountid,
      _sfdccontactid,
      _contact_type,
      _leadorcontactid,
      CAST(NULL AS STRING) AS _contactlinkedin,
      CAST(NULL AS STRING) AS _accountlinkedin, 
FROM (
SELECT 
          id  AS _id,
          LOWER(email) AS _email,
          CONCAT(first_name, ' ', last_name) AS _name,
          RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
          job_title AS _jobtitle,
          CASE
            WHEN LOWER(job_title) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%Senior Counsel%") THEN "VP"  
            WHEN LOWER(job_title) LIKE LOWER("%General Counsel%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Founder%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%C-Level%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CDO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CIO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CMO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CFO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CEO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Chief%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%COO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%srvp%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%SR VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%EVP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%SVP%") THEN "Senior VP" 
            WHEN LOWER(job_title) LIKE LOWER("%V.P%") THEN "VP" 
            WHEN LOWER(job_title) LIKE LOWER("%VP%") THEN "VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Vice Pres%") THEN "VP" 
            WHEN LOWER(job_title) LIKE LOWER("%V P%") THEN "VP" 
            WHEN LOWER(job_title) LIKE LOWER("%President%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Director%") THEN "Director" 
            WHEN LOWER(job_title) LIKE LOWER("%CTO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Dir%") THEN "Director" 
            WHEN LOWER(job_title) LIKE LOWER("%Dir.%") THEN "Director" 
            WHEN LOWER(job_title) LIKE LOWER("%MDR%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%MD%") THEN "Director" 
            WHEN LOWER(job_title) LIKE LOWER("%GM%") THEN "Director" 
            WHEN LOWER(job_title) LIKE LOWER("%Head%") THEN "VP" 
            WHEN LOWER(job_title) LIKE LOWER("%Manager%") THEN "Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%escrow%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%cross%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%crosse%") THEN "Non-Manager" 
            WHEN LOWER(job_title) LIKE LOWER("%Partner%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%CRO%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Chairman%") THEN "C-Level" 
            WHEN LOWER(job_title) LIKE LOWER("%Owner%") THEN "C-Level"
            WHEN LOWER(job_title) LIKE LOWER("%Team Lead%") THEN "Manager"
          END AS _seniority,
          ""AS _function,
          phone AS _phone,
          company AS _company,
          "" AS _revenue,
          industry AS _industry,
          city AS _city, 
          state AS _state,
          country AS _country,
          "" AS _persona,
          "" AS _lifecycleStage,
          crm_contact_fid AS _sfdccontactid,
          crm_lead_fid AS _sfdcleadid,
          CASE 
          WHEN crm_contact_fid IS NOT NULL THEN "Contact"
          WHEN crm_contact_fid IS NULL THEN "Lead"
        END AS _contact_type,
          COALESCE(crm_contact_fid, crm_lead_fid) AS _leadorcontactid,
          CAST(NULL AS STRING) AS _contactlinkedin,
      CAST(NULL AS STRING) AS _accountlinkedin, 
          ROW_NUMBER() OVER( PARTITION BY email ORDER BY created_at DESC) AS _rownum,
        FROM 
          `x-marketing.terrasmart_pardot.prospects` 
) main
 LEFT JOIN
    (SELECT id, accountid AS _sfdcaccountid, FROM terrasmart_salesforce.Contact) sfcontact ON (sfcontact.id = main._leadorcontactid AND main._contact_type = 'Contact')
), not_inprospect AS ( 
  SELECT DISTINCT
      CAST( _prospectID AS INT64) AS _id,
      _email,
      _name,
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      _seniority AS _jobtitle,
      _seniority,
      _function,
      _phone,
      _company,
      CAST(NULL AS STRING) AS _revenue,
      _industry,
      _city, 
      _state,
      _country,
      CAST(NULL AS STRING) AS _persona,
      CAST(NULL AS STRING) AS _lifecycleStage,
      CAST(NULL AS STRING) AS _sfdcaccountid,
      CAST(NULL AS STRING) AS _sfdccontactid,
      CAST(NULL AS STRING) AS _contact_type,
      CAST(NULL AS STRING) AS _leadorcontactid,
      CAST(NULL AS STRING) AS _contactlinkedin,
      CAST(NULL AS STRING) AS _accountlinkedin, 
    FROM 
      `terrasmart.db_email_engagements_log`tam
      WHERE _email NOT IN (SELECT DISTINCT _email FROM icp_contact)
), abm_engagement AS (
  SELECT * EXCEPT (rownum)
      FROM (
        SELECT * ,
        ROW_NUMBER() OVER(
            PARTITION BY _email
            ORDER BY _id DESC
          ) 
          AS rownum
          FROM (
            SELECT 
            DISTINCT 
            _id AS _id, 
            CONCAT(_contactname,"@",
            CASE WHEN _accountdomain LIKE "%www.%" THEN ( REGEXP_REPLACE(
              SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
              '(www\\.|/+$)',''))
              ELSE (
                REGEXP_REPLACE(_accountdomain,'(https?://|www\\.|/+$)',''))END )  AS _email,
                _contactname,
                CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        _contacttitle,
        CAST(NULL AS STRING) AS _seniority,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _phone,
        _accountname AS    _company,
        CAST(NULL AS STRING) AS _revenue,
        CAST(NULL AS STRING) AS _industry,
        CAST(NULL AS STRING) AS _city, 
        CAST(NULL AS STRING) AS _state,
        _contactcountry AS    _country,
        CAST(NULL AS STRING) AS    _persona,
        CAST(NULL AS STRING) AS    _lifecycleStage,
        CAST(NULL AS STRING) AS    _sfdcaccountid,
        CAST(NULL AS STRING) AS    _sfdccontactid,
        CAST(NULL AS STRING) AS   _contact_type,
        CAST(NULL AS STRING) AS   _leadorcontactid,
        _contactlinkedin,
        _accountlinkedin, 
        FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`
        )
        ) contacts
        WHERE rownum = 1
), ga4_domain AS (
   SELECT DISTINCT
  session_ids,
  CONCAT(user_pseudo_id,"@",_domain) AS _email,
  CAST(NULL AS STRING) AS _contactname,
  _domain,
  CAST(NULL AS STRING) AS _contacttitle,
  CAST(NULL AS STRING) AS _seniority,
  CAST(NULL AS STRING) AS _function,
  CAST(NULL AS STRING) AS _phone,
  _name AS    _company,
  CAST(NULL AS STRING) AS _revenue,
  _industry AS _industry,
  CAST(NULL AS STRING) AS _city, 
  CAST(NULL AS STRING) AS _state,
  _country AS    _country,
  CAST(NULL AS STRING) AS    _persona,
  CAST(NULL AS STRING) AS    _lifecycleStage,
  CAST(NULL AS STRING) AS    _sfdcaccountid,
  CAST(NULL AS STRING) AS    _sfdccontactid,
  CAST(NULL AS STRING) AS   _contact_type,
  CAST(NULL AS STRING) AS   _leadorcontactid,
  CAST(NULL AS STRING) AS _contactlinkedin,
  CAST(NULL AS STRING) AS _accountlinkedin,
    
  FROM
    `x-marketing.terrasmart.db_web_engagements_log`
    WHERE _domain IS NOT NULL
  AND _domain <> 'Non-Company Visitor GA'
  AND _domain <> '(Non-Company Visitor)' 
),event AS (
  SELECT DISTINCT
__sdc_row AS _id,
COALESCE(LOWER(email_address),CONCAT(LOWER(first_name),'@',domain)) AS _email, 
CONCAT(first_name," ",last_name) AS _contactname,
domain AS _domain,
job_title,
CAST(NULL AS STRING) AS _seniority,
CAST(NULL AS STRING) AS _function,
CAST(NULL AS STRING) AS _phone, 
company_name, 
CAST(NULL AS STRING) AS _revenue,
CAST(NULL AS STRING) AS _industry,
CAST(NULL AS STRING) AS _city, 
CAST(NULL AS STRING) AS _state,
CAST(NULL AS STRING) AS    _country,
CAST(NULL AS STRING) AS    _persona,
CAST(NULL AS STRING) AS    _lifecycleStage,
CAST(NULL AS STRING) AS    _sfdcaccountid,
CAST(NULL AS STRING) AS    _sfdccontactid,
CAST(NULL AS STRING) AS   _contact_type,
CAST(NULL AS STRING) AS   _leadorcontactid,
CAST(NULL AS STRING) AS _contactlinkedin,
CAST(NULL AS STRING) AS _accountlinkedin,
FROM `x-marketing.terrasmart_googlesheet.Event_Attendees_New`

)
SELECT * FROM icp_contact
UNION ALL 
SELECT * FROM not_inprospect
UNION ALL 
SELECT * FROM abm_engagement
UNION ALL 
SELECT * FROM ga4_domain
UNION ALL
SELECT * FROM event 
 ) contacts
 LEFT JOIN  (SELECT * FROM `x-marketing.terrasmart_mysql_2.db_key_accounts` 
 WHERE _linkedinurl <> 'www.linkedin.com/company/centrica-business-solutions/')  acc ON contacts._key_account_domain = acc._otherdomain
)) WHERE rownum = 1
)
  ,
  accounts AS (
      SELECT 
        DISTINCT _key_account_domain, 
        CAST(NULL AS INT64) AS _id,
        CAST(NULL AS STRING) AS _name, 
        -- CAST(NULL AS STRING) AS _lastname,
        CAST(NULL AS STRING) AS _title,
        CAST(NULL AS STRING) AS _seniority,
        MAX(_phone) AS _phone,
        MIN(_company) AS _company, 
        MAX(_revenue) AS _revenue,
        MAX(_industry) AS _industry, 
        MAX(_city) AS _city,
        MAX(_state) AS _state,
        MAX(_country) AS _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        MAX(_sfdccontactid) AS _sfdcaccountid,
        CAST(NULL AS STRING) _sfdccontactid,
        MAX(_ebos), 
        MAX(_utilityprojects), 
        MAX(_tracker),
        MAX(_acc_linkedinurl), 
        MAX(_canopy), 
        MAX(_prospect), 
        MAX(_fixedtilt), 
        MAX(_account),  
        MAX(_acc_persona), 
        MAX(_rep), 
        MAX(_midwest),
        MAX(_account_segment)
        -- ROW_NUMBER() OVER(PARTITION BY _sfdcaccountid ORDER BY lastactivitydate DESC) AS _order
      FROM 
        contacts
      WHERE
        _country IS NOT NULL
        OR _industry IS NOT NULL
        OR _company IS NOT NULL
        OR _revenue IS NOT NULL
        OR _phone IS NOT NULL
        OR _city IS NOT NULL
        OR _state IS NOT NULL
      GROUP BY 
       _key_account_domain
      ORDER BY
        _key_account_domain
      /* RIGHT JOIN
        `terrasmart_salesforce.Account` sfdc ON contacts._sfdcaccountid = sfdc.id */
)
,
  #Query to pull the email engagement 
email_engagement AS (
      SELECT 
        *
      FROM ( 
        SELECT DISTINCT _email, 
        RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
        TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _campaignSentDate)) AS _timestamp,
        DATE_TRUNC(DATE(_campaignSentDate), MONTH) AS _month,
        DATE_TRUNC(DATE(_campaignSentDate), QUARTER) AS _quater,
        EXTRACT(WEEK FROM _campaignSentDate) AS _week,  
        EXTRACT(YEAR FROM _campaignSentDate) AS _year,
        _utmcampaign AS _contentTitle, 
        CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
        _description,
        _email_id AS _campaignID,
        FROM 
          (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
          /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
          AND */ LOWER(_engagement) NOT IN ('sent', 'downloaded', 'bounced', 'unsubscribed', 'processed', 'deffered', 'spam', 'suppressed', 'dropped')
      ) a
      WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart') 
        AND _domain IS NOT NULL 
      ORDER BY 
        1, 3 DESC, 2 DESC
),form_filled_engagement AS (
     SELECT * 
    FROM ( 
        SELECT 
            LOWER(_email) AS _email, 
            RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
            TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date , 
            DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
            DATE_TRUNC(DATE(_timestamp), QUARTER) AS _quater,
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year,
            _utmcampaign AS _contentTitle, 
            CONCAT("Email ", "Form Filled") AS _engagement,
            CAST(NULL AS STRING) AS _description,
            _email_id
        FROM 
            (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ 
            _engagement IN ('Downloaded')
    ) a
    WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart|gmail|yahoo|outlook|hotmail') 
        AND _domain IS NOT NULL 
    ORDER BY 1, 3 DESC, 2 DESC
), paid_social_engagment AS (
    SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
         DATE_TRUNC(DATE(_date), QUARTER) AS _quater,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Paid Social ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
        CAST(campaign_id AS STRING)
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement` abm,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    LEFT JOIN x-marketing.terrasmart_google_sheet.Campaign_Topic_Grouping c ON abm._campaignname = c.campaign_name
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow','Click') 
        AND _medium = 'Paid Social'
), organic_social_engagment AS (
     SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
        DATE_TRUNC(DATE(_date), QUARTER) AS _quater,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Organic Social ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
        CAST(campaign_id AS STRING)
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement` abm,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    LEFT JOIN x-marketing.terrasmart_google_sheet.Campaign_Topic_Grouping c ON abm._campaignname = c.campaign_name
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow') 
        AND _medium = 'Organic Social'
), webinar_engagement AS (
    SELECT DISTINCT
  CONCAT(_contactname,"@",CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END )  AS _email,
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        TIMESTAMP(_date) AS _date,
        DATE_TRUNC(DATE(_date), MONTH) AS _month,
        DATE_TRUNC(DATE(_date), QUARTER) AS _quater,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _campaignname AS _contentTitle, 
        CONCAT('Webinar ', INITCAP(sepEngagementType)) AS _engagement,
        _frequency AS _description,
        ''
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    WHERE  TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow') 
        AND _medium = 'Webinar'
), web_engagement AS (
  SELECT 
  DISTINCT 
  CONCAT(user_pseudo_id,"@",_domain) AS _email, 
  _domain, 
  _timestamp, 
  DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
  DATE_TRUNC(DATE(_timestamp), QUARTER) AS _quater,
  EXTRACT(WEEK FROM _timestamp) AS _week,
  EXTRACT(YEAR FROM _timestamp) AS  _year, 
  page_location AS _webActivity, 
  "Web Visit" AS _engagement,
  CONCAT(
      "Engagement Time (s): ", COALESCE(_engagementtime,0.0), "\n",
      "utm_source: ", COALESCE(_source,''), "\n",
      "utm_campaign: ",COALESCE( _utmcampaign,''), "\n",
      "utm_medium: ", COALESCE( _medium,''), "\n",
      "utm_content: ",COALESCE(  _page_title,''), "\n") AS _description,
      ''
  FROM
    `x-marketing.terrasmart.db_web_engagements_log`
  WHERE _domain IS NOT NULL
  AND _domain <> 'Non-Company Visitor GA'
  AND _domain <> '(Non-Company Visitor)' 
    --NOT REGEXP_CONTAINS(LOWER(_fullurl), 'unsubscribe')
    --AND NOT REGEXP_CONTAINS(LOWER(_fullurl), '=linkedin|=google|=6sense')
  ORDER BY 
    _domain, _timestamp DESC
) , webinar_sign_up AS (
  SELECT DISTINCT
  LOWER(email) AS _email, 
  RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
 joined AS _date,
 DATE_TRUNC( CAST(joined AS DATE), MONTH) AS _month,
  DATE_TRUNC(CAST(joined AS DATE), QUARTER) AS _quater,
 EXTRACT(WEEK FROM joined) AS _week,  
 EXTRACT(YEAR FROM  joined) AS _year,
event_webinar_name AS _contentTitle, 
CONCAT("Webinar ", "Sign Up") AS _engagement,
campaign AS _description,
CAST(list_id AS STRING)
FROM `x-marketing.terrasmart_googlesheet.Webinar_Sign_ups`  

) , webinar_attendees AS (
  SELECT DISTINCT
LOWER(email) AS _email, 
RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain, 
 joined AS _date,
DATE_TRUNC( CAST(joined AS DATE), MONTH) AS _month,
  DATE_TRUNC(CAST(joined AS DATE), QUARTER) AS _quater,
EXTRACT(WEEK FROM joined) AS _week,  
EXTRACT(YEAR FROM  joined) AS _year,
event_webinar_name AS _contentTitle, 
CONCAT("Webinar ", "Attendees") AS _engagement,
campaign AS _description,
CAST(list_id AS STRING)
FROM `x-marketing.terrasmart_googlesheet.Webinar_Attendees` 
), event_attendees AS (
   SELECT DISTINCT
COALESCE(LOWER(email_address),CONCAT(LOWER(first_name),'@',domain)) AS _email, 
domain AS _domain, 
 CAST(date AS TIMEstamp) AS _date,
DATE_TRUNC( CAST(date AS DATE), MONTH) AS _month,
  DATE_TRUNC(CAST(date AS DATE), QUARTER) AS _quater,
EXTRACT(WEEK FROM CAST(date AS DATE)) AS _week,  
EXTRACT(YEAR FROM  CAST(date AS DATE)) AS _year,
event_name AS _contentTitle, 
"Event Attendees" AS _engagement,
event_type AS _description,
CAST(event_id AS STRING)
FROM `x-marketing.terrasmart_googlesheet.Event_Attendees_New`
)
,
  dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements

    SELECT
      _date,
      DATE_TRUNC(DATE(_date), MONTH) AS _month,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 

  ),
  account_scores AS (

    SELECT
      * EXCEPT (_year),
      EXTRACT(WEEK FROM _extract_date) AS _week,
      EXTRACT(MONTH FROM _extract_date) AS _month,
      EXTRACT(YEAR FROM _extract_date) AS _year,
      (COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_paid_social , 0)+ COALESCE(_quarterly_organic_social , 0)+ COALESCE(_quarterly_webinar , 0)+ COALESCE(_quarterly_event_score, 0)+ COALESCE(_quarterly_form_filled_score, 0) )AS _account_90days_score
    FROM 
      `x-marketing.terrasmart.account_90days_score`
    ORDER BY
      _extract_date DESC

  ), contact_engagements AS (
  SELECT     DISTINCT /* dummy_dates.*, */
      engagements.* EXCEPT(_domain) /* EXCEPT(_date, _week, _year) */,
      contacts.*EXCEPT( _email)
  FROM
  (
SELECT * FROM email_engagement
UNION ALL 
SELECT * FROM form_filled_engagement
UNION ALL 
SELECT * FROM paid_social_engagment
UNION ALL 
SELECT * FROM organic_social_engagment
UNION ALL 
SELECT * FROM webinar_engagement
UNION ALL 
SELECT * FROM web_engagement
UNION ALL 
SELECT * FROM webinar_sign_up
UNION ALL 
SELECT * FROM webinar_attendees 
UNION ALL
SELECT * FROM event_attendees
)engagements
    LEFT JOIN
      contacts USING(_email)
),
  consolidated_engagement AS (

    SELECT * FROM contact_engagements

    
), all_data_join AS (
SELECT 
  DISTINCT consolidated_engagement.*, 
  COALESCE(account_scores. _account_90days_score,0)  AS _account_90days_score,
  --COALESCE(account_scores. _account_90days_score,0) ,
  /*COALESCE(latest_score._account_90days_score,0)*/
  COALESCE(latest_score._account_90days_score,0) AS _latest_account_score,
  /* COALESCE(_weekly_first_party_score, 0) AS _weekly_first_party_score, 
  COALESCE(_ytd_first_party_score, 0) AS _ytd_first_party_score, 
  engagement_grade._weekly_contact_score, 
  engagement_grade._ytd_contact_score,
  engagement_grade._ytd_grade */
FROM 
  consolidated_engagement
LEFT JOIN 
  account_scores 
    ON ( consolidated_engagement._domain = account_scores._domain AND DATE(consolidated_engagement._month ) = account_scores._extract_date ) 
LEFT JOIN 
  account_scores AS latest_score 
    ON (latest_score._domain =  consolidated_engagement._domain AND latest_score._month = EXTRACT(MONTH FROM CURRENT_DATE())  AND latest_score._year = EXTRACT(year FROM CURRENT_DATE()) ) 

WHERE
  LENGTH(consolidated_engagement._domain) > 1
  -- AND consolidated_engagement._year IN ( 2023,2024)
  -- AND consolidated_engagement._domain NOT LIKE '%pubpng.com%'
  -- AND  consolidated_engagement._domain NOT LIKE '%2x.marketing%'
  ---AND LOWER(consolidated_engagement._country) IN ('united states', 'us')
  ---AND consolidated_engagement._domain = '2u.com'
ORDER BY 
  _week DESC
), campaign AS (
SELECT 
COALESCE(CAST(campaign_id AS STRING),list_id) AS _campaignID,
campaign_name, 
notes, 
campaign_topic, 
platform
FROM x-marketing.terrasmart_googlesheet.Campaign_Topic_Grouping c
) 
SELECT all_data_join.*,campaign.* EXCEPT (_campaignID)
FROM all_data_join
LEFT JOIN campaign ON all_data_join._campaignID = campaign._campaignID;
---WHERE _domain = 'centricabusinesssolutions.com'