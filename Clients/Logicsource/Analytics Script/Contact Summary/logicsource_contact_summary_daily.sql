--snapshot script (daily 4pm)
--contact summary
INSERT INTO `x-marketing.logicsource.contact_summary_daily` (
  date,
  _prospectid,
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
  leadscore,
  _hubspotscores
)
WITH all_contact AS (
SELECT
    * EXCEPT( _rownum)
  FROM (
    SELECT DISTINCT CURRENT_DATE('Hongkong') AS date,
      CAST(vid AS STRING) AS _prospectid,
      property_email.value AS _email,
      COALESCE(CONCAT(property_firstname.value, ' ', property_lastname.value),property_firstname.value) AS _name,
      /*COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)*/associated_company.properties.domain.value AS _domain,
      properties.jobtitle.value,
      properties.job_function.value AS _function,
     /*CASE
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Assistant to%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Counsel%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%General Counsel%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Founder%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%C-Level%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CDO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CIO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CMO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CFO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CEO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chief%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%coordinator%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%COO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr.VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%srvp%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SR VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%S.V.P%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vp%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec VP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive Vice President%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%EVP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%E.V.P%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SVP%") THEN "Senior VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V.P%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%VP%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Vice Pres%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V P%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%President%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Director%") THEN "Director"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CTO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir%") THEN "Director"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir.%") THEN "Director"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MDR%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MD%") THEN "Director"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%GM%") THEN "Director"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Head%") THEN "VP"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Manager%") THEN "Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%escrow%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%cross%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%crosse%") THEN "Non-Manager"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Partner%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CRO%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chairman%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Owner%") THEN "C-Level"
        WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Team Lead%") THEN "Manager"
      END*/  CASE WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value ELSE property_management_level.value END AS _seniority,
      property_phone.value AS _phone,
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      associated_company.properties.industry.value AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
      l.lead_score__c AS leadscore,
      property_hubspotscore.value,
      ROW_NUMBER() OVER(
          PARTITION BY vid,property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value)
          ORDER BY vid DESC
      ) AS _rownum
    FROM
      `x-marketing.logicsource_hubspot.contacts` k
      LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
    --WHERE
      --property_email.value IS NOT NULL
      --AND property_email.value NOT LIKE '%2x.marketing%'
      --AND property_email.value NOT LIKE '%logicsource%' 
  )
  WHERE
    _rownum = 1
    --AND _domain NOT IN ('logicsource.com',
      --'logicsource',
      --'2x.marketing'
    --) 
) SELECT * FROM all_contact 
WHERE date NOT IN (
    SELECT DISTINCT
        date
    FROM `x-marketing.logicsource.contact_summary_daily`
)
;

CREATE OR REPLACE TABLE `x-marketing.logicsource.contact_summary_leadscore` AS 
WITH current_week AS (
SELECT  d.date, CAST(vid AS STRING) AS _prospectid,
      property_email.value AS _email,
      COALESCE(CONCAT(property_firstname.value, ' ', property_lastname.value),property_firstname.value) AS _name,
      /*COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)*/associated_company.properties.domain.value AS _domain,
      properties.jobtitle.value AS jobtitle,
      properties.job_function.value AS _function,
     CASE WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value ELSE property_management_level.value END AS _seniority,
      property_phone.value AS _phone,
      associated_company.properties.name.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      associated_company.properties.industry.value AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
     d.leadscore,_title,
      CASE WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value ELSE property_job_role.value END AS _jobrole,
       properties.hs_lead_status.value AS _leadstatus,
          property_leadstatus.value AS _property_leadstatus, 
      _hubspotscore, _hubspotscores,associated_company.company_id,
DATE_SUB(date, INTERVAL 1 WEEK ) last_week_date 
FROM `x-marketing.logicsource.contact_summary_daily` d
LEFT JOIN `x-marketing.logicsource_hubspot.contacts` c ON d._prospectid = CAST(c.vid AS STRING)
WHERE --d._prospectid NOT IN ( '1009904', '1007951' , '943501', '71451', '251', '285405', '1008603', '1008051', '1007853', '1009301', '1010002', '1009103') AND 
TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(properties.date_stamp.value AS INT64) / 1000 AS INT64))) = (SELECT MAX(TIMESTAMP(TIMESTAMP_SECONDS(CAST(CAST(properties.date_stamp.value AS INT64) / 1000 AS INT64)))) FROM `x-marketing.logicsource_hubspot.contacts`  ) AND property_found_in_hubspot.value = 'true'
), last_week AS (
  SELECT  *,
DATE_ADD(date, INTERVAL 1 WEEK) next_week_date 
FROM `x-marketing.logicsource.contact_summary_daily` 
)SELECT current_week.*,
last_week._hubspotscores AS _last_week_hubspotscores,
last_week._hubspotscore AS _last_week_hubspotscore
FROM 
current_week 
 LEFT JOIN last_week ON current_week.last_week_date = last_week.date AND current_week._prospectid = last_week._prospectid;

--account summary
INSERT INTO `x-marketing.logicsource.account_summary_daily` (
    date,
    _companyID,
    _domain,
    _accountindustry,
    _accountrevenue,
    _accountName,
    _totalContacts,
    _contactTouched,
    _accountTouched,
    _campaign_record,
    _emailOpen, 
    _emailClick, 
    _ads_download,
    _delivered
)
with all_contacts AS (
    SELECT  
    CAST(companyid AS STRING) AS companyid, 
    companies.properties.domain.value AS domain, 
    companies.property_industry.value As industry, 
    CAST(companies.property_annualrevenue.value AS STRING) AS annualRevenue, 
    property_name.value AS companyName, 
    COUNT(vid) AS _totalContacts,
    FROM `x-marketing.logicsource_hubspot.companies` companies
    LEFT JOIN `x-marketing.logicsource_hubspot.contacts` contacts ON companies.companyid = contacts.associated_company.company_id
    WHERE property_email.value IS NOT NULL
    GROUP BY 1, 2, 3, 4,5
    ORDER BY COUNT(vid) DESC

), contacts_touched AS (

    SELECT _company AS company, COUNT(DISTINCT(LOWER(_email))) _ct 
    FROM `x-marketing.logicsource.db_email_engagements_log` d
    WHERE _engagement = 'Opened' AND _email IS NOT NULL
    GROUP BY company

), account_touched AS (
    SELECT _company AS company, COUNT(DISTINCT(_company)) _ct 
    FROM `x-marketing.logicsource.db_email_engagements_log` d
    WHERE _engagement = 'Opened' AND _email IS NOT NULL
    GROUP BY company

),campaign_engagement AS (
    SELECT STRING_AGG( DISTINCT _contentTitle order by _contentTitle) campaign_record,
    _company AS company, 
    COUNT(_company) _ct 
    FROM `x-marketing.logicsource.db_email_engagements_log` d
    GROUP BY company
    order by company asc
),engagement AS (
    SELECT 
        _company AS company, 
        SUM(CASE WHEN _engagement = 'Opened'THEN 1 ELSE 0 END), 
        SUM(CASE WHEN _engagement = 'Clicked'THEN 1 ELSE 0 END), 
        SUM(CASE WHEN _engagement = 'Downloaded'THEN 1 ELSE 0 END),
        SUM(CASE WHEN _engagement = 'Delivered'THEN 1 ELSE 0 END)
    FROM `x-marketing.logicsource.db_email_engagements_log` d
    GROUP BY 1
)
SELECT DISTINCT CURRENT_DATE('Hongkong') AS date,
all_contacts.*,
contacts_touched._ct,
account_touched._ct,
campaign_engagement.campaign_record,
engagement.* EXCEPT (company)
FROM all_contacts
LEFT JOIN contacts_touched ON all_contacts.companyName = contacts_touched.company
LEFT JOIN account_touched ON all_contacts.companyName = account_touched.company
LEFT JOIN campaign_engagement ON all_contacts.companyName = campaign_engagement.company
LEFT JOIN engagement ON all_contacts.companyName = engagement.company ; 