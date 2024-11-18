/* 
  Script to run database activation and penetration
 */

-- SELECT SUM(_weekly_account_engagement) FROM `logicsource.dashboard_database_activation`;

-- CREATE OR REPLACE TABLE `logicsource.dashboard_database_activation` AS
TRUNCATE TABLE `logicsource.dashboard_database_activation`;
INSERT INTO `logicsource.dashboard_database_activation` (
  _date,
  _engaged_accounts,
  _contacts_created,
  _account_created
)
WITH 
  dummy_dates AS (
    SELECT
      _date,
      EXTRACT(WEEK FROM _date) AS _week,
      EXTRACT(YEAR FROM _date) AS _year
    FROM 
      UNNEST(GENERATE_DATE_ARRAY('2019-12-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
  ),
  tam_database AS (
  SELECT DISTINCT CAST(vid AS STRING) AS vid, 
property_email.value AS _email,
COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value) AS _domain,
properties.createdate.value AS _createddate 
FROM `x-marketing.logicsource_hubspot.contacts`
  ),
  contacts_created AS (
    SELECT
      DISTINCT DATE(_createddate) AS _createddate,
      COUNT(DISTINCT vid) AS _contacts_created,
    FROM
      tam_database
    GROUP BY 
      1
  ),
  companies AS (
    SELECT MIN(property_createdate.value) AS _createddate,
--property_domain.value 
companyid AS _domain

 FROM `x-marketing.logicsource_hubspot.companies` 
 GROUP BY 2 
  ),
  accounts_created AS (
 SELECT 
      DATE(_createddate) AS _createddate,
      COUNT(DISTINCT _domain) AS _account_created
    FROM companies
    GROUP BY 
      1
  ),
  consolidated_engagements AS (
        SELECT DISTINCT * FROM `x-marketing.logicsource.db_consolidated_engagements_log` 
        --WHERE _engagement IN ('Email Clicked', 'Email Opened', 'Form Filled')
  ),
  consolidated_engagement_dates AS (
    SELECT
      MIN(
        EXTRACT(
          DATE
          FROM _date
        )
      ) AS _date,
      _sfdcaccountid
    FROM consolidated_engagements
    GROUP BY 2
  ),
  account_engagement AS (
    SELECT 
      _date,
      COUNT(DISTINCT _sfdcaccountid) AS _engaged_accounts
    FROM consolidated_engagement_dates
    GROUP BY 
      1
  )
SELECT 
  DISTINCT dummy_dates._date, 
  -- SUM(IF(_hasEngaged IS NULL, 0, _hasEngaged)) OVER(PARTITION BY dummy_dates._date) AS _engaged_accounts,
  COALESCE(_engaged_accounts, 0) AS _engaged_accounts,
  COALESCE(_contacts_created, 0) AS _contacts_created,
  COALESCE(_account_created, 0) AS _account_created
FROM
  dummy_dates
LEFT JOIN
  account_engagement ON dummy_dates._date = account_engagement._date
LEFT JOIN
  accounts_created ON dummy_dates._date = DATE(accounts_created._createddate)
LEFT JOIN
  contacts_created ON dummy_dates._date = DATE(contacts_created._createddate)
ORDER BY 
  _date DESC;