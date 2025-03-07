CREATE OR REPLACE TABLE
  `x-marketing.terrasmart.db_opportunity_log` AS
WITH
  acc AS (
  SELECT
    name,
    id AS accountid,
    LEFT(main.id, LENGTH(main.id) - 3) AS account_id,
    website,
    type AS prospect_type,
    new_vs_existing_customer__c AS new_vs_existing_customer
  FROM
    `x-marketing.terrasmart_salesforce_alt.Account` main ),
  standardize_account AS (
  SELECT
    DISTINCT database_account_name__standardized_ AS _account,
    salesforce_account_name AS salesforce_account_name,
    account_id,
    CASE
      WHEN database_account_name__standardized_ IS NULL THEN 'Other Account'
      ELSE 'Key Account'
  END
    AS _account_segment,
  FROM
    `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name` key_account ),
  key_account AS (
  SELECT DISTINCT
    _account,
    _domain,
    _persona,
    _fixedtilt,
    _prospect,
    _canopy,
    _otherdomain,
    _tracker,
    _type,
    _utilityprojects,
    _ebos,
    _rep,
    _key_account_year,
    _midwest
  FROM
    `x-marketing.terrasmart_mysql_2.db_key_accounts`
  WHERE
    _key_account_year = '2025' 
    QUALIFY
    ROW_NUMBER() OVER(PARTITION BY _account,
    _domain ORDER BY _account DESC) = 1 
    ) 
  ,
  sandardize_key_account AS (
   SELECT
     COALESCE(standardize_account._account, key_account._account) AS _account,
     COALESCE(standardize_account.salesforce_account_name, key_account._account) AS salesforce_account_name,
      COALESCE(standardize_account.account_id,acc.account_id) AS account_id,
     'Key Account' AS _account_segment,
    key_account.* EXCEPT (_account)
  FROM
    key_account 
  LEFT JOIN
    standardize_account
  ON
    standardize_account._account = key_account._account
  LEFT JOIN acc ON key_account._account = acc.name
  
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY _account,
    _domain,account_id ORDER BY salesforce_account_name DESC) = 1
    ),
  owner AS (
  SELECT
    DISTINCT id,
    name AS owner,
    alias
  FROM
    `x-marketing.terrasmart_salesforce.User` ),
  contact AS (
  SELECT
    DISTINCT id,
    name AS contact,
    email
  FROM
    `x-marketing.terrasmart_salesforce.Contact` ),
  site AS (
  SELECT
    id,
    address__statecode__s,
    name AS address,
    address__street__s,
    address__city__s,
    address__countrycode__s
  FROM
    `x-marketing.terrasmart_salesforce.Site__c` ),
  record AS (
  SELECT
    id,
    name AS recordtype,
    businessprocessid,
    developername
  FROM
    `x-marketing.terrasmart_salesforce.RecordType` ),
  opps_created AS (
  SELECT
    opps.id AS _opportunity_id,
    amount_won__c,
    amount,
    ownerid,
    owner_id_d365__c,
    name opportunity_name,
    project_name__c,
    stagename,
    type,
    account_type__c,
    recordtypeid,
    recordtype,
    site_city__c,
    site_lat_long__c,
    project_site_address__c,
    project_site__c,
    project_site_access__c,
    site_street__c,
    rt_name__c,
    closedate,
    createddate AS _createdate,
    system_size_in_mw__c,
    system_size_mwdc__c,
    system_size_mwdc_conga__c,
    total_system_size_auto_calc__c,
    inverter_string_size__c,
    contact.contact AS contact_name,
    contact.email,
    opps.isdeleted,
    owner.owner,
    alias AS owner_alias,
    accountid,
    LEFT(accountid, LENGTH(accountid) - 3) AS account_id,
    address__statecode__s,
    address,
    address__street__s,
    address__city__s,
    address__countrycode__s,
    EXTRACT(YEAR
    FROM
      createddate) AS _year__opportunity_created,
    DATE_TRUNC(CAST(createddate AS DATE), MONTH) AS _month_opportunity_created,
    DATE_TRUNC(CAST(createddate AS DATE), QUARTER) AS _quater__opportunity_created,
  FROM
    `x-marketing.terrasmart_salesforce.Opportunity` opps
  LEFT JOIN
    owner
  ON
    opps.ownerid = owner.id
  LEFT JOIN
    contact
  ON
    opps.contactid = contact.id
  LEFT JOIN
    site
  ON
    opps.project_site__c = site.id
  LEFT JOIN
    record
  ON
    opps.recordtypeid = record.id
  WHERE
    opps.isdeleted IS FALSE ),
  opp_hist AS(
  SELECT
    stage.*EXCEPT(_opportunity_id,
      createddate),
    opps_created.*,
    -- EXCEPT(_order),
    ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY createddate DESC) AS _order
  FROM (
    SELECT
      DISTINCT opportunityid AS _opportunity_id,
      -- createddate AS _oppLastChangeinStage,
      oldvalue AS _previousstage,
      --probability,
      newvalue AS _currentstage,
      createddate
    FROM
      `x-marketing.terrasmart_salesforce.OpportunityFieldHistory`
    WHERE
      field = 'StageName'
      AND isdeleted IS FALSE ) stage
  RIGHT JOIN
    opps_created
  USING
    (_opportunity_id) --
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY createddate DESC) = 1 )
SELECT
  opp_hist.*,
  COALESCE(sandardize_key_account._account,acc.name) AS _account, 
  COALESCE(sandardize_key_account.salesforce_account_name,acc.name) AS salesforce_account_name,
  _domain,
   _persona,
    _fixedtilt,
    _prospect,
    _canopy,
    _otherdomain,
    _tracker,
    _type,
    _utilityprojects,
    _ebos,
    _rep,
    _key_account_year,
    _midwest,
  CASE
      WHEN _account_segment IS NULL THEN 'Other Account'
      ELSE 'Key Account'
  END
    AS _account_segment,
  website,
  prospect_type,
  new_vs_existing_customer
FROM
  opp_hist
LEFT JOIN
  sandardize_key_account
ON
  opp_hist.account_id = sandardize_key_account.account_id
LEFT JOIN
  acc
ON
  opp_hist.accountid = acc.accountid;


CREATE OR REPLACE TABLE `x-marketing.terrasmart.db_engagement_opportunity` AS 
WITH
  consolidated_account_engagement AS (
  SELECT
    _account,
    _account_segment,
    _type,
    _engagement,
    _contentTitle,
    MAX(_timestamp) AS _latest_engagement_time
  FROM
    `x-marketing.terrasmart.db_consolidated_engagements_log`
  WHERE
    _engagement = "Webinar Attendees"
  GROUP BY
    _account,
    _account_segment,
    _type,
    _engagement,
    _contentTitle 
  ),
  opps_with_engagement AS (
  SELECT
    ot._opportunity_id,
    ot._createdate,
    ot._account,
    ct._latest_engagement_time
  FROM
    `x-marketing.terrasmart.db_opportunity_log` AS ot
  JOIN
    consolidated_account_engagement AS ct
  ON
    ot._account = ct._account 
  ),
  _webinar_engagement AS (
  SELECT
    _opportunity_id,
    MAX(_latest_engagement_time) AS _latest_webinar_engagement_time
  FROM
    opps_with_engagement
  WHERE
    (_createdate > _latest_engagement_time)
    AND DATE_DIFF(CAST(_createdate AS DATE), CAST(_latest_engagement_time AS DATE),DAY) <= 180
  GROUP BY
    _opportunity_id )
SELECT
  ot._opportunity_id,
  ot.amount_won__c,
  ot.amount,
  ot.ownerid,
  ot.owner_id_d365__c,
  ot.opportunity_name,
  ot.project_name__c,
  ot.stagename,
  ot.type,
  ot.account_type__c,
  ot.recordtypeid,
  ot.site_city__c,
  ot.site_lat_long__c,
  ot.project_site_address__c,
  ot.project_site__c,
  ot.project_site_access__c,
  ot.site_street__c,
  ot.rt_name__c,
  ot.closedate,
  ot._createdate,
  ot.system_size_in_mw__c,
  ot.system_size_mwdc__c,
  ot.system_size_mwdc_conga__c,
  ot.total_system_size_auto_calc__c,
  ot.inverter_string_size__c,
  ot.contact_name,
  ot.email,
  ot.isdeleted,
  ot.owner,
  ot.owner_alias,
  ot.accountid,
  ot.account_id,
  ot.address__statecode__s,
  ot.address,
  ot.address__street__s,
  ot.address__city__s,
  ot.address__countrycode__s,
  ot._year__opportunity_created,
  ot._month_opportunity_created,
  ot._quater__opportunity_created,
  ot._order,
  ot._account,
  ot.salesforce_account_name,
  ot._account_segment,
  ot._domain,
  ot._persona,
  ot._fixedtilt,
  ot._prospect,
  ot._canopy,
  ot._otherdomain,
  ot._tracker,
  ot._type,
  ot._utilityprojects,
  ot._ebos,
  ot._rep,
  ot._key_account_year,
  ot._midwest,
  website AS _website,
  prospect_type,
  new_vs_existing_customer,
  recordtype,
  we._latest_webinar_engagement_time
FROM
  `x-marketing.terrasmart.db_opportunity_log` AS ot
LEFT JOIN
  _webinar_engagement AS we
ON
  ot._opportunity_id = we._opportunity_id;


CREATE OR REPLACE TABLE `x-marketing.terrasmart.db_opportunity_with_engagement` AS 
WITH consolidated_account_engagement AS (SELECT  
_account,
_account_segment,
_type,
_engagement,
_contentTitle,
MAX(_timestamp) AS `_latest_engagement_time`
FROM `x-marketing.terrasmart.db_consolidated_engagements_log`
GROUP BY 
_account,
_account_segment,
_type,
_engagement,
_contentTitle
)

SELECT 
ot._opportunity_id,
ot.amount_won__c,
ot.amount,
ot.ownerid,
ot.owner_id_d365__c,
ot.opportunity_name,
ot.project_name__c,
ot.stagename,
ot.type,
ot.account_type__c,
ot.recordtypeid,
ot.site_city__c,
ot.site_lat_long__c,
ot.project_site_address__c,
ot.project_site__c,
ot.project_site_access__c,
ot.site_street__c,
ot.rt_name__c,
ot.closedate,
ot._createdate,
ot.system_size_in_mw__c,
ot.system_size_mwdc__c,
ot.system_size_mwdc_conga__c,
ot.total_system_size_auto_calc__c,
ot.inverter_string_size__c,
ot.contact_name,
ot.email,
ot.isdeleted,
ot.owner,
ot.owner_alias,
ot.accountid,
ot.account_id,
ot.address__statecode__s,
ot.address,
ot.address__street__s,
ot.address__city__s,
ot.address__countrycode__s,
ot._year__opportunity_created,
ot._month_opportunity_created,
ot._quater__opportunity_created,
ot._order,
ot._account,
ot.salesforce_account_name,
ot._account_segment,
ot._domain,
ot._persona,
ot._fixedtilt,
ot._prospect,
ot._canopy,
ot._otherdomain,
ot._tracker,
ot._type,
ot._utilityprojects,
ot._ebos,
ot._rep,
ot._key_account_year,
ot._midwest,
ct._engagement,
ct._contentTitle,
ct._latest_engagement_time,
DATE_DIFF(ot._createdate, ct._latest_engagement_time, DAY) AS _date_different,
 ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY  ct._latest_engagement_time DESC) AS _order_of_engagement
FROM `x-marketing.terrasmart.db_opportunity_log` AS ot
JOIN consolidated_account_engagement AS ct ON ot._account = ct._account
WHERE DATE_DIFF(ot._createdate, ct._latest_engagement_time, DAY) BETWEEN 1 AND 180 ;


CREATE OR REPLACE TABLE `x-marketing.terrasmart.opportunity_with_account_90days_score_2025` AS
with acc AS (
  SELECT name, id AS accountid,LEFT(main.id, LENGTH(main.id) - 3) AS account_id, website,type AS prospect_type
  FROM `x-marketing.terrasmart_salesforce.Account` main
 
 ), opps_created AS (
  SELECT 
  opps.id AS _opportunity_id,
  amount_won__c, 
  amount, 
  ownerid, 
  owner_id_d365__c, 
  name opportunity_name, 
  project_name__c, 
  stagename, 
  type, 
  account_type__c, 
  recordtypeid, 
  site_city__c, 
  site_lat_long__c, 
  project_site_address__c, 
  project_site__c, 
  project_site_access__c, 
  site_street__c,
  rt_name__c, 
   closedate, 
   createddate AS _createdate, 
   system_size_in_mw__c, 
   system_size_mwdc__c, 
   system_size_mwdc_conga__c, 
   total_system_size_auto_calc__c, 
   inverter_string_size__c ,
   contact.contact AS contact_name,
   contact.email,
   opps.isdeleted,
   owner.owner,
   alias AS owner_alias,
   accountid AS account_id,
   address__statecode__s , 
   address , 
   address__street__s, 
   address__city__s, 
   address__countrycode__s,
   EXTRACT(YEAR FROM createddate) AS _year__opportunity_created,
   DATE_TRUNC(CAST(createddate AS DATE), MONTH) AS _month_opportunity_created,
   DATE_TRUNC(CAST(createddate AS DATE), QUARTER) AS _quater__opportunity_created,
  -- ROW_NUMBER() OVER(PARTITION BY opps.id ORDER BY opps.createddate  ASC) AS _order,
  
  FROM `x-marketing.terrasmart_salesforce.Opportunity` opps
  LEFT JOIN (SELECT DISTINCT id, name as owner,alias from `x-marketing.terrasmart_salesforce.User` ) owner 
  ON opps.ownerid = owner.id 
  LEFT JOIN (SELECT DISTINCT id, name as contact, email from `x-marketing.terrasmart_salesforce.Contact` ) contact
  ON opps.contactid = contact.id 
  LEFT JOIN (SELECT id,address__statecode__s , name as address , address__street__s, address__city__s, address__countrycode__s FROM `x-marketing.terrasmart_salesforce.Site__c` ) site ON opps.project_site__c = site.id
   WHERE opps.isdeleted IS FALSE
 ),
opp_hist AS(
  SELECT
    stage.*EXCEPT(_opportunity_id,createddate),
    opps_created.*,
    -- EXCEPT(_order),
    ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY createddate DESC) AS _order
  FROM
  (
    SELECT
      DISTINCT opportunityid AS _opportunity_id,
      -- createddate AS _oppLastChangeinStage,
      oldvalue AS _previousstage,
      --probability,
      newvalue AS _currentstage,
      createddate
    FROM
      `x-marketing.terrasmart_salesforce.OpportunityFieldHistory`
    WHERE 
    --opportunityid = '0062S00000weSXnQAM' AND
      field = 'StageName' 
      AND isdeleted IS FALSE
   /*  ORDER BY
      _oppLastChangeinStage DESC */
  ) stage
  RIGHT JOIN  
    opps_created USING(_opportunity_id)

  -- WHERE
  --   _opportunity_id = '006UW000003os6jYAA'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY createddate DESC) = 1
) , standardize_account AS (
 SELECT *
  FROM
  (
 SELECT 
 DISTINCT
 COALESCE(database_account_name__standardized_, name) AS _account,COALESCE(salesforce_account_name, name) AS salesforce_account_name,
acc.account_id,accountid, 
CASE WHEN database_account_name__standardized_ IS NULL THEN 'Other Account' 
ELSE 'Key Account' END AS _account_segment,
_prospect,
domain._ebos, 
domain._canopy, 
domain._fixedtilt, 
domain._midwest, 
domain._tracker,
domain._type,
prospect_type
 FROM acc 
LEFT JOIN `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name` key_account ON acc.account_id = key_account.account_id
 LEFT JOIN (SELECT DISTINCT _account, _domain, _ebos, _utilityprojects, _tracker, _linkedinurl, _canopy, _prospect, _fixedtilt, _persona, _rep, _midwest, _account_segment, _type
  FROM `x-marketing.terrasmart.account_90days_score_2025`  acc
 ) domain ON COALESCE(database_account_name__standardized_, name) = domain._account
  )
   QUALIFY ROW_NUMBER() OVER(PARTITION BY accountid,prospect_type ORDER BY _midwest DESC) = 1
 
), account_score AS (
  SELECT DISTINCT _account,_account_segment, _domain,_year, _extract_date,_min_quater_date,_monthly_account_score ,_cumulative_quaterly_engagement_score
  FROM `x-marketing.terrasmart.account_90days_score_2025`  acc
  
  --  JOIN `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name` key_account ON key_account.salesforce_account_name = acc._account

), account_score_aggregate AS (
  SELECT 
 _account, _account_segment,_year,SUM(_monthly_account_score) AS _account_score
  FROM `x-marketing.terrasmart.account_90days_score_2025`  acc

   GROUP BY 1,2,3
),dates AS (
  SELECT AS STRUCT
    DATE_SUB(_date, INTERVAL 1 DAY)  AS max_date, 
    DATE_SUB(_date, INTERVAL 1 MONTH) AS min_date,
    DATE_TRUNC(DATE_SUB(_date, INTERVAL 1 MONTH), QUARTER) AS _min_quater_date,
    EXTRACT(YEAR FROM _date) AS _year,
    DATE_ADD(DATE_TRUNC(DATE_SUB(_date, INTERVAL 1 MONTH), QUARTER),INTERVAL 1 QUARTER)-1 AS _max_quater_date,
    
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2011-01-01',DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS _date
  ORDER BY
    1 DESC
), all_account_with_dates AS (
SELECT * 
FROM standardize_account
CROSS JOIN dates
)
, combine_alls AS ( 
  SELECT all_account_with_dates.*,
opp_hist.* EXCEPT (account_id),
score.* EXCEPT(_account,_account_segment,_domain,_year, _extract_date,_min_quater_date), 
aggr.* EXCEPT(_account, _account_segment,_year),

--ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY _createdate DESC) AS _order
FROM all_account_with_dates
LEFT JOIN opp_hist ON CONCAT(all_account_with_dates.accountid,min_date) = CONCAT(opp_hist.account_id,_month_opportunity_created)
LEFT JOIN account_score score ON CONCAT(all_account_with_dates._account,all_account_with_dates.min_date,all_account_with_dates._account_segment)  = CONCAT(score._account,score._extract_date,score._account_segment)
LEFT JOIN account_score_aggregate aggr ON CONCAT(all_account_with_dates._account, all_account_with_dates._account_segment,all_account_with_dates._year)  = CONCAT(aggr._account, aggr._account_segment,aggr._year)
) , avg_combine AS ( 
  SELECT *,
COUNT(account_id) OVER(PARTITION BY _account, _account_segment,min_date ORDER BY min_date DESC) AS _account_with_different_name,
AVG(_monthly_account_score) OVER(PARTITION BY _account, _account_segment,min_date ORDER BY min_date DESC) AS _account_score_monthly ,
COUNT(_opportunity_id)  OVER(PARTITION BY _account, _account_segment,min_date ORDER BY min_date DESC) AS _total_opportunity
FROM combine_alls
--WHERE combine_alls._account = "Ace Solar"
) SELECT * , CASE WHEN _total_opportunity > 0 THEN _account_score_monthly/_total_opportunity ELSE _account_score_monthly END AS _average_score_with_opportunity_month,
SAFE_DIVIDE( _account_score_monthly, _account_with_different_name) AS _account_score_per_account
FROM avg_combine ;