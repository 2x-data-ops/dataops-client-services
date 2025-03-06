CREATE OR REPLACE TABLE `x-marketing.terrasmart.opportunity_with_account_90days_score` AS
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
  FROM `x-marketing.terrasmart.account_90days_score` acc
 ) domain ON COALESCE(database_account_name__standardized_, name) = domain._account
  )
   QUALIFY ROW_NUMBER() OVER(PARTITION BY accountid,prospect_type ORDER BY _midwest DESC) = 1
 
), account_score AS (
  SELECT DISTINCT _account,_account_segment, _domain,_year, _extract_date,_min_quater_date,_monthly_account_score ,_cumulative_quaterly_engagement_score
  FROM `x-marketing.terrasmart.account_90days_score` acc
  
  --  JOIN `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name` key_account ON key_account.salesforce_account_name = acc._account

), account_score_aggregate AS (
  SELECT 
 _account, _account_segment,_year,SUM(_monthly_account_score) AS _account_score
  FROM `x-marketing.terrasmart.account_90days_score` acc

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


CREATE OR REPLACE TABLE `x-marketing.terrasmart.account_contacts_score` AS
WITH owner AS (
  SELECT name AS ownername , id AS  ownerid
  FROM `x-marketing.terrasmart_salesforce.User`
),contact AS (
 SELECT 
 email, 
 id, 
 name, 
 accountid, 
 cnt.ownerid AS contactownerid, 
 --ownername AS contactowner,
 title, 
 phone, 
 leadsource 
 --,isdeleted
 FROM `x-marketing.terrasmart_salesforce.Contact` cnt
 --LEFT JOIN owner ON cnt.ownerid = owner.ownerid
 WHERE isdeleted IS false

), account AS (
  SELECT 
  id AS accountid, 
  name AS accountname, 
  tier__c, 
  website,
  acc.ownerid, 
  owner.ownername,
  type,
  LEFT(id, LENGTH(id) - 3) AS account_id
  FROM `x-marketing.terrasmart_salesforce.Account` acc
  LEFT JOIN owner ON acc.ownerid = owner.ownerid
), standardize_name AS (
  SELECT account_id , 
  tracker, 
  canopy, 
  midwest, 
  fixed_tilt,
  prospect, 
  salesforce_account_name, 
  ebos_field_fabricated, 
  ebos, 
  CASE WHEN database_account_name__standardized_ = 'Professional Electrical Contractors of CT (PEC)' THEN "Professional Electrical Contractors of CT  (PEC)" ELSE database_account_name__standardized_ END AS database_account_name__standardized_ 
  FROM `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name`
), account_contact AS (
   SELECT 
contact.* ,
account.* EXCEPT (accountid)
FROM contact 
LEFT JOIN account ON contact.accountid = account.accountid
) , account_score_aggregate AS (
  SELECT 
 _account, 
 _account_segment,
 SUM(_monthly_account_score) AS _account_score,
CASE WHEN SUM(_monthly_account_score) >= 35 THEN "High"
WHEN  SUM(_monthly_account_score) > 19 AND SUM(_monthly_account_score) < 35 THEN  "Medium"
WHEN SUM(_monthly_account_score) = 0 THEn "No Engagement" 
ELSE "Low" END as _engagement_level
  FROM `x-marketing.terrasmart.account_90days_score` acc
  WHERE _account_segment = 'Key Account' AND _year >= 2024
   GROUP BY 1,2
), std AS (
  SELECT std_name.*,
_account_segment,
_account_score,
 _engagement_level
FROM standardize_name std_name
LEFT JOIN account_score_aggregate aggr ON std_name.database_account_name__standardized_ = aggr._account
)
--,account_contact_std AS (
  SELECT account_contact.*,
standardize_name.* EXCEPT (account_id,_account_segment),
CASE WHEN database_account_name__standardized_ IS NULL THEN 'Other Account' 
ELSE 'Key Account' END AS _account_segment
 FROM account_contact 
FULL JOIN std standardize_name ON account_contact.account_id = standardize_name.account_id;

CREATE OR REPLACE TABLE `x-marketing.terrasmart.account_lead_score` AS
WITH owner AS (
  SELECT name AS ownername , id AS  ownerid
  FROM `x-marketing.terrasmart_salesforce.User`
), account AS (
  SELECT 
  id AS accountid, 
  name AS accountname, 
  tier__c, 
  website,
  acc.ownerid, 
  owner.ownername,
  type,
  LEFT(id, LENGTH(id) - 3) AS account_id
  FROM `x-marketing.terrasmart_salesforce.Account` acc
  LEFT JOIN owner ON acc.ownerid = owner.ownerid
)
,lead AS (
 SELECT 
 id, 
 LEFT(id, LENGTH(id) - 3) AS _leadid,
 email, 
 industry, 
 lead_id_long__c, 
 db_lead_age__c, 
 createdbyid, 
 email_opt_in__c, 
 name, 
 isconverted, 
 convertedaccountid, 
 accountname,
 seniority__c, 
 account_database__c, 
 ld.ownerid,
 owner.ownername, 
 most_recent_downloads__c, 
 target_market__c, 
 title, 
 phone, 
 state, 
 leadsource, 
 job_function__c, 
 annual_revenue_estimate__c, interested_in__c, company, numberofemployees,
 createddate 
 FROM `x-marketing.terrasmart_salesforce_alt.Lead` ld
 LEFT JOIN owner ON ld.ownerid = owner.ownerid
 LEFT JOIN account ON ld.convertedaccountid = account.accountid
 WHERE isdeleted IS False
 ) , std AS ( 
  SELECT 
  database_account_name__standardized_,
   ebos, salesforce_account_link, ebos_field_fabricated, salesforce_account_name, prospect, fixed_tilt, midwest, canopy, tracker, account_id, record_type FROM `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name__Leads_`
) , account_score_aggregate AS (
  SELECT 
 _account, 
 _account_segment,
 SUM(_monthly_account_score) AS _account_score,
 CASE WHEN SUM(_monthly_account_score) >= 35 THEN "High"
 WHEN  SUM(_monthly_account_score) > 19 AND SUM(_monthly_account_score) < 35 THEN  "Medium"
 WHEN SUM(_monthly_account_score) = 0 THEn "No Engagement" 
 ELSE "Low" END as _engagement_level
 FROM `x-marketing.terrasmart.account_90days_score` acc
  WHERE _account_segment = 'Key Account' AND _year >= 2024
   GROUP BY 1,2
) , standardize_name AS (
SELECT std_name.*,
_account_segment,
_account_score,
 _engagement_level
FROM std std_name
LEFT JOIN account_score_aggregate aggr ON std_name.database_account_name__standardized_ = aggr._account
)
--, lead_account AS (
    SELECT *,
    CASE WHEN database_account_name__standardized_ IS NULL THEN 'Other Account' 
    ELSE 'Key Account' END AS _account_segment
    FROM 
    (
      SELECT lead.* ,
      std.* EXCEPT (account_id,_account_segment),
      FROM lead 
      LEFT JOIN standardize_name  std ON LOWER(lead.company) = LOWER(std.salesforce_account_name)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY createddate DESC) = 1
    ) 
-- )SELECT 
-- lead_account.*,
-- aggr.* EXCEPT (_account,_account_segment)
-- FROM lead_account 
-- LEFT JOIN standardize_name aggr ON CONCAT(lead_account.accountname, lead_account._account_segment)  = CONCAT(aggr._account, aggr._account_segment);





