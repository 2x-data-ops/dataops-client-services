--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

-- No bombora report so it's being excluded.
-- CREATE OR REPLACE TABLE faro.db_icp_database_log;
TRUNCATE TABLE `faro.db_icp_database_log` ;
INSERT INTO `faro.db_icp_database_log`
WITH
  -- To be filled up if required
  /* suppressed_industry AS (
    SELECT 
      DISTINCT _industry, 1 AS _suppressed 
    FROM
    (
      SELECT "Air Transportation" AS _industry UNION DISTINCT
      SELECT "Apparel Manufacturing and Fashion Industry" UNION DISTINCT
      SELECT "Architecture and Planning" UNION DISTINCT
      SELECT "Artists, Writers, and Performers" UNION DISTINCT
      SELECT "Arts, Entertainment and Recreation" UNION DISTINCT
      SELECT "Automobile / Vehicle / Original Equipment Manufacturing" UNION DISTINCT
      SELECT "Banking" UNION DISTINCT
      SELECT "Book, Magazine, and Other Publishing Companies" UNION DISTINCT
      SELECT "Breweries, Wineries, Distilleries, Spirits Manufacturing" UNION DISTINCT
      SELECT "Broadcast Media" UNION DISTINCT
      SELECT "Building Materials" UNION DISTINCT
      SELECT "Chemical Manufacturing" UNION DISTINCT
      SELECT "Civic and Social Organization" UNION DISTINCT
      SELECT "Civil Engineering" UNION DISTINCT
      SELECT "Construction" UNION DISTINCT
      SELECT "Consumer Electronics" UNION DISTINCT
      SELECT "Consumer Services" UNION DISTINCT
      SELECT "Cosmetics / Skin Care and Beauty Products" UNION DISTINCT
      SELECT "Design Services" UNION DISTINCT
      SELECT "Educational Services" UNION DISTINCT
      SELECT "Entertainment" UNION DISTINCT
      SELECT "Environmental Services" UNION DISTINCT
      SELECT "Events Services" UNION DISTINCT
      SELECT "Facilities Services" UNION DISTINCT
      SELECT "Facilities Support Services" UNION DISTINCT
      SELECT "Farming and Crop Production" UNION DISTINCT
      SELECT "Financial Services" UNION DISTINCT
      SELECT "Fitness and Recreational Facilities" UNION DISTINCT
      SELECT "Food and Beverages" UNION DISTINCT
      SELECT "Fundraising" UNION DISTINCT
      SELECT "Furniture and Fixtures" UNION DISTINCT
      SELECT "Gambling and Casinos" UNION DISTINCT
      SELECT "Gaming Services" UNION DISTINCT
      SELECT "Glass, Ceramics and Concrete" UNION DISTINCT
      SELECT "Government Administration Services" UNION DISTINCT
      SELECT "Hospitals and Healthcare" UNION DISTINCT
      SELECT "Heavy and Civil Engineering Construction" UNION DISTINCT
      SELECT "Interior, Industrial, Creative Design Services" UNION DISTINCT
      SELECT "International Trade and Development" UNION DISTINCT
      SELECT "Internet Publishers" UNION DISTINCT
      SELECT "Legal Services" UNION DISTINCT
      SELECT "Leisure, Travel and Tourism" UNION DISTINCT
      SELECT "Luxury Goods and Jewelry" UNION DISTINCT
      SELECT "Machinery Manufacturing" UNION DISTINCT
      SELECT "Medical Devices" UNION DISTINCT
      SELECT "Mining and Metals" UNION DISTINCT
      SELECT "Newspaper Publishers" UNION DISTINCT
      SELECT "Non Profit Organization Management" UNION DISTINCT
      SELECT "Office Equipment - Retail Trade" UNION DISTINCT
      SELECT "Online Audio and Video Media Services" UNION DISTINCT
      SELECT "Other" UNION DISTINCT
      SELECT "Other Information Services" UNION DISTINCT
      SELECT "Other Justice, Public Order, and Safety Activities" UNION DISTINCT
      SELECT "Package / Freight Delivery Service" UNION DISTINCT
      SELECT "Packaging and Containers" UNION DISTINCT
      SELECT "Paper and Related Products" UNION DISTINCT
      SELECT "Performing Art Companies" UNION DISTINCT
      SELECT "Pharmaceutical and Medicine Manufacturing" UNION DISTINCT
      SELECT "Photography" UNION DISTINCT
      SELECT "Primary, Secondary amd Higher Education" UNION DISTINCT
      SELECT "Printing Services" UNION DISTINCT
      SELECT "Professional Training and Coaching" UNION DISTINCT
      SELECT "Public Relations and Communications" UNION DISTINCT
      SELECT "Publishing Industries" UNION DISTINCT
      SELECT "Real Estate Services" UNION DISTINCT
      SELECT "Renewables and Environment" UNION DISTINCT
      SELECT "Restaurants, Bars, and Food Services" UNION DISTINCT
      SELECT "Retail" UNION DISTINCT
      SELECT "Retail Trade" UNION DISTINCT
      SELECT "Semiconductor and Other Electronic Component Manufacturing" UNION DISTINCT
      SELECT "Semiconductor Products" UNION DISTINCT
      SELECT "Spectator Sports Services" UNION DISTINCT
      SELECT "Sports" UNION DISTINCT
      SELECT "Textile Manufacturing - Fiber / Yarn / Thread Mills" UNION DISTINCT
      SELECT "Think Tanks Services" UNION DISTINCT
      SELECT "Transportation / Trucking / Transit / Railroad" UNION DISTINCT
      SELECT "Transportation, Logistics and Warehousing" UNION DISTINCT
      SELECT "Trucking / Railroad" UNION DISTINCT
      SELECT "Utilities" UNION DISTINCT
      SELECT "Warehousing and Storage Service" UNION DISTINCT
      SELECT "Waste Management and Remediation Services" UNION DISTINCT
      SELECT "Wholesale Trade" 
    )
  ), */
  market_segment AS (
    SELECT * EXCEPT(rownum) 
    FROM (
      SELECT 
        leadorcontactid, 
        pull_market_segment__c,
        ROW_NUMBER() OVER(PARTITION BY leadorcontactid, pull_market_segment__c ORDER BY lastmodifieddate DESC) AS rownum
      FROM `faro_salesforce.CampaignMember` main
    ) WHERE rownum = 1
  ),
  contacts AS (
    SELECT * EXCEPT(rownum)
    FROM (
      SELECT
        prospect.email AS _email,
        CONCAT(prospect.first_name, ' ', prospect.last_name) AS _name,
        prospect.job_title AS _title,
        CAST(NULL AS STRING) AS _seniority,
        prospect.company AS _company,
        prospect.industry AS _industry,
        prospect.annual_revenue AS _revenuerange,
        prospect.employees AS _employees,
        prospect.city AS _city,
        prospect.state AS _state,
        prospect.country AS _country,
        prospect.crm_lead_fid AS _sfdcLeadid,
        prospect.crm_contact_fid AS _sfdcContactid,
        prospect.crm_owner_fid AS _sfdcOwnerid,
        prospect.source AS _source,
        COALESCE(cnt.leadsource, ld.leadsource) AS _lead_source,
        COALESCE(cnt.waterfall_stage__c, ld.waterfall_stage__c) AS _waterfall_stage,
        COALESCE(cnt.division_region__c, ld.division_region__c) AS _division_region,
        pull_market_segment__c AS _market_segment,
        ROW_NUMBER() OVER(
          PARTITION BY prospect.email
          ORDER BY prospect.email DESC
        ) AS rownum
      FROM
        `x-marketing.faro_pardot.prospects` prospect
      LEFT JOIN 
        `faro_salesforce.Contact` cnt ON prospect.crm_contact_fid = cnt.id
      LEFT JOIN 
        `faro_salesforce.Lead` ld ON prospect.crm_lead_fid = ld.id
      LEFT JOIN 
        market_segment ON COALESCE(crm_contact_fid, crm_lead_fid) = market_segment.leadorcontactid
      -- LEFT JOIN
      --   seniority
      -- ON
      --   prospect.job_title LIKE CONCAT ('%',seniority.title,'%')
    )
    WHERE rownum = 1
  )
SELECT 
  DISTINCT 
  _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _phone,
  _company,
  _industry,
  CAST(_revenue AS STRING) _revenue,
  CAST(_employee AS STRING) _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _sfdcaccountid,
  _sfdccontactid,
  _sfdcleadid,
  _country,
  -- _targetAccount,
  -- To be enabled when the airtable is updated > update the values accordingly 
  IF(
    (CAST(_employee AS INT64) BETWEEN 1000 AND 10000)
    OR (CAST(_revenue AS FLOAT64) BETWEEN 1000000000 AND 10000000000)
    OR (LOWER(_industry) LIKE "%financ%"
          OR LOWER(_industry) LIKE "%health%"
          OR (LOWER(_industry) LIKE "%high%"AND LOWER(_industry) LIKE "%edu%")
          OR LOWER(_industry) LIKE "%energy%"
          OR LOWER(_industry) LIKE "%gov%")
    OR (_function LIKE "%Security / Risk / Compliance%")
    OR ((LOWER(_jobtitle) LIKE "%ciso%"
          OR LOWER(_jobtitle) LIKE "%security%"
          AND (LOWER(_seniority) LIKE "%senior%" OR LOWER(_seniority) LIKE "%sr%")))
    OR (_region LIKE "EMEA" 
          OR _country = "United States" 
          OR LOWER(_country) = "us" 
          OR _country = "Canada" 
          OR _country = "Australia" 
          OR _country = "New Zealand"),
    1,
    0
  ) AS _target_contacts,
  /* IF(_suppressed IS NULL AND (_revenue >= 50000000 OR _employee >= 500) AND _country = 'US', 1, 0)  0*/  
  IF(
    _targetAccount = true
    OR (CAST(_employee AS INT64) BETWEEN 1000 AND 10000)
    OR (CAST(_revenue AS FLOAT64) BETWEEN 1000000000 AND 10000000000)
    OR (LOWER(_industry) LIKE "%financ%"
          OR LOWER(_industry) LIKE "%health%"
          OR (LOWER(_industry) LIKE "%high%"AND LOWER(_industry) LIKE "%edu%")
          OR LOWER(_industry) LIKE "%energy%"
          OR LOWER(_industry) LIKE "%gov%")
    OR (_region LIKE "EMEA" 
          OR _country = "United States" 
          OR LOWER(_country) = "us" 
          OR _country = "Canada" 
          OR _country = "Australia" 
          OR _country = "New Zealand"),
    1,
    0
  ) AS _target_accounts,
FROM 
  contacts
/* LEFT JOIN
  suppressed_industry USING(_industry) */
;
  





