--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

-- No bombora report so it's being excluded.
-- CREATE OR REPLACE TABLE terrasmart.db_icp_database_log;
TRUNCATE TABLE `terrasmart.db_icp_database_log` ;
INSERT INTO `terrasmart.db_icp_database_log`
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
  contacts AS (
    SELECT 
      * EXCEPT( _rownum, _country),
      CASE WHEN LOWER(_country) IN ('us',  'usa', 'united states', 'united states of america') THEN 'US' ELSE _country END AS _country,
      sfcontact.accountid  AS _sfdcaccountid,
    FROM (
      SELECT 
          id AS _id,
          email AS _email,
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
          "" AS _employee,
          city AS _city, 
          state AS _state,
          country AS _country,
          "" AS _persona,
          "" AS _lifecycleStage,
          created_at AS _createddate,
          crm_contact_fid AS _sfdccontactid,
          crm_lead_fid AS _sfdcleadid,
          CASE 
          WHEN crm_contact_fid IS NOT NULL THEN "Contact"
          WHEN crm_contact_fid IS NULL THEN "Lead"
        END AS _contact_type,
          COALESCE(crm_contact_fid, crm_lead_fid) AS _leadorcontactid,
          ROW_NUMBER() OVER( PARTITION BY email ORDER BY _sdc_received_at DESC) AS _rownum,
        FROM 
          `x-marketing.terrasmart_pardot.prospects` 
        WHERE 
         NOT REGEXP_CONTAINS(email, 'terrasmart|2x.marketing') 
        ) main
        LEFT JOIN
    (SELECT id, accountid FROM terrasmart_salesforce.Contact) sfcontact ON (sfcontact.id = main._leadorcontactid AND main._contact_type = 'Contact')
    WHERE 
      _rownum = 1
  )
SELECT 
  DISTINCT _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _phone,
  _company,
  _industry,
  _revenue,
  _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _sfdcaccountid,
  _sfdccontactid,
  _sfdcleadid,
  _leadorcontactid,
  _contact_type,
  _country,
  -- To be enabled when the airtable is updated > update the values accordingly 
  /* IF(
      (_suppressed IS NULL AND (_revenue >= 50000000 OR _employee >= 500) AND _country = 'US') 
      AND (NOT REGEXP_CONTAINS(LOWER(_jobtitle), r"content|design|art|product|brand|writer|analyst") OR _jobtitle IS NULL)
      AND (_seniority IN ('Director', 'C-Level') OR _seniority LIKE '%VP%')
      AND (LOWER(_function) LIKE '%marketing%' OR LOWER(_jobtitle) LIKE '%marketing%'), 
      1, 
      0
    ) */ 0 AS _target_contacts,
  /* IF(_suppressed IS NULL AND (_revenue >= 50000000 OR _employee >= 500) AND _country = 'US', 1, 0)  */ 0 AS _target_accounts,
FROM 
  contacts
/* LEFT JOIN
  suppressed_industry USING(_industry) */
;
  





