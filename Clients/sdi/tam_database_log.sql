CREATE OR REPLACE TABLE `sdi.db_icp_database_log` AS
WITH
  suppressed_industry AS (
    SELECT _industry, 1 AS _suppressed FROM
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
    SELECT "Wholesale Trade" )
  )
  , contacts AS (

      SELECT 
          vid AS _id,
          property_email.value AS _email,
          CONCAT(
            COALESCE(INITCAP(property_firstname.value), ''), ' ', COALESCE(INITCAP(property_lastname.value), '')
          ) AS _name,
          COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value) AS _domain, 
          properties.jobtitle.value AS _jobtitle,
          CASE
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
          END AS _seniority,
          properties.job_function.value AS _function,
          property_phone.value AS _phone,
          property_company.value AS _company,
          COALESCE(associated_company.properties.annualrevenue.value, SAFE_CAST(property_annualrevenue.value AS FLOAT64)) AS _revenue,
          COALESCE(associated_company.properties.industry.value, property_industry.value) AS _industry,
          COALESCE(associated_company.properties.numberofemployees.value, property_numberofemployees.value)AS _employee,
          property_city.value AS _city, 
          property_state.value AS _state,
          COALESCE(property_country.value, associated_company.properties.country.value, property_ip_country_code.value) AS _country,
          '' AS _persona,
          property_lifecyclestage.value AS _lifecycleStage,
          property_createdate.value AS _createddate,
          property_salesforceaccountid.value AS _sfdcaccountid,
          property_salesforcecontactid.value AS _sfdccontactid,
          property_salesforceleadid.value AS _sfdcleadid,
          associated_company.company_id AS _company_id,
          property_hs_analytics_source.value AS _original_traffic_source,
          property_hs_object_source_label.value AS _record_source,
          property_hs_marketable_status.value AS _marketing_contact_status,
          property_hs_lead_status.value AS _lead_status,
          property_hs_lifecyclestage_marketingqualifiedlead_date.value AS _lifecyclestage_marketingqualifiedlead_date,
          property_hs_lifecyclestage_salesqualifiedlead_date.value AS _lifecyclestage_salesqualifiedlead_date,
          property_hs_lifecyclestage_opportunity_date.value AS _lifecyclestage_opportunity_date,
          property_hs_lifecyclestage_lead_date.value AS _lifecyclestage_lead_date,
          property_hs_lifecyclestage_customer_date.value AS _lifecyclestage_customer_date,
          property_hs_lifecyclestage_subscriber_date.value AS _lifecyclestage_subscriber_date,
          property_hs_lifecyclestage_other_date.value AS _lifecyclestage_other_date,
          property_utm_campaign.value AS _utm_campaign,
          property_utm_source.value AS _utm_source,
          property_utm_medium.value AS  _utm_medium,
          property_utm_content.value AS _utm_content,
          property_utm_term.value AS _utm_term

        FROM 
          `x-marketing.sdi_hubspot.contacts` hs
  ) 
  SELECT 
      * EXCEPT( _country),
      CASE WHEN LOWER(_country) IN ('us',  'usa', 'united states', 'united states of america') THEN 'US' ELSE _country END AS _country
       FROM contacts
