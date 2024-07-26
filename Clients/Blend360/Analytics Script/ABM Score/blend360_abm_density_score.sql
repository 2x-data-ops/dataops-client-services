---Engagement Score Model

CREATE OR REPLACE TABLE `x-marketing.blend360.abm_score_engagement` AS
WITH impression AS  
  (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        a.ad_name,
        a.icp_tier,
        a.customer_segment,
        "Impressions - FD" AS _engagement,
        "" AS _id,
      SUM(impressions_delivered) AS _impressions,
      '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.FD_Account_Data` ON fd_name = company
    WHERE impressions_delivered iS NOT NULL
    GROUP BY 1,2,3,4,5
    
    UNION ALL 
    SELECT 
       std_name AS _standardizedcompanyname,
       new_industry,
       ad_name,
       icp_tier,
       customer_segment,
       "Impressions - LI" AS _engagement,
         "" AS _id,
       SUM(impressions) AS _impressions,
       '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.LI_Account_Engagement` ON li_name = li_company_name
    WHERE impressions iS NOT NULL
    GROUP BY 1,2,3,4,5
    UNION ALL 
     SELECT 
       std_name AS _standardizedcompanyname,
       a.new_industry,
       a.ad_name,
       a.icp_tier,
       a.customer_segment,
       "Impressions - DB" AS _engagement,
         "" AS _id,
       SUM(impressions) AS _impressions,
       '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.DB_Domain_Summary` ON db_name = domain_name
     WHERE impressions iS NOT NULL
    GROUP BY 1,2,3,4,5
  ), click AS (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        a.ad_name,
        a.icp_tier,
        a.customer_segment,
        "Clicked - FD" AS _engagement,
          "" AS _id,
        SUM(clicks_delivered) AS  _clicks,
        '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.FD_Account_Data` ON fd_name = company
    WHERE clicks_delivered iS NOT NULL AND clicks_delivered >=1
    GROUP BY 1,2,3,4,5
    
    UNION ALL 
    SELECT 
       std_name AS _standardizedcompanyname,
       new_industry,
       ad_name,
       icp_tier,
       customer_segment,
       "Clicked - LI" AS _engagement,
         "" AS _id,
      SUM(CAST(ad_engagements AS INT64)) AS fd_click_delivered,
      '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.LI_Account_Engagement` ON li_name = li_company_name
    WHERE CAST(ad_engagements AS INT64) iS NOT NULL
    GROUP BY 1,2,3,4,5
    UNION  ALL 
     SELECT 
       std_name AS _standardizedcompanyname,
       a.new_industry,
       a.ad_name,
       a.icp_tier,
       a.customer_segment,
       "Clicked - DB" AS _engagement,
         "" AS _id,
       SUM(clicks) AS _click,
       '',
      CURRENT_DATE (),
      '' AS _page_category,
      '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.DB_Domain_Summary` ON db_name = domain_name
     WHERE clicks iS NOT NULL AND clicks >= 1
    GROUP BY 1,2,3,4,5
  ), awareness_score_type AS (
     SELECT  *, "Awareness Score" AS score_type FROM  impression
  UNION  ALL 
  SELECT  *,"Awareness Score" AS score_type FROM  click
  ), 
  
  webtraffic AS (
     SELECT * , 
 CASE 
 WHEN REGEXP_CONTAINS(_Page_Category, '1.0') THEN 'Blend 1.0'
 WHEN _Page_Category IN ('Responsible AI', 'Artificial Intelligence') THEN 'AI' 
 WHEN _Page_Category IN ('Business Intelligence', 'Data Science', 'Data Engineering', 'Data Governance', 'MLOps') THEN 'Capabilities'
 WHEN _Page_Category IN ('Healthcare & Life Science', 'Financial Services', 'Energy', 'Retail', 'Tech, Telecom & Media', 'Travel & Hospitality') THEN 'Industries'
 WHEN _Page_Category IN ('Customer Experience', 'Supply Chain', 'Product', 'Enterprise Operations') THEN 'Domain'
 WHEN _Page_Category IN ('Culture', 'Careers', 'Leadership Team', 'Job Board', 'All Stars') THEN 'Life @ Blend'
 WHEN _Page_Category IN ('News', 'Team Highlights', 'Case Studies', 'Thought Leadership') THEN 'Insights'
 WHEN _Page_Category IN ('About Us', 'Our Journey', 'Awards & Recognition' ,'Partners') THEN 'About'
 WHEN _Page_Category IN ('Contact Us') THEN 'Contact Us'
 WHEN _Page_Category = 'Home Page' THEN 'Home Page'
 WHEN _Page_Category = 'Event LP' THEN 'Event LP'
 ELSE 'Others' END AS _Page_group

 FROM (
 SELECT 
        std_name  AS _standardizedcompanyname,
        new_industry,
        ad_name,
        icp_tier,
        customer_segment,
        "Dealfront - Web Visit"  AS _engagement,
       _visitid,
       NULL,
       _url,
       PARSE_DATE('%m/%d/%Y',_visitstartdate) AS _visitstartdate,
        CASE 
         WHEN REGEXP_CONTAINS(_url, '/about-us') AND NOT REGEXP_CONTAINS(_url, '/about/') AND NOT REGEXP_CONTAINS(_url, '/about-us/') THEN 'About Us'
         WHEN REGEXP_CONTAINS(_url, '/about-us/') THEN '1.0 About Us'
         WHEN REGEXP_CONTAINS(_url, '/about/') THEN '1.0 About Us'
         WHEN REGEXP_CONTAINS(_url, '/careers') AND NOT REGEXP_CONTAINS(_url,'=')  AND NOT REGEXP_CONTAINS(_url,'solutions') AND NOT REGEXP_CONTAINS(_url, '/zoherecruit.in') AND NOT REGEXP_CONTAINS(_url,'quality-assurance-automation-engineer-remote') AND NOT REGEXP_CONTAINS(_url,'jobdetails') THEN 'Careers'
         WHEN _url LIKE '%/capabilities/business-intelligence%' THEN 'Business Intelligence'
         WHEN _url LIKE '%/capabilities/data-science%' THEN 'Data Science'
         WHEN _url LIKE  '%/capabilities/data-engineering%' THEN 'Data Engineering'
         WHEN _url LIKE '%/capabilities/data-governance%' THEN 'Data Governance'
         WHEN _url LIKE '%/capabilities/mlops%' THEN 'MLOps'
         WHEN _url LIKE '%/industries/healthcare-life-sciences%' THEN 'Healthcare & Life Science'  
         WHEN _url LIKE '%/industries/financial-services%' THEN 'Financial Services'
         WHEN _url LIKE '%/industries/energy%' THEN 'Energy'
         WHEN _url LIKE '%/industries/retail&' THEN 'Retail'
         WHEN _url LIKE 'www.blend360.com/industries/retail' THEN 'Retail'
         WHEN _url LIKE '%/industries/tech-telecom-media%' THEN 'Tech, Telecom & Media'
         WHEN _url LIKE '%/industries/travel-hospitality%' THEN 'Travel & Hospitality'
         WHEN _url LIKE '%/jobs%' THEN 'Job Board'
         WHEN _url LIKE '%/all-star%' THEN 'All Stars'
         WHEN _url LIKE '%/culture%' THEN 'Culture'
         WHEN _url LIKE '%/our-journey%' THEN 'Our Journey'
         WHEN _url LIKE '%/awards-recognition%' THEN 'Awards & Recognition'
         WHEN _url LIKE '%/partners%' THEN 'Partners'
         WHEN _url LIKE '%/artificial-intelligence%' THEN 'Artificial Intelligence'
         WHEN _url LIKE '%/responsible-ai%' THEN 'Responsible AI'
         WHEN _url LIKE '%/domain/customer-experience%' THEN 'Customer Experience'
         WHEN _url LIKE '%/domain/supply-chain%' THEN 'Supply Chain'
         WHEN _url LIKE '%/domain/product%' THEN 'Product'
         WHEN _url LIKE '%/domain/enterprise-operations%' THEN 'Enterprise Operations'
         WHEN _url LIKE '%/contact%' THEN 'Contact Us'
         WHEN _url LIKE '%www.blend360.com/' THEN 'Home Page'
         WHEN _url LIKE 'www.blend360.com/news' THEN 'News'
         WHEN _url LIKE 'www.blend360.com/news/%' THEN 'News'
         WHEN REGEXP_CONTAINS(_url,'/news') AND NOT REGEXP_CONTAINS(_url, 'honored-as-pioneer') AND NOT REGEXP_CONTAINS(_url, '/sample') AND NOT REGEXP_CONTAINS(_url, 'announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_url, '-the-7th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_url, 'opening-new-offices-denver-colorado') AND NOT REGEXP_CONTAINS(_url, '-announces-suite-of-new-generative-ai-features-to-drive-clients-business-performance') AND NOT REGEXP_CONTAINS(_url, '-announces-the-launch-of-a-denver-delivery-center-hiring-100-people-to-continue-the-hyper-growth-in-data-science-solutions-business') AND NOT REGEXP_CONTAINS(_url, 'ai-sparks') AND NOT REGEXP_CONTAINS(_url, 'for-the-8th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_url, 'rss.xml') AND NOT REGEXP_CONTAINS(_url, '/insights') AND NOT REGEXP_CONTAINS(_url, '/feed') AND NOT REGEXP_CONTAINS(_url, '/atom') AND NOT REGEXP_CONTAINS(_url, '-announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_url,'=') AND REGEXP_CONTAINS(_url,'/news/blend-earns-2023-great-place-to-work-certification-tm') THEN 'News'
         WHEN REGEXP_CONTAINS(_url, '/team-highlights') AND NOT REGEXP_CONTAINS(_url, 'consultant-spotlight-from-battlefield-to-boardroom-ismaels-inspirational-journey') AND NOT REGEXP_CONTAINS(_url, '-appoints-rebekah-hudson-as-vice-president-of-business-development-for-north-americaaeu') AND NOT REGEXP_CONTAINS(_url, '-appoints-xavier-marta-as-vp-business-development-for-emea-to-drive-global-expansion') THEN 'Team Highlights'
         WHEN _url LIKE '%/leadership-team%' THEN 'Leadership Team'
         WHEN REGEXP_CONTAINS(_url, '/privacy-policy') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_url, '/cookie-policy') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_url,'/engagement-factory-terms-and-conditions') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_url, 'thought-leadership') THEN 'Thought Leadership'
         WHEN REGEXP_CONTAINS(_url, '/case-studies') THEN 'Case Studies'
         WHEN REGEXP_CONTAINS(_url, '/our-work') THEN 'Case Studies'
         WHEN REGEXP_CONTAINS(_url,'/?hsa') THEN 'Home Page'
         WHEN REGEXP_CONTAINS(_url,'utm_') THEN 'Home Page'
         ELSE 'Uncategorized'
         END AS _Page_Category
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON _companyname =df_name 
   WHERE _visitstartdate IS NOT NULL
 )
  ),

  _webtraffic_dealfront AS (
    SELECT
      _standardizedcompanyname,
      new_industry,
      ad_name,
      icp_tier,
      customer_segment,
      'Dealfront - Web Visit (till Apr 24)' AS _engagement,
      _visitid,
      NULL,
      _url,
      _visitstartdate,
      _Page_Category,
      _Page_group
    FROM webtraffic
    WHERE _visitstartdate < '2024-04-01'

  ),
  _6sense_website AS (
         SELECT * , 
 CASE 
 WHEN REGEXP_CONTAINS(_Page_Category, '1.0') THEN 'Blend 1.0'
 WHEN _Page_Category IN ('Responsible AI', 'Artificial Intelligence') THEN 'AI' 
 WHEN _Page_Category IN ('Business Intelligence', 'Data Science', 'Data Engineering', 'Data Governance', 'MLOps') THEN 'Capabilities'
 WHEN _Page_Category IN ('Healthcare & Life Science', 'Financial Services', 'Energy', 'Retail', 'Tech, Telecom & Media', 'Travel & Hospitality') THEN 'Industries'
 WHEN _Page_Category IN ('Customer Experience', 'Supply Chain', 'Product', 'Enterprise Operations') THEN 'Domain'
 WHEN _Page_Category IN ('Culture', 'Careers', 'Leadership Team', 'Job Board', 'All Stars') THEN 'Life @ Blend'
 WHEN _Page_Category IN ('News', 'Team Highlights', 'Case Studies', 'Thought Leadership') THEN 'Insights'
 WHEN _Page_Category IN ('About Us', 'Our Journey', 'Awards & Recognition' ,'Partners') THEN 'About'
 WHEN _Page_Category IN ('Contact Us') THEN 'Contact Us'
 WHEN _Page_Category = 'Home Page' THEN 'Home Page'
 WHEN _Page_Category = 'Event LP' THEN 'Event LP'
 ELSE 'Others' END AS _Page_group

 FROM (
 SELECT 
        std_name  AS _standardizedcompanyname,
        new_industry,
        ad_name,
        icp_tier,
        customer_segment,
        "6sense - Web Visit"  AS _engagement,
       _companyinfo,
       NULL,
       _activitytarget,
       PARSE_DATE('%m/%d/%Y',_activitydate) ,
        CASE 
         WHEN REGEXP_CONTAINS(_activitytarget, '/about-us') AND NOT REGEXP_CONTAINS(_activitytarget, '/about/') AND NOT REGEXP_CONTAINS(_activitytarget, '/about-us/') THEN 'About Us'
         WHEN REGEXP_CONTAINS(_activitytarget, '/about-us/') THEN '1.0 About Us'
         WHEN REGEXP_CONTAINS(_activitytarget, '/about/') THEN '1.0 About Us'
         WHEN REGEXP_CONTAINS(_activitytarget, '/careers') AND NOT REGEXP_CONTAINS(_activitytarget,'=')  AND NOT REGEXP_CONTAINS(_activitytarget,'solutions') AND NOT REGEXP_CONTAINS(_activitytarget, '/zoherecruit.in') AND NOT REGEXP_CONTAINS(_activitytarget,'quality-assurance-automation-engineer-remote') AND NOT REGEXP_CONTAINS(_activitytarget,'jobdetails') THEN 'Careers'
         WHEN _activitytarget LIKE '%/capabilities/business-intelligence%' THEN 'Business Intelligence'
         WHEN _activitytarget LIKE '%/capabilities/data-science%' THEN 'Data Science'
         WHEN _activitytarget LIKE  '%/capabilities/data-engineering%' THEN 'Data Engineering'
         WHEN _activitytarget LIKE '%/capabilities/data-governance%' THEN 'Data Governance'
         WHEN _activitytarget LIKE '%/capabilities/mlops%' THEN 'MLOps'
         WHEN _activitytarget LIKE '%/industries/healthcare-life-sciences%' THEN 'Healthcare & Life Science'  
         WHEN _activitytarget LIKE '%/industries/financial-services%' THEN 'Financial Services'
         WHEN _activitytarget LIKE '%/industries/energy%' THEN 'Energy'
         WHEN _activitytarget LIKE '%/industries/retail&' THEN 'Retail'
         WHEN _activitytarget LIKE 'www.blend360.com/industries/retail' THEN 'Retail'
         WHEN _activitytarget LIKE '%/industries/tech-telecom-media%' THEN 'Tech, Telecom & Media'
         WHEN _activitytarget LIKE '%/industries/travel-hospitality%' THEN 'Travel & Hospitality'
         WHEN _activitytarget LIKE '%/jobs%' THEN 'Job Board'
         WHEN _activitytarget LIKE '%/all-star%' THEN 'All Stars'
         WHEN _activitytarget LIKE '%/culture%' THEN 'Culture'
         WHEN _activitytarget LIKE '%/our-journey%' THEN 'Our Journey'
         WHEN _activitytarget LIKE '%/awards-recognition%' THEN 'Awards & Recognition'
         WHEN _activitytarget LIKE '%/partners%' THEN 'Partners'
         WHEN _activitytarget LIKE '%/artificial-intelligence%' THEN 'Artificial Intelligence'
         WHEN _activitytarget LIKE '%/responsible-ai%' THEN 'Responsible AI'
         WHEN _activitytarget LIKE '%/domain/customer-experience%' THEN 'Customer Experience'
         WHEN _activitytarget LIKE '%/domain/supply-chain%' THEN 'Supply Chain'
         WHEN _activitytarget LIKE '%/domain/product%' THEN 'Product'
         WHEN _activitytarget LIKE '%/domain/enterprise-operations%' THEN 'Enterprise Operations'
         WHEN _activitytarget LIKE '%/contact%' THEN 'Contact Us'
         WHEN _activitytarget LIKE '%www.blend360.com/' THEN 'Home Page'
         WHEN _activitytarget LIKE 'www.blend360.com/news' THEN 'News'
         WHEN _activitytarget LIKE 'www.blend360.com/news/%' THEN 'News'
         WHEN REGEXP_CONTAINS(_activitytarget,'/news') AND NOT REGEXP_CONTAINS(_activitytarget, 'honored-as-pioneer') AND NOT REGEXP_CONTAINS(_activitytarget, '/sample') AND NOT REGEXP_CONTAINS(_activitytarget, 'announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_activitytarget, '-the-7th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_activitytarget, 'opening-new-offices-denver-colorado') AND NOT REGEXP_CONTAINS(_activitytarget, '-announces-suite-of-new-generative-ai-features-to-drive-clients-business-performance') AND NOT REGEXP_CONTAINS(_activitytarget, '-announces-the-launch-of-a-denver-delivery-center-hiring-100-people-to-continue-the-hyper-growth-in-data-science-solutions-business') AND NOT REGEXP_CONTAINS(_activitytarget, 'ai-sparks') AND NOT REGEXP_CONTAINS(_activitytarget, 'for-the-8th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_activitytarget, 'rss.xml') AND NOT REGEXP_CONTAINS(_activitytarget, '/insights') AND NOT REGEXP_CONTAINS(_activitytarget, '/feed') AND NOT REGEXP_CONTAINS(_activitytarget, '/atom') AND NOT REGEXP_CONTAINS(_activitytarget, '-announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_activitytarget,'=') AND REGEXP_CONTAINS(_activitytarget,'/news/blend-earns-2023-great-place-to-work-certification-tm') THEN 'News'
         WHEN REGEXP_CONTAINS(_activitytarget, '/team-highlights') AND NOT REGEXP_CONTAINS(_activitytarget, 'consultant-spotlight-from-battlefield-to-boardroom-ismaels-inspirational-journey') AND NOT REGEXP_CONTAINS(_activitytarget, '-appoints-rebekah-hudson-as-vice-president-of-business-development-for-north-americaaeu') AND NOT REGEXP_CONTAINS(_activitytarget, '-appoints-xavier-marta-as-vp-business-development-for-emea-to-drive-global-expansion') THEN 'Team Highlights'
         WHEN _activitytarget LIKE '%/leadership-team%' THEN 'Leadership Team'
         WHEN REGEXP_CONTAINS(_activitytarget, '/privacy-policy') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_activitytarget, '/cookie-policy') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_activitytarget,'/engagement-factory-terms-and-conditions') THEN 'Policies'
         WHEN REGEXP_CONTAINS(_activitytarget, 'thought-leadership') THEN 'Thought Leadership'
         WHEN REGEXP_CONTAINS(_activitytarget, '/case-studies') THEN 'Case Studies'
         WHEN REGEXP_CONTAINS(_activitytarget, '/our-work') THEN 'Case Studies'
         WHEN REGEXP_CONTAINS(_activitytarget,'/?hsa') THEN 'Home Page'
         WHEN REGEXP_CONTAINS(_activitytarget,'utm_') THEN 'Home Page'
         ELSE 'Uncategorized'
         END AS _Page_Category
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_6s_daily_intent_data` ON _6sensecompanyname = _6s_ad_name
   WHERE _activitydate IS NOT NULL

  )
  ), 
  email_campaign AS (
     SELECT     	
        std_name AS _standardizedcompanyname,
        new_industry,
        ad_name,
        icp_tier,
        customer_segment,
        'Clicked - Email' AS _engagement,
        _email AS _email,
        NULL AS _click_impression,
        _contentTitle AS _contentTitle,
        CAST( _timestamp AS DATE) AS _date,
        _emailfilters AS _Page_Category,
        '' AS _page_group
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360.db_campaign_analysis` ON hs_name = _company
    WHERE _engagement = 'Clicked' 
    AND CAST( _timestamp AS DATE) >= "2023-10-01"

  ),
  download_contact_form AS (
    SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _companyname AS _standardizedcompanyname,
      acc.new_industry AS _industry,
      acc.ad_name,
      acc.icp_tier,
      acc.customer_segment,
      'Form Fill - Contact Us' AS _engagement,
      _email,
      NULL AS _click_impression,
      _contactusconversiontitle AS _campaign,
      CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) AS _timestamp,
      '' AS _Page_Category,
      '' AS _page_group,
      ROW_NUMBER() OVER(PARTITION BY _email, _contactusconversiontitle ORDER BY CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) DESC) AS rownum
      FROM
        `x-marketing.blend360_mysql.db_contact_us_form_submission` c
      LEFT JOIN 
          `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` acc ON c._companyname = acc.std_name 
    )
    WHERE
      rownum = 1

 ),
 download_content AS (
    SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _companyname AS _standardizedcompanyname,
      acc.new_industry AS _industry,
      acc.ad_name,
      acc.icp_tier,
      acc.customer_segment,
      'Form Fill - Contact Us' AS _engagement,
      _email,
      NULL AS _click_impression,
      _contactusconversiontitle AS _campaign,
      CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) AS _timestamp,
      '' AS _Page_Category,
      '' AS _page_group,
      ROW_NUMBER() OVER(PARTITION BY _email, _contactusconversiontitle ORDER BY CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) DESC) AS rownum
      FROM
        `x-marketing.blend360_mysql.db_contact_us_form_submission` c  -- TEMPORARILY USING THIS TABLE, USE db_content_form_submission WHEN IT IS AVAILABLE
      LEFT JOIN 
          `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` acc ON c._companyname = acc.std_name 
      WHERE acc.industry = 'ZZZZZ'  -- TO MAKE TABLE EMPTY SINCE USING DUMMY TABLE
    )
    WHERE
      rownum = 1

 ),
 download_newsletter_form AS (
    SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _companyname AS _standardizedcompanyname,
      acc.new_industry AS _industry,
      acc.ad_name,
      acc.icp_tier,
      acc.customer_segment,
      'Form Fill - Contact Us' AS _engagement,
      _email,
      NULL AS _click_impression,
      _contactusconversiontitle AS _campaign,
      CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) AS _timestamp,
      '' AS _Page_Category,
      '' AS _page_group,
      ROW_NUMBER() OVER(PARTITION BY _email, _contactusconversiontitle ORDER BY CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%m/%d/%Y', _contactusconversiondate)) AS DATE) DESC) AS rownum
      FROM
        `x-marketing.blend360_mysql.db_contact_us_form_submission` c  -- TEMPORARILY USING THIS TABLE, USE db_newsletter_form_submission WHEN IT IS AVAILABLE
      LEFT JOIN 
          `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` acc ON c._companyname = acc.std_name 
      WHERE acc.industry = 'ZZZZZ'  -- TO MAKE TABLE EMPTY SINCE USING DUMMY TABLE
    )
    WHERE
      rownum = 1

 ),
 download AS (
  SELECT * 
 FROM (
  SELECT  
 std_name AS _standardizedcompanyname,
        new_industry,
        a.ad_name,
        a.icp_tier,
        a.customer_segment,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
 LEFT JOIN download_contact_form on std_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Contact Us'
 UNION ALL
 SELECT
 std_name AS _standardizedcompanyname,
        new_industry,
        a.ad_name,
        a.icp_tier,
        a.customer_segment,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
 LEFT JOIN download_content on std_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Content'
  UNION ALL
 SELECT
 std_name AS _standardizedcompanyname,
        new_industry,
        a.ad_name,
        a.icp_tier,
        a.customer_segment,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
 LEFT JOIN download_newsletter_form  on std_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Newsletter'
 ) WHERE _timestamp >= "2023-10-01"
 ),
 content_syndication AS (
  SELECT 
    _company AS _standardizedcompanyname,
    new_industry,
    ad_name,
    icp_tier,
    customer_segment,
    'Content Syndication' AS _engagement,
    _primaryemail AS _email,
    NULL AS _click_impression,
    _gatedcontentname AS _campaign,
    PARSE_DATE('%m/%d/%Y', _dateapproved) AS _timestamp,
    '' AS _Page_Category,
    '' AS _page_group
  FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
  RIGHT JOIN `x-marketing.blend360_mysql.db_audyence_leads` cs ON cs._company = std_name
 ),
 event_attendance AS (
    WITH awareness_event AS (
      SELECT
        std_name AS _standardizedcompanyname,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        "Event - Awareness Event" AS _engagement,
        '' AS _email,
        NULL AS _clicks,
        '' AS _contentittle,
        CAST(TIMESTAMP_ADD(properties.event_attendance.timestamp, INTERVAL 8 HOUR) AS DATE) AS _date,
        '' AS _pagecategory,
        '' AS _pagegroup
      FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
      LEFT JOIN `x-marketing.blend360_hubspot_v2.companies` ON std_name = properties.name.value
      CROSS JOIN UNNEST(SPLIT(properties.event_attendance.value, ';')) AS event_attendance
      WHERE event_attendance = 'DataIQ 2024'
      GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    ),
    engagement_event AS (
      SELECT
        std_name AS _standardizedcompanyname,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        "Event - Engagement Event" AS _engagement,
        '' AS _email,
        NULL AS _clicks,
        '' AS _contentittle,
        CAST(TIMESTAMP_ADD(properties.event_attendance.timestamp, INTERVAL 8 HOUR) AS DATE) AS _date,
        '' AS _pagecategory,
        '' AS _pagegroup
      FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
      LEFT JOIN `x-marketing.blend360_hubspot_v2.companies` ON std_name = properties.name.value
      CROSS JOIN UNNEST(SPLIT(properties.event_attendance.value, ';')) AS event_attendance
      WHERE event_attendance LIKE '%Engaged%'
      GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    )
    SELECT *
    FROM (
      SELECT * FROM awareness_event
      UNION ALL
      SELECT * FROM engagement_event
    )
    
 ), engagement_score AS (

  SELECT * , "Engagement Score" AS score_type FROM email_campaign 
  UNION  ALL
  SELECT  *, "Engagement Score" AS score_type FROM webtraffic
  UNION  ALL
  SELECT *, "Engagement Score" AS score_type FROM _webtraffic_dealfront
  UNION ALL
  SELECT *, "Engagement Score" AS score_type FROM _6sense_website
  UNION ALL
  SELECT  *, "Engagement Score" AS score_type FROM download 
  UNION ALL
  SELECT *, "Engagement Score" AS score_type FROM content_syndication
  UNION ALL
  SELECT *, "Engagement Score" AS score_type FROM event_attendance
  ) 
  SELECT * FROM engagement_score 
  UNION ALL 
  SELECT * FROM awareness_score_type ;






----------density score new--------------------
CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_engagement` AS 
WITH awareness_score AS (
WITH impression AS  
  (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        _adgroup,
        a.icp_tier,
        a.customer_segment,
        "Impressions - FD" AS _engagement,
        '' AS _email,
      SUM(CAST(_impressionsdelivered AS INT64)) AS _impressions,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_avgfdengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_foundry_campaign_data_avg` ON fd_name = _company
    WHERE _impressionsdelivered IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    
    UNION ALL 
    SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        '' AS _adgroup,
        a.icp_tier,
        a.customer_segment,
        "Impressions - LI" AS _engagement,
        '' AS _email,
      SUM(CAST(_impressions AS INT64)) AS _impressions,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_liavgengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_linkedin_campaign_data_avg` ON li_name = _companyname
    WHERE _impressions IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12

    UNION ALL 
     SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        '' AS ad_name,
        a.icp_tier,
        a.customer_segment,
        "Impressions - DB" AS _engagement,
        '' AS _email,
      SUM(CAST(_impressions AS INT64)) AS _impressions,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_dbavrengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_demandbase_campaign_data_avg` ON db_name = _domainname
    WHERE _impressions IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12

  ), click AS (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        _adgroup,
        a.icp_tier,
        a.customer_segment,
        "Clicks - FD" AS _engagement,
        '' AS _email,
      SUM(CAST(_clicksdelivered AS INT64)) AS _clicks,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_avgfdengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_foundry_campaign_data_avg` ON fd_name = _company
    WHERE _clicksdelivered IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12
    
    UNION ALL 
    SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        '' AS _adgroup,
        a.icp_tier,
        a.customer_segment,
        "Clicks - LI" AS _engagement,
        '' AS _email,
      SUM(CAST(_adengagements AS INT64)) AS _clicks,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_liavgengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_linkedin_campaign_data_avg` ON li_name = _companyname
    WHERE _adengagements IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12

    UNION  ALL 
     SELECT 
        std_name AS _standardizedcompanyname,
        a.new_industry,
        '' AS _adgroup,
        a.icp_tier,
        a.customer_segment,
        "Clicks - DB" AS _engagement,
        '' AS _email,
      SUM(CAST(_clicks AS INT64)) AS _clicks,
      '' AS _contenttitle,
      PARSE_DATE('%m/%d/%Y',_dbavrengagementdate) AS _date,
      '' AS _pagecategory,
      '' AS _pagegroup
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_demandbase_campaign_data_avg` ON db_name = _domainname
    WHERE _impressions IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,9,10,11,12

  ),

impression_li_6sense AS (
WITH impressions_data AS (
    SELECT 
        std_name,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        CASE WHEN _liextractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
        WHEN _liextractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
        ELSE
        PARSE_DATE('%m/%d/%Y', _liextractdate) END AS _date,
        SUM(CAST(REPLACE(_impressions, ',', '') AS INT64)) AS _impressions,
        CASE WHEN _liextractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _liextractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) END AS year,
        CASE WHEN _liextractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _liextractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) END AS month,
        _campaignID
    FROM
        `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_6s_li_account_reached` ON _6s_li_ad_name = _6sensecompanyname
    WHERE _impressions IS NOT NULL AND _sdc_deleted_at IS NULL
    GROUP BY 1,2,3,4,5,6,8,9,10
),
max_date_per_month AS (
    SELECT 
        std_name,
        _campaignID,
        FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
        MAX(_date) AS max_date
    FROM 
        impressions_data
    GROUP BY 
        1,2,3
),

impressions_on_max_date AS (
    SELECT t.* EXCEPT (year, month),
        m.max_date
    FROM
        impressions_data t
    JOIN
        max_date_per_month m
    ON
        t.std_name = m.std_name AND t._campaignID = m._campaignID AND t._date = m.max_date
),
lagged_impressions AS (
    SELECT
      m.*,
        LAG(_impressions) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_impressions
    FROM
        impressions_on_max_date m
), 
combined_data AS (
SELECT
  lagged_impressions.*,
  IFNULL(_impressions - prev_impressions, _impressions) AS change_impressions
FROM
    lagged_impressions
ORDER BY
    std_name,
    _impressions,
    _campaignID,
    _date
)
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  ad_name,
  icp_tier,
  customer_segment,
  "Impressions - LI 6sense" AS _engagement,
  '' AS _email,
  SUM(change_impressions),
  '' AS _contentittle,
  _date,
  '' AS _pagecategory,
  '' AS _pagegroup 
FROM 
  combined_data
GROUP BY 1,2,3,4,5,6,7,9,10,11,12

),

impression_6sense AS (
WITH impressions_data AS (
    SELECT 
        std_name,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        CASE WHEN _extractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
        WHEN _extractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
        ELSE
        PARSE_DATE('%m/%d/%Y', _extractdate) END AS _date,
        SUM(CAST(REPLACE(_impressions, ',', '') AS INT64)) AS _impressions,
        CASE WHEN _extractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _extractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _extractdate)) END AS year,
        CASE WHEN _extractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _extractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _extractdate)) END AS month,
        _campaignID
    FROM
        `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_6s_account_reached` ON _6s_ad_name = _6sensecompanyname
    WHERE _impressions IS NOT NULL AND _sdc_deleted_at IS NULL
    GROUP BY 1,2,3,4,5,6,8,9,10
),
max_date_per_month AS (
    SELECT 
        std_name,
        _campaignID,
        FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
        MAX(_date) AS max_date
    FROM 
        impressions_data
    GROUP BY 
        1,2,3
),
impressions_on_max_date AS (
    SELECT t.* EXCEPT (year, month),
        m.max_date
    FROM
        impressions_data t
    JOIN
        max_date_per_month m
    ON
        t.std_name = m.std_name AND t._campaignID = m._campaignID AND t._date = m.max_date
),
lagged_impressions AS (
    SELECT
      m.*,
        LAG(_impressions) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_impressions
    FROM
        impressions_on_max_date m
), 
combined_data AS (
SELECT
  lagged_impressions.*,
  IFNULL(_impressions - prev_impressions, _impressions) AS change_impressions
FROM
    lagged_impressions
ORDER BY
    std_name,
    _impressions,
    _campaignID,
    _date
)
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  ad_name,
  icp_tier,
  customer_segment,
  "Impressions - 6sense" AS _engagement,
  '' AS _email,
  SUM(change_impressions),
  '' AS _contentittle,
  _date,
  '' AS _pagecategory,
  '' AS _pagegroup 
FROM 
  combined_data
GROUP BY 1,2,3,4,5,6,7,9,10,11,12

),

clicks_li_6sense AS (
WITH clicks_data AS (
    SELECT 
        std_name,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        CASE WHEN _liextractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
        WHEN _liextractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
        ELSE
        PARSE_DATE('%m/%d/%Y', _liextractdate) END AS _date,
        SUM(CAST(_clicks AS INT64)) AS _clicks,
        CASE WHEN _liextractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _liextractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) END AS year,
        CASE WHEN _liextractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _liextractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) END AS month,
        _campaignID
    FROM
        `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_6s_li_account_reached` ON _6s_li_ad_name = _6sensecompanyname
    WHERE _impressions IS NOT NULL AND _sdc_deleted_at IS NULL
    GROUP BY 1,2,3,4,5,6,8,9,10
),
max_date_per_month AS (
    SELECT 
        std_name,
        _campaignID,
        FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
        MAX(_date) AS max_date
    FROM 
        clicks_data
    GROUP BY 
        1,2,3
),

clicks_on_max_date AS (
    SELECT t.* EXCEPT (year, month),
        m.max_date
    FROM
        clicks_data t
    JOIN
        max_date_per_month m
    ON
        t.std_name = m.std_name AND t._campaignID = m._campaignID AND t._date = m.max_date
),

lagged_clicks AS (
    SELECT
      m.*,
        LAG(_clicks) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_clicks
    FROM
        clicks_on_max_date m
), 
combined_data AS (
SELECT
  lagged_clicks.*,
  IFNULL(_clicks - prev_clicks, _clicks) AS change_clicks
FROM
    lagged_clicks
ORDER BY
    std_name,
    _clicks,
    _campaignID,
    _date
)
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  ad_name,
  icp_tier,
  customer_segment,
  "Clicks - LI 6sense" AS _engagement,
  '' AS _email,
  SUM(change_clicks),
  '' AS _contentittle,
  _date,
  '' AS _pagecategory,
  '' AS _pagegroup 
FROM 
  combined_data
GROUP BY 1,2,3,4,5,6,7,9,10,11,12

),

clicks_6sense AS (
WITH clicks_data AS (
    SELECT 
        std_name,
        a.new_industry,
        ad_name,
        a.icp_tier,
        a.customer_segment,
        CASE WHEN _extractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
        WHEN _extractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
        ELSE
        PARSE_DATE('%m/%d/%Y', _extractdate) END AS _date,
        SUM(CAST(_clicks AS INT64)) AS _clicks,
        CASE WHEN _extractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _extractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _extractdate)) END AS year,
        CASE WHEN _extractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
        WHEN _extractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
        ELSE
        EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _extractdate)) END AS month,
        _campaignID
    FROM
        `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_mysql.db_6s_account_reached` ON _6s_ad_name = _6sensecompanyname
    WHERE _impressions IS NOT NULL AND _sdc_deleted_at IS NULL
    GROUP BY 1,2,3,4,5,6,8,9,10
),
max_date_per_month AS (
    SELECT 
        std_name,
        _campaignID,
        FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
        MAX(_date) AS max_date
    FROM 
        clicks_data
    GROUP BY 
        1,2,3
),
clicks_on_max_date AS (
    SELECT t.* EXCEPT (year, month),
        m.max_date
    FROM
        clicks_data t
    JOIN
        max_date_per_month m
    ON
        t.std_name = m.std_name AND t._campaignID = m._campaignID AND t._date = m.max_date
),
lagged_clicks AS (
    SELECT
      m.*,
        LAG(_clicks) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_clicks
    FROM
        clicks_on_max_date m
), 
combined_data AS (
SELECT
  lagged_clicks.*,
  IFNULL(_clicks - prev_clicks, _clicks) AS change_clicks
FROM
    lagged_clicks
ORDER BY
    std_name,
    _clicks,
    _campaignID,
    _date
)
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  ad_name,
  icp_tier,
  customer_segment,
  "Clicks - 6sense" AS _engagement,
  '' AS _email,
  SUM(change_clicks),
  '' AS _contentittle,
  _date,
  '' AS _pagecategory,
  '' AS _pagegroup 
FROM 
  combined_data
GROUP BY 1,2,3,4,5,6,7,9,10,11,12


  )
SELECT *, 'Awareness Score' AS score_type FROM impression
UNION ALL
SELECT *, 'Awareness Score' AS score_type FROM click
UNION ALL
SELECT *, 'Awareness Score' AS score_type FROM impression_li_6sense
UNION ALL
SELECT *, 'Awareness Score' AS score_type FROM impression_6sense
UNION ALL
SELECT *, 'Awareness Score' AS score_type FROM clicks_li_6sense
UNION ALL
SELECT *, 'Awareness Score' AS score_type FROM clicks_6sense
),



engagement_score AS (
  SELECT * FROM `x-marketing.blend360.abm_score_engagement`
  WHERE score_type = 'Engagement Score'
)
SELECT * FROM engagement_score
UNION ALL
SELECT * FROM awareness_score;







CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_score` AS
WITH account_master AS (
  WITH account AS (
    SELECT DISTINCT 
      std_name AS _standardizedcompanyname, 
      g.new_industry AS industry, 
      g.icp_tier, 
      g.customer_segment, 
      g.q1_2024__6s_, 
      g.priority
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` g 
    LEFT JOIN `x-marketing.blend360.abm_density_engagement` h 
      ON _standardizedcompanyname = std_name
  ),
  Months AS (
    SELECT DISTINCT 
      calendar_date AS month_date
    FROM UNNEST(GENERATE_DATE_ARRAY('2023-10-01', CURRENT_DATE(), INTERVAL 1 MONTH)) AS calendar_date
  )
  SELECT 
    a._standardizedcompanyname,
    a.industry,
    a.icp_tier,
    a.customer_segment,
    a.q1_2024__6s_,
    a.priority,
    m.month_date
  FROM 
    account a
  CROSS JOIN 
    Months m
),
score_calculate AS (
  SELECT 
    _standardizedcompanyname,
    DATE_TRUNC(_date, MONTH) AS _monthyear,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - LI' THEN _click_impression END) AS _li_impression,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Clicks - LI' THEN _click_impression END) AS _li_click,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - FD' THEN _click_impression END) AS _fd_impression,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Clicks - FD' THEN _click_impression END) AS _fd_click,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - DB' THEN _click_impression END) AS _db_impression,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Clicks - DB' THEN _click_impression END) AS _db_click,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - LI 6sense' THEN _click_impression END) AS _li_6sense_impression,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Clicks - LI 6sense' THEN _click_impression END) AS _li_6sense_click,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - 6sense' THEN _click_impression END) AS _6sense_impression,
    SUM(CASE WHEN _click_impression >= 1 AND _engagement = 'Clicks - 6sense' THEN _click_impression END) AS _6sense_click,
    ---old dealfront count, excluded from score but still need in dashboard
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category = 'Case Studies' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_case_study,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Domain' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_domain,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'AI' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_ai_page,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Industries' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_industry,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Capabilities' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_capability,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category IN ("About Us",'Home Page') THEN _contentTitle END) AS _dealfront_homepage,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category IN ('News', 'Thought Leadership') THEN _contentTitle END) AS _dealfront_thought_leader,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category IN ('Culture', 'Careers', 'Team Highlights', 'Job Board') THEN _contentTitle END) AS _dealfront_career,
    ---new dealfront count, used as a score
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_category = 'Case Studies' THEN CONCAT(_email, _contentTitle) END) AS _dealf_case_study,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_group = 'Domain' THEN CONCAT(_email, _contentTitle) END) AS _dealf_domain,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_group = 'AI' THEN CONCAT(_email, _contentTitle) END) AS _dealf_ai_page,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_group = 'Industries' THEN CONCAT(_email, _contentTitle) END) AS _dealf_industry,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_group = 'Capabilities' THEN CONCAT(_email, _contentTitle) END) AS _dealf_capability,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_category IN ("About Us",'Home Page') THEN _contentTitle END) AS _dealf_homepage,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_category IN ('News', 'Thought Leadership') THEN _contentTitle END) AS _dealf_thought_leader,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND _page_category IN ('Culture', 'Careers', 'Team Highlights', 'Job Board') THEN _contentTitle END) AS _dealf_career,
    ---new web score via 6sense, include in the score + dealfront (till Apr 24)
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_category = 'Case Studies' THEN CONCAT(_email, _contentTitle) END) AS _6senseweb_case_study,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_group = 'Domain' THEN CONCAT(_email, _contentTitle) END) AS _6senseweb_domain,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_group = 'AI' THEN CONCAT(_email, _contentTitle) END) AS _6senseweb_ai_page,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_group = 'Industries' THEN CONCAT(_email, _contentTitle) END) AS _6senseweb_industry,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_group = 'Capabilities' THEN CONCAT(_email, _contentTitle) END) AS _6senseweb_capability,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_category IN ("About Us",'Home Page') THEN _contentTitle END) AS _6senseweb_homepage,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_category IN ('News', 'Thought Leadership') THEN _contentTitle END) AS _6senseweb_thought_leader,
    COUNT(CASE WHEN _engagement = '6sense - Web Visit' AND _page_category IN ('Culture', 'Careers', 'Team Highlights', 'Job Board') THEN _contentTitle END) AS _6senseweb_career,
    COUNT(CASE WHEN _engagement = 'Clicked - Email' AND _page_category = 'Campaign' THEN CONCAT(_email, _contentTitle) END) AS _email_click_campaign,
    COUNT(DISTINCT CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND (_page_group IN ('Domain', 'AI', 'Industries', 'Capabilities') OR _page_category = 'Case Studies') THEN _email END) AS _dealfront_sesion,
    COUNT(CASE WHEN _engagement = 'Dealfront - Web Visit (till Apr 24)' AND (_page_group IN ('Domain', 'AI', 'Industries', 'Capabilities') OR _page_category = 'Case Studies') THEN CONCAT(_email, _contentTitle) END) AS _dealfront_target_page,
    COUNT(CASE WHEN _engagement = 'Form Fill - Contact Us' THEN CONCAT(_email, _standardizedcompanyname) END) AS _formfill_contact_us,
    COUNT(CASE WHEN _engagement = 'Form Fill - Content' THEN CONCAT(_email, _standardizedcompanyname) END) AS _formfill_content,
    COUNT(CASE WHEN _engagement = 'Form Fill - Newsletter' THEN CONCAT(_email, _standardizedcompanyname) END) AS _formfill_newsletter,
    COUNT(DISTINCT CASE WHEN _engagement = 'Clicked - Email' AND _page_category = 'Nuture' THEN CONCAT(_email, _contentTitle) END) AS _email_click_nurture,
    COUNT(DISTINCT CASE WHEN _engagement = 'Content Syndication' THEN CONCAT(_email, _standardizedcompanyname) END) AS _content_syndication,
    COUNT(CASE WHEN _engagement = 'Event - Awareness Event' THEN _standardizedcompanyname END) AS _event_attendance_awareness,
   COUNT(CASE WHEN _engagement = 'Event - Engagement Event' THEN _standardizedcompanyname END) AS _event_attendance_engagement,
  FROM `x-marketing.blend360.abm_density_engagement`
  GROUP BY 1, 2
), 
action_score AS (
  SELECT *,
    (COALESCE(_li_impression,0) + COALESCE(_fd_impression,0) + COALESCE(_db_impression,0) + COALESCE(_li_6sense_impression,0) + COALESCE(_6sense_impression,0)) AS _total_impresion,
    (COALESCE(_li_click,0) + COALESCE(_fd_click,0) + COALESCE(_db_click,0) + COALESCE(_li_6sense_click,0) + COALESCE(_6sense_click,0)) AS _total_click,
    (COALESCE(_li_impression,0) * 0.1 + COALESCE(_fd_impression,0) * 0.1 + COALESCE(_db_impression,0) * 0.1 + COALESCE(_li_6sense_impression,0) * 0.1 + COALESCE(_6sense_impression,0) * 0.1) AS _total_score_impression,
    (COALESCE(_fd_click,0) * 0.5 + COALESCE(_li_click,0) * 0.5 + COALESCE(_db_click,0) * 0.5 + COALESCE(_li_6sense_click,0) * 0.5 + COALESCE(_6sense_click,0) * 0.5) AS _total_score_click,
    ---old score, exclude from engagement score
    COALESCE(_dealfront_case_study,0) * 200 AS _dealfront_case_study_score,
    COALESCE(_dealfront_domain,0) * 100 AS _dealfront_domain_score,
    COALESCE(_dealfront_ai_page,0) * 100 AS _dealfront_ai_page_score,
    COALESCE(_dealfront_industry,0) * 100 AS _dealfront_industry_score,
    COALESCE(_dealfront_capability,0) * 100 AS _dealfront_capability_score,
    COALESCE(_dealfront_homepage,0) * 50 AS _dealfront_homepage_score,
    COALESCE(_dealfront_thought_leader,0) * 30 AS _dealfront_thought_leader_score,
    COALESCE(_dealfront_career,0) * 10 AS _dealfront_career_score,
    ---new score that use in engagement score
    (COALESCE(_dealf_case_study,0) * 200) + (COALESCE(_6senseweb_case_study,0) * 200) AS _df6s_case_study_score,
    (COALESCE(_dealf_domain,0) * 100) + (COALESCE(_6senseweb_domain,0) * 100) AS _df6s_domain_score,
    (COALESCE(_dealf_ai_page,0) * 100) + (COALESCE(_6senseweb_ai_page,0) * 100) AS _df6s_ai_page_score,
    (COALESCE(_dealf_industry,0) * 100) + (COALESCE(_6senseweb_industry,0) * 100) AS _df6s_industry_score,
    (COALESCE(_dealf_capability,0) * 100) + (COALESCE(_6senseweb_capability,0) * 100) AS _df6s_capability_score,
    (COALESCE(_dealf_homepage,0) * 50) + (COALESCE(_6senseweb_homepage,0) * 50) AS _df6s_homepage_score,
    (COALESCE(_dealf_thought_leader,0) * 30) + (COALESCE(_6senseweb_thought_leader,0) * 30) AS _df6s_thought_leader_score,
    (COALESCE(_dealf_career,0) * 10) + (COALESCE(_6senseweb_career,0) * 10) AS _df6s_career_score,

    COALESCE(_email_click_campaign,0) * 50 AS _email_click_campaign_score,
    COALESCE(_email_click_nurture,0) * 80 AS _email_click_nurture_score,
    CAST(CASE WHEN _dealfront_sesion >= 3 THEN 150 ELSE 0 END AS INT64) AS _dealfront_sesion_score,
    CAST(CASE WHEN _dealfront_target_page >= 3 THEN 150 ELSE 0 END AS INT64) AS _dealfront_target_page_score,
    COALESCE(_formfill_contact_us,0) * 250 AS _formfill_contact_us_score,
    COALESCE(_formfill_content,0) * 200 AS _formfill_content_score,
    COALESCE(_formfill_newsletter,0) * 150 AS _formfill_newsletter_score,
    COALESCE(_content_syndication,0) * 200 AS _content_syndication_score,
    CAST(CASE WHEN _event_attendance_awareness >= 1 THEN 75 ELSE 0 END AS INT64) AS _event_attendance_awareness_score,
    CAST(CASE WHEN _event_attendance_engagement >= 1 THEN 300 ELSE 0 END AS INT64) AS _event_attendance_engagement_score,    
  FROM score_calculate
), 
_all AS (
  SELECT 
    account_master.*,
    action_score.* EXCEPT (_standardizedcompanyname, _monthyear),
    (_total_score_impression + _total_score_click) AS _awareness_score,
    (_df6s_case_study_score + _df6s_domain_score + _df6s_ai_page_score + _df6s_industry_score + _df6s_capability_score + _df6s_homepage_score + _df6s_thought_leader_score + _df6s_career_score + _email_click_campaign_score + _dealfront_sesion_score + _dealfront_target_page_score + _formfill_contact_us_score + _formfill_content_score + _formfill_newsletter_score + _email_click_nurture_score + _content_syndication_score + _event_attendance_awareness_score + _event_attendance_engagement_score) AS _engagement_score,
    (_total_score_impression + _total_score_click + _df6s_case_study_score + _df6s_domain_score + _df6s_ai_page_score + _df6s_industry_score + _df6s_capability_score + _df6s_homepage_score + _df6s_thought_leader_score + _df6s_career_score + _email_click_campaign_score + _dealfront_sesion_score + _dealfront_target_page_score + _formfill_contact_us_score + _formfill_content_score + _formfill_newsletter_score + _email_click_nurture_score + _content_syndication_score + _event_attendance_awareness_score + _event_attendance_engagement_score) AS _total_score
  FROM 
    account_master
  LEFT JOIN 
    action_score 
    ON account_master._standardizedcompanyname = action_score._standardizedcompanyname
    AND account_master.month_date = _monthyear
), 
--to make the comparison MoM
_with_lag AS (
  SELECT 
    _all.* EXCEPT (_dealf_domain, _dealf_case_study, _dealf_ai_page, _dealf_industry, _dealf_capability, _dealf_homepage, _dealf_thought_leader, _dealf_career),
    LAG(_awareness_score) OVER (PARTITION BY _standardizedcompanyname ORDER BY month_date) AS prev_awareness_score,
    LAG(_engagement_score) OVER (PARTITION BY _standardizedcompanyname ORDER BY month_date) AS prev_engagement_score,
    LAG(_total_score) OVER (PARTITION BY _standardizedcompanyname ORDER BY month_date) AS prev_total_score,
    MAX(_awareness_score) OVER (PARTITION BY _standardizedcompanyname) AS max_awareness_score,
    MAX(_engagement_score) OVER (PARTITION BY _standardizedcompanyname) AS max_engagement_score,
    MAX(_total_score) OVER (PARTITION BY _standardizedcompanyname) AS max_total_score,
    ROW_NUMBER() OVER (PARTITION BY _standardizedcompanyname ORDER BY _total_score DESC) AS _rank
  FROM 
    _all
),
--find the highest score and targeted month for each accounts
_rank AS (
WITH ranked_data AS (
  SELECT
    _standardizedcompanyname AS account,
    month_date,
    CASE WHEN _total_score = MAX(_total_score) OVER (PARTITION BY _standardizedcompanyname) THEN FORMAT_DATE('%b %Y', month_date) END AS highest_month_total_score,
    CASE WHEN _awareness_score = MAX(_awareness_score) OVER (PARTITION BY _standardizedcompanyname) THEN FORMAT_DATE('%b %Y', month_date) END AS highest_month_awareness_score,
    CASE WHEN _engagement_score = MAX(_engagement_score) OVER (PARTITION BY _standardizedcompanyname) THEN FORMAT_DATE('%b %Y', month_date) END AS highest_month_engagement_score
  FROM
    `x-marketing.blend360.abm_density_score`
  WHERE
    month_date >= '2023-10-01'
),
filled_data AS (
  SELECT
    account,
    month_date,
    FIRST_VALUE(highest_month_total_score IGNORE NULLS) OVER (PARTITION BY account ORDER BY month_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS filled_highest_month_total_score,
    FIRST_VALUE(highest_month_awareness_score IGNORE NULLS) OVER (PARTITION BY account ORDER BY month_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS filled_highest_month_awareness_score,
    FIRST_VALUE(highest_month_engagement_score IGNORE NULLS) OVER (PARTITION BY account ORDER BY month_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS filled_highest_month_engagement_score
  FROM
    ranked_data
)
SELECT
  DISTINCT account,
  filled_highest_month_total_score AS highest_month_score_total,
  filled_highest_month_awareness_score AS highest_month_score_awareness,
  filled_highest_month_engagement_score AS highest_month_score_engagement
FROM
  filled_data
ORDER BY
  account
)
SELECT 
  _with_lag.*,
  CASE 
    WHEN _awareness_score IS NOT NULL AND prev_awareness_score IS NOT NULL 
    THEN _awareness_score - prev_awareness_score 
    ELSE NULL 
  END AS awareness_score_change,
  CASE 
    WHEN _engagement_score IS NOT NULL AND prev_engagement_score IS NOT NULL 
    THEN _engagement_score - prev_engagement_score 
    ELSE NULL 
  END AS engagement_score_change,
  CASE 
    WHEN _total_score IS NOT NULL AND prev_total_score IS NOT NULL 
    THEN _total_score - prev_total_score 
    ELSE NULL 
  END AS total_score_change,
  CONCAT(
    CASE 
      WHEN _awareness_score > 0 
      THEN ROUND(_awareness_score / (_awareness_score + _engagement_score) * 100.0 / 10)
      ELSE 0 
    END, 
    " : ", 
    CASE 
      WHEN _engagement_score > 0 
      THEN ROUND(_engagement_score / (_awareness_score + _engagement_score) * 100.0 / 10)
      ELSE 0 
    END
  ) AS _ratio,
  highest_month_score_total,
  highest_month_score_awareness,
  highest_month_score_engagement
FROM 
  _with_lag
LEFT JOIN _rank ON _rank.account = _with_lag._standardizedcompanyname;






CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_industry` AS
WITH account_count AS (
  SELECT industry,
         COUNT(DISTINCT _standardizedcompanyname) AS _count
  FROM `x-marketing.blend360.abm_density_score`
  GROUP BY 1
),
industry_count AS (
SELECT main.industry, 
       month_date,
       CONCAT ("Q",FORMAT_DATE('%Q',month_date),"-"," ",EXTRACT(YEAR FROM month_date)) AS _quarter,
       _count,
       SUM(_awareness_score) AS awareness_score,
       SUM(_engagement_score) AS engagement_score,
       SUM(_awareness_score + _engagement_score) AS total_score
FROM `x-marketing.blend360.abm_density_score` main
JOIN account_count sub ON main.industry = sub.industry
GROUP BY 1,2,3,4
),
_all AS (
SELECT *,
       awareness_score / _count AS awareness_density,
       engagement_score / _count AS engagement_density,
       total_score / _count AS total_density
FROM industry_count
),
quarter_count AS (
  SELECT 
    *, 
    count(_quarter) 
    OVER (PARTITION BY industry, _quarter ) AS reduced_count_quarter
  FROM _all
),
reduced_quarter AS (
SELECT * EXCEPT (reduced_count_quarter),
  _count/reduced_count_quarter AS _quarter_count
FROM quarter_count
)
SELECT industry,
       month_date,
       _quarter,
       _count AS month_count,
       awareness_score,
       engagement_score,
       total_score,
       awareness_density AS monthly_awareness_density,
       engagement_density AS monthly_engagement_density,
       total_density AS monthly_total_density,
       _quarter_count
FROM reduced_quarter
;



CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_tier` AS
WITH account_count AS (
  SELECT icp_tier,
         COUNT(DISTINCT _standardizedcompanyname) AS _count
  FROM `x-marketing.blend360.abm_density_score`
  GROUP BY 1
),
tier_count AS (
SELECT main.icp_tier, 
       month_date,
       CONCAT ("Q",FORMAT_DATE('%Q', month_date),"-"," ",EXTRACT(YEAR FROM month_date)) AS _quarter,
       _count,
       SUM(_awareness_score) AS awareness_score,
       SUM(_engagement_score) AS engagement_score,
       SUM(_awareness_score + _engagement_score) AS total_score
FROM `x-marketing.blend360.abm_density_score` main
JOIN account_count sub ON main.icp_tier = sub.icp_tier
GROUP BY 1,2,3,4
),
_all AS (
SELECT *,
       awareness_score / _count AS awareness_density,
       engagement_score / _count AS engagement_density,
       total_score / _count AS total_density
FROM tier_count
),
quarter_count AS (
  SELECT 
    *, 
    count(_quarter) 
    OVER (PARTITION BY icp_tier, _quarter ) AS reduced_count_quarter
  FROM _all
),
reduced_quarter AS (
SELECT * EXCEPT (reduced_count_quarter),
  _count/reduced_count_quarter AS _quarter_count
FROM quarter_count
)
SELECT icp_tier,
       month_date,
       _quarter,
       _count AS month_count,
       awareness_score,
       engagement_score,
       total_score,
       awareness_density AS monthly_awareness_density,
       engagement_density AS monthly_engagement_density,
       total_density AS monthly_total_density,
       _quarter_count
FROM reduced_quarter
;



CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_segment` AS
WITH account_count AS (
  SELECT customer_segment,
         COUNT(DISTINCT _standardizedcompanyname) AS _count
  FROM `x-marketing.blend360.abm_density_score`
  GROUP BY 1
),
tier_count AS (
SELECT main.customer_segment, 
       month_date,
       CONCAT ("Q",FORMAT_DATE('%Q', month_date),"-"," ",EXTRACT(YEAR FROM month_date)) AS _quarter,
       _count,
       SUM(_awareness_score) AS awareness_score,
       SUM(_engagement_score) AS engagement_score,
       SUM(_awareness_score + _engagement_score) AS total_score
FROM `x-marketing.blend360.abm_density_score` main
JOIN account_count sub ON main.customer_segment = sub.customer_segment
GROUP BY 1,2,3,4
),
_all AS (
SELECT *,
       awareness_score / _count AS awareness_density,
       engagement_score / _count AS engagement_density,
       total_score / _count AS total_density
FROM tier_count
),
quarter_count AS (
  SELECT 
    *, 
    count(_quarter) 
    OVER (PARTITION BY customer_segment, _quarter ) AS reduced_count_quarter
  FROM _all
),
reduced_quarter AS (
SELECT * EXCEPT (reduced_count_quarter),
  _count/reduced_count_quarter AS _quarter_count
FROM quarter_count
)
SELECT customer_segment,
       month_date,
       _quarter,
       _count AS month_count,
       awareness_score,
       engagement_score,
       total_score,
       awareness_density AS monthly_awareness_density,
       engagement_density AS monthly_engagement_density,
       total_density AS monthly_total_density,
       _quarter_count
FROM reduced_quarter
;


CREATE OR REPLACE TABLE `x-marketing.blend360.abm_density_score` AS
SELECT _all.*,
       ind.monthly_total_density AS industry_total_density,
       ind.monthly_awareness_density AS industry_awareness_density,
       ind.monthly_engagement_density AS industry_engagement_density,
       tier.monthly_total_density AS tier_total_density,
       tier.monthly_awareness_density AS tier_awareness_density,
       tier.monthly_engagement_density AS tier_engagement_density,
       segment.monthly_total_density AS segment_total_density,
       segment.monthly_awareness_density AS segment_awareness_density,
       segment.monthly_engagement_density AS segment_engagement_density
FROM `x-marketing.blend360.abm_density_score` _all
LEFT JOIN `x-marketing.blend360.abm_density_industry` ind ON ind.month_date = _all.month_date AND ind.industry = _all.industry
LEFT JOIN `x-marketing.blend360.abm_density_tier` tier ON tier.month_date = _all.month_date AND tier.icp_tier = _all.icp_tier
LEFT JOIN `x-marketing.blend360.abm_density_segment` segment ON segment.month_date = _all.month_date AND segment.customer_segment = _all.customer_segment;