CREATE OR REPLACE TABLE `x-marketing.blend360.abm_score_engagement` AS
WITH impression AS  
  (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.industry,
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
    GROUP BY 1,2,3
    
    UNION ALL 
    SELECT 
       std_name AS _standardizedcompanyname,
       industry,
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
    GROUP BY 1,2,3
    UNION ALL 
     SELECT 
       std_name AS _standardizedcompanyname,
       a.industry,
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
    GROUP BY 1,2,3
  ), click AS (
   SELECT 
        std_name AS _standardizedcompanyname,
        a.industry,
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
    GROUP BY 1,2,3
    
    UNION ALL 
    SELECT 
       std_name AS _standardizedcompanyname,
       industry,
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
    GROUP BY 1,2,3
    UNION  ALL 
     SELECT 
       std_name AS _standardizedcompanyname,
       a.industry,
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
    GROUP BY 1,2,3
  ), awareness_score_type AS (
     SELECT  *, "Awareness Score" AS score_type FROM  impression
  UNION  ALL 
  SELECT  *,"Awareness Score" AS score_type FROM  click
  ), webtraffic AS (
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
 ELSE 'Others' END AS _Page_group

 FROM (
 SELECT 
        COALESCE(std_name,_companyname) AS _standardizedcompanyname,
        industry,
        "Dealfront - Web Visit"  AS _engagement,
       --_visitoridleadfeeder,
       _visitid,
       NULL,
       /*CASE WHEN _url LIKE '%utm_%' THEN 'www.blend360.com/'
        WHEN _url LIKE '%hsa_%' THEN SPLIT(_url,'?hsa_acc')[SAFE_OFFSET(0)]
        ELSE _url END AS*/
       _url,
       --_url AS fill_url,
       PARSE_DATE('%m/%d/%Y',_visitstartdate) ,
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
         WHEN _url LIKE '/artificial-intelligence' THEN 'Artificial Intelligence'
         WHEN _url LIKE '%/responsible-ai%' THEN 'Responsible AI'
         WHEN _url LIKE '%/domain/customer-experience%' THEN 'Customer Experience'
         WHEN _url LIKE '%/domain/supply-chain%' THEN 'Supply Chain'
         WHEN _url LIKE '%/domain/product%' THEN 'Product'
         WHEN _url LIKE '%/domain/enterprise-operations%' THEN 'Enterprise Operations'
         WHEN _url LIKE '%/contact%' THEN 'Contact Us'
         WHEN _url LIKE '%www.blend360.com/' THEN 'Home Page'
         WHEN _url LIKE 'www.blend360.com/news' THEN 'News'
         WHEN _url LIKE 'www.blend360.com/news/%' THEN 'News'
         WHEN REGEXP_CONTAINS(_url,'/news') AND NOT REGEXP_CONTAINS(_url, 'honored-as-pioneer') AND /*NOT REGEXP_CONTAINS(_url, '-earns-2023-great-place-to-work-certification-tm') AND*/ NOT REGEXP_CONTAINS(_url, '/sample') AND NOT REGEXP_CONTAINS(_url, 'announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_url, '-the-7th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_url, 'opening-new-offices-denver-colorado') AND NOT REGEXP_CONTAINS(_url, '-announces-suite-of-new-generative-ai-features-to-drive-clients-business-performance') AND NOT REGEXP_CONTAINS(_url, '-announces-the-launch-of-a-denver-delivery-center-hiring-100-people-to-continue-the-hyper-growth-in-data-science-solutions-business') AND NOT REGEXP_CONTAINS(_url, 'ai-sparks') AND NOT REGEXP_CONTAINS(_url, 'for-the-8th-time-blend360-appears-on-the-inc-5000-with-three-year-revenue-growth-of-68-percent') AND NOT REGEXP_CONTAINS(_url, 'rss.xml') AND NOT REGEXP_CONTAINS(_url, '/insights') AND NOT REGEXP_CONTAINS(_url, '/feed') AND NOT REGEXP_CONTAINS(_url, '/atom') AND NOT REGEXP_CONTAINS(_url, '-announces-suite-of-new-generative-ai-features') AND NOT REGEXP_CONTAINS(_url,'=') AND REGEXP_CONTAINS(_url,'/news/blend-earns-2023-great-place-to-work-certification-tm') THEN 'News'
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
         --WHEN (WHEN _url LIKE '%hsa_%' THEN SPLIT(_url,'?hsa_acc')[SAFE_OFFSET(0)]) = 'www.blend360.com/'
         ELSE 'Uncategorized'
         END AS _Page_Category
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON _companyname =df_name 
    --FROM `x-marketing.blend360_mysql.db_dealfront_web_visits` 
   -- LEFT JOIN  `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` ON  _companyname =df_name 
   WHERE PARSE_DATE('%m/%d/%Y',_visitstartdate) >= "2023-10-01"
 )
  ), email_campaign AS (
     SELECT     	
        std_name AS _standardizedcompanyname,
        industry,
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
    -- AND CAST(_timestamp AS DATE) >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL -90 DAY)
    -- AND CAST(_timestamp AS DATE) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)

  ),download_contact_form AS (
  SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _company AS _standardizedcompanyname,
       _industry,
        'Form Fill - Contact Us' AS _engagement,
        activity.email AS _email,
        NULL AS _click_impression,
        COALESCE(form_title, campaign.name) AS _campaign,
        activity.timestamp AS _timestamp,
        '' AS _Page_Category,
        '' AS _page_group,
        ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) AS rownum
      FROM (
        SELECT
          c._sdc_sequence,
          CAST(NULL AS STRING) AS devicetype,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
          form.value.title AS form_title,
          properties.email.value AS email,
          CAST(form.value.timestamp AS DATE) AS timestamp,
          form.value.page_url AS description,
          campaignguid,
           COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
           INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
           LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain, 
        FROM
          `x-marketing.blend360_hubspot_v2.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.blend360_hubspot_v2.forms` forms
        ON
          form.value.form_id = forms.guid
          WHERE form.value.form_id IN ( 'd82baf06-b18c-4057-8ded-7ec8c39c40e0' )
          --AND 
          --properties.email.value = 'rohithb2143@gmail.com'
          ) activity
      LEFT JOIN
        `x-marketing.blend360_hubspot_v2.campaigns` campaign
      ON
        activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 
 ),download_content AS (
    SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _company AS _standardizedcompanyname,
       _industry,
        'Form Fill - Content' AS _engagement,
        activity.email AS _email,
        NULL AS _click_impression,
        COALESCE(form_title, campaign.name) AS _campaign,
        activity.timestamp AS _timestamp,
        '' AS _Page_Category,
        '' AS _page_group,
        ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) AS rownum
      FROM (
        SELECT
          c._sdc_sequence,
          CAST(NULL AS STRING) AS devicetype,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
          form.value.title AS form_title,
          properties.email.value AS email,
          CAST(form.value.timestamp AS DATE) AS timestamp,
          form.value.page_url AS description,
          campaignguid,
           COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
           INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
           LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain, 
        FROM
          `x-marketing.blend360_hubspot_v2.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.blend360_hubspot_v2.forms` forms
        ON
          form.value.form_id = forms.guid
          WHERE form.value.form_id IN ( '0c3f1dea-3cfc-40fc-8be3-94622c4e3e51' )
          --AND 
          --properties.email.value = 'rohithb2143@gmail.com'
          ) activity
      LEFT JOIN
        `x-marketing.blend360_hubspot_v2.campaigns` campaign
      ON
        activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 

 ),download_newsletter_form AS (
    SELECT
      * EXCEPT (rownum)
    FROM (
      SELECT
      _company AS _standardizedcompanyname,
       _industry,
        'Form Fill - Newsletter' AS _engagement,
        activity.email AS _email,
        NULL AS _click_impression,
        COALESCE(form_title, campaign.name) AS _campaign,
        activity.timestamp AS _timestamp,
        '' AS _Page_Category,
        '' AS _page_group,
        ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) AS rownum
      FROM (
        SELECT
          c._sdc_sequence,
          CAST(NULL AS STRING) AS devicetype,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
          REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
          form.value.title AS form_title,
          properties.email.value AS email,
          CAST(form.value.timestamp AS DATE) AS timestamp,
          form.value.page_url AS description,
          campaignguid,
           COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
           INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
           LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain, 
        FROM
          `x-marketing.blend360_hubspot_v2.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.blend360_hubspot_v2.forms` forms
        ON
          form.value.form_id = forms.guid
          WHERE form.value.form_id IN ( 'd94f5d20-5982-47aa-a4c0-d7b91e4b3f24' )
          --AND 
          --properties.email.value = 'rohithb2143@gmail.com'
          ) activity
      LEFT JOIN
        `x-marketing.blend360_hubspot_v2.campaigns` campaign
      ON
        activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 

 ),download AS (
  SELECT * 
 FROM (
  SELECT  
 std_name AS _standardizedcompanyname,
        industry,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
 LEFT JOIN download_contact_form on hs_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Contact Us'
 UNION ALL
 SELECT
 std_name AS _standardizedcompanyname,
        industry,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
 LEFT JOIN download_content on hs_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Content'
  UNION ALL
 SELECT
 std_name AS _standardizedcompanyname,
        industry,
        _engagement,
        _email,
        _click_impression,
         _campaign,
         _timestamp,
         _Page_Category,
         _page_group,
 FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
 LEFT JOIN download_newsletter_form  on hs_name =  _standardizedcompanyname
 WHERE _engagement = 'Form Fill - Newsletter'
 ) WHERE _timestamp >= "2023-10-01"
 ), engagement_score AS (

  SELECT * , "Engagement Score" AS score_type FROM email_campaign 
  UNION  ALL
  SELECT  *, "Engagement Score" AS score_type FROM webtraffic
   UNION  ALL
  SELECT  *, "Engagement Score" AS score_type FROM download 
  ) SELECT * FROM engagement_score 
  UNION ALL 
  SELECT * FROM awareness_score_type ;



CREATE OR REPLACE TABLE `x-marketing.blend360.abm_score` AS
WITH account AS (
  SELECT DISTINCT _standardizedcompanyname, COALESCE(g.industry,h.industry) AS industry 
  FROM `x-marketing.blend360.abm_score_engagement` g
  LEFT JOIN  `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` h ON  _standardizedcompanyname = std_name
  --WHERE _standardizedcompanyname = 'KBR'
), score_calculate AS (
  ----- awareness score 

   SELECT _standardizedcompanyname,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - LI' THEN _click_impression END) AS _li_impression,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Clicked - LI' THEN _click_impression END) AS _li_click,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - FD' THEN _click_impression END) AS _fd_impression,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Clicked - FD' THEN _click_impression END) AS _fd_click,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Impressions - DB' THEN _click_impression END) AS _db_impression,
SUM (CASE WHEN _click_impression >= 1 AND _engagement = 'Clicked - DB' THEN _click_impression END) AS _db_click,
--------- engagement score 
--- webtraffic

COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category = 'Case Studies' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_case_study,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Domain'  THEN CONCAT(_email, _contentTitle) END) AS _dealfront_domain,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'AI'  THEN CONCAT(_email, _contentTitle) END) AS _dealfront_ai_page,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Industries'  THEN CONCAT(_email, _contentTitle) END) AS _dealfront_industry,
COUNT (CASE WHEN _engagement = 'Dealfront - Web Visit'  AND _page_group = 'Capabilities'  THEN CONCAT(_email, _contentTitle) END) AS _dealfront_capability,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_category IN ("About Us",'Home Page')    THEN /*CONCAT(_date, _contentTitle)*/ _contentTitle END) AS _dealfront_homepage,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group = 'Insights'  THEN /*CONCAT(_date, _contentTitle)*/ _contentTitle END) AS _dealfront_thought_leader,
COUNT ( CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group =  'Life @ Blend' THEN /*CONCAT(_date, _contentTitle)*/ _contentTitle END) AS _dealfront_career, 

---- email campaign

COUNT ( CASE WHEN _engagement = 'Clicked - Email'  AND _page_category = 'Campaign' THEN CONCAT(_email, _contentTitle) END) AS _email_click_campaign,

-- website action 
COUNT (DISTINCT CASE WHEN _engagement = 'Dealfront - Web Visit'  THEN  _email END) AS _dealfront_sesion,
COUNT (DISTINCT CASE WHEN _engagement = 'Dealfront - Web Visit' AND _page_group IN ('Case Studies','Domain','AI','Industries','Capabilities') THEN CONCAT(_email, _contentTitle) END) AS _dealfront_target_page,
--- website action form fill 

COUNT ( CASE WHEN _engagement = 'Form Fill - Contact Us' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_contact_us,

COUNT ( CASE WHEN _engagement = 'Form Fill - Content' THEN CONCAT(_email, _contentTitle) END) AS _dealfront_content,

COUNT ( CASE WHEN _engagement = 'Form Fill - Newsletter'   THEN CONCAT(_email, _contentTitle) END) AS _dealfront_newsletter,

---- email nurture 
COUNT (DISTINCT CASE WHEN _engagement = 'Clicked - Email' AND _page_category = 'Nuture'  THEN CONCAT(_email, _contentTitle) END) AS _email_click_nurture,

---- content syndication 

COUNT (DISTINCT CASE WHEN _engagement = 'Content Syndication'  THEN CONCAT(_email, _contentTitle) END) AS _content_syndication,

FROM `x-marketing.blend360.abm_score_engagement`
--WHERE  _standardizedcompanyname = '3M'
GROUP BY 1
), action_score AS (
   SELECT *,

--- awareness score 
((COALESCE(_li_impression,0)) + (COALESCE(_fd_impression,0)) + (COALESCE(_db_impression,0))) _total_impresion,
((COALESCE(_li_click,0)) + (COALESCE(_fd_click,0)) + (COALESCE(_db_click,0))) _total_click,
((COALESCE(_li_impression,0)*0.1) + (COALESCE(_fd_impression,0)*0.1) + (COALESCE(_db_impression,0)*0.1)) _total_score_impresion,
((COALESCE(_fd_click,0)*0.5) + (COALESCE(_li_click,0)*0.5) + (COALESCE(_db_click,0)*0.5)) _total_score_click, 

----- website traffic 
COALESCE(_dealfront_case_study,0)*20 AS _dealfront_case_study_score,
COALESCE(_dealfront_domain,0)*10 AS _dealfront_domain_score,
COALESCE(_dealfront_ai_page,0)*10 AS _dealfront_ai_page_score,
COALESCE(_dealfront_industry,0)*10 AS _dealfront_industry_score,
COALESCE(_dealfront_capability,0)*10 AS _dealfront_capability_score,
COALESCE(_dealfront_homepage,0)*5 AS _dealfront_homepage_score,
COALESCE(_dealfront_thought_leader,0)*3 AS _dealfront_thought_leader_score,
COALESCE(_dealfront_career,0)*1 AS _dealfront_career_score,

---- email campaign 
COALESCE(_email_click_campaign,0)*5 AS _email_click_campaign_score,

--- web  action 
CAST(CASE WHEN _dealfront_sesion >= 3 THEN 15 ELSE 0 END AS INT64) AS _dealfront_sesion_score,
CAST(CASE WHEN _dealfront_target_page >= 3 THEN 15 ELSE 0 END AS INT64) AS _dealfront_target_page_score,
--COALESCE(_dealfront_sesion,0)*15 AS _dealfront_sesion_score,
--COALESCE(_dealfront_target_page,0)*15 AS _dealfront_target_page_score,

COALESCE(_dealfront_contact_us,0)*25 AS _dealfront_contact_us_score,
COALESCE(_dealfront_content,0)*20 AS _dealfront_content_score,
COALESCE(_dealfront_newsletter,0)*8 AS _dealfront_newsletter_score,

COALESCE(_email_click_nurture,0)*8 AS _email_click_nurture_score,

FROM 
score_calculate
) SELECT account.*,
action_score.* EXCEPT ( _standardizedcompanyname) ,
(_total_score_impresion + _total_score_click) AS _awareness_score,
(_dealfront_case_study_score+_dealfront_domain_score+_dealfront_ai_page_score+_dealfront_industry_score+_dealfront_capability_score+_dealfront_homepage_score+_dealfront_thought_leader_score+_dealfront_career_score+_email_click_campaign_score+_dealfront_sesion_score+_dealfront_target_page_score+_dealfront_contact_us_score+_dealfront_content_score+_dealfront_newsletter_score+_email_click_nurture_score) AS _engagement_score,
(_total_score_impresion + _total_score_click) + (_dealfront_case_study_score+_dealfront_domain_score+_dealfront_ai_page_score+_dealfront_industry_score+_dealfront_capability_score+_dealfront_homepage_score+_dealfront_thought_leader_score+_dealfront_career_score+_email_click_campaign_score+_dealfront_sesion_score+_dealfront_target_page_score+_dealfront_contact_us_score+_dealfront_content_score+_dealfront_newsletter_score+_email_click_nurture_score) AS _total_score
FROM account
LEFT JOIN action_score  ON account._standardizedcompanyname = action_score._standardizedcompanyname