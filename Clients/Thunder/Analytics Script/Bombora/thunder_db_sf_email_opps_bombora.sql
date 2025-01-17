CREATE OR REPLACE TABLE `x-marketing.thunder.db_sf_email_opps_bombora` AS
WITH _temp_bombora_account AS (
    SELECT
        _company_domain,
        COUNT(_email) AS  `GB_placeholder`,
        "Yes" AS `_bombora_account`
    FROM `x-marketing.thunder_mysql.db_thunder_purchased_contacts_bombora`
    GROUP BY _company_domain
),
_2x_campaign_pa AS (
  SELECT 
    Opp_id, 
    COUNT(_prospectID) AS `Count of Prospects`,
    "Yes" AS `_2x_campaign_pa`
    FROM `x-marketing.thunder.db_email_opps_combined`
    WHERE _engagement IN ('Opened', 'Clicked') 
        AND _utmcampaign IN (
        'Email Campaign - Pipeline Acceleration', 
        '20231115-PipelineAcc-EM-WC-Thunder_Will_Make_You_Love_Change_Management-W1',
        '20231128-PipelineAcc-EM-EB-Your_One_Stop_Solution_To_Salesforce_Success-W1',
        '20231201-PipelineAcc-EM-EB-Your_One_Stop_Solution_To_Salesforce_Success-W2',
        '20231205-PipelineAcc-EM-WC-Thunder_Activation-W1',
        '20231211-PipelineAcc-EM-WC-Thunder_Activation-W2',
        '20231214-PipelineAcc-EM-WC-Thunder_Amplify-W1',
        '20231219-PipelineAcc-EM-WC-Thunder_Amplify-W2',
        '20240103-PipelineAcc-EM-WC-Its-Time-To-AMP-UP-Your-Success-With-Amplify-W1',
        '20240108-PipelineAcc-EM-WC-Its-Time-To-AMP-UP-Your-Success-With-Amplify-W2',
        '20240117-PipelineAcc-EM-WC-Thunder-Salesforce-Partnership-Real-Results',
        '20240122-PipelineAcc-EM-WC-Thunder-Salesforce-Partnership-Real-Results-W2')
      AND DATE_DIFF(opp_created_date, _timestamp, DAY) BETWEEN 0 AND 90
  GROUP BY Opp_id
),
_2x_campaign_pg AS (
    SELECT 
        Opp_id, 
        COUNT(_prospectID) AS `Count of Prospects`,
        "Yes" AS `_2x_campaign_pg`
    FROM `x-marketing.thunder.db_email_opps_combined`
    WHERE _engagement IN ('Opened', 'Clicked') 
      AND _utmcampaign IN (
        'Email Campaign - Pipeline Generation CPQ & Billing FY25',
        'Email Campaign - Pipeline Generation Service Cloud FY25',
        'Email Campaign - Pipeline Generation Marketing Cloud FY25',
        '20231214-PipelineGen-EM-WC-Give-PEACE-A-Chance-With-CPQ-Solutions-W1',
        '20231219-PipelineGen-EM-WC-Three-Tips-For-Using-Salesforce-Billing',
        '20240109-PipelineGen-EM-WC-Complex-Pricing-Salesforce-CPQ-Solution-For-Hyphen-So',
        '20240116-PipelineGen-EM-WC-Billing-Solution-For-Education-Tech-Platform-SchooLin',
        '20240123-PipelineGen-EM-WC-Accelerating-Revenue-Cloud-Success-With-Thunder'
)
      AND DATE_DIFF(opp_created_date, _timestamp, DAY) BETWEEN 0 AND 90
    GROUP BY Opp_id
),
temp_opps AS (
    SELECT 
        sf._campaignID,
        sf._engagement,
        sf._email,
        sf._prospectID,
        sf._timestamp,
        sf._description,
        sf._list_email_id,
        sf._name,
        sf._phone,
        sf._jobtitle,
        sf._seniority,
        sf._company,
        sf._domain,
        sf._industry,
        sf._country,
        sf._city,
        sf._annualrevenue,
        sf._employees,
        sf._subject,
        sf._landingPage,
        sf._utm_source,
        sf._utmcampaign,
        sf._utm_medium,
        sf._contentID,
        sf._contentTitle,
        sf._state,
        sf._function,
        sf._utm_content,
        sf._campaignSentDate,
        sf._subCampaign,
        sf._stage,
        sf._lists,
        sf._contenttype,
        sf._createdby,
        sf._emailname,
        sf._form_id,
        sf._account_owner,
        sf._email_template_id,
        sf._email_template_name,
        sf._campaign_name_gs,
        sf._email_type_gs,
        sf.opp_id,
        sf.domain,
        sf.opp_type,
        sf.opp_name,
        sf.opp_stage,
        sf.opp_amount,
        sf.opp_close_date,
        sf.owner_id,
        sf.owner_name,
        sf.products,
        sf.opp_created_date,
        sf.lead_source,
        sf.account_annual_revenue,
        sf.account_employee_count,
        sf.account_name,
        sf.account_id,
        sf.account_sector,
        CASE WHEN ba._bombora_account IS NULL THEN "No"ELSE ba._bombora_account
        END AS `_bombora_account` ,
        CASE WHEN pa._2x_campaign_pa IS NULL THEN "No" ELSE pa._2x_campaign_pa
        END AS `_2x_campaign_pa`,
        CASE WHEN pg._2x_campaign_pg IS NULL THEN "No" ELSE pg._2x_campaign_pg
        END AS `_2x_campaign_pg`
    FROM `x-marketing.thunder.db_email_opps_combined` AS sf
    LEFT JOIN _temp_bombora_account AS ba
        ON sf.domain = ba._company_domain
    LEFT JOIN _2x_campaign_pa AS pa
        ON sf.opp_id = pa.opp_id
    LEFT JOIN _2x_campaign_pg AS pg
        ON sf.opp_id = pg.opp_id
),
gb_opps AS(
    SELECT
        opp_id,
        COUNT(opp_id) AS `_no_of_engagement`
    FROM temp_opps
    GROUP BY opp_id
)
SELECT
    _campaignID,
    _engagement,
    _email,
    _prospectID,
    _timestamp,
    _description,
    _list_email_id,
    _name,
    _phone,
    _jobtitle,
    _seniority,
    _company,
    _domain,
    _industry,
    _country,
    _city,
    _annualrevenue,
    _employees,
    _subject,
    _landingPage,
    _utm_source,
    _utmcampaign,
    _utm_medium,
    _contentID,
    _contentTitle,
    _state,
    _function,
    _utm_content,
    _campaignSentDate,
    _subCampaign,
    _stage,
    _lists,
    _contenttype,
    _createdby,
    _emailname,
    _form_id,
    _account_owner,
    _email_template_id,
    _email_template_name,
    _campaign_name_gs,
    _email_type_gs,
    ot.opp_id,
    domain,
    opp_type,
    opp_name,
    opp_stage,
    opp_amount,
    opp_close_date,
    owner_id,
    owner_name,
    products,
    opp_created_date,
    lead_source,
    account_annual_revenue,
    account_employee_count,
    account_name,
    account_id,
    account_sector,
    _bombora_account,
    _2x_campaign_pa,
    _2x_campaign_pg,
    gb._no_of_engagement,
    (ot.opp_amount / gb._no_of_engagement) AS `amount_per_engagement`
FROM temp_opps AS ot
LEFT JOIN gb_opps AS gb
    ON ot.opp_id = gb.opp_id