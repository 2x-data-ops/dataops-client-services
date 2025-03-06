
/*

PARTITION BY DATE(_timestamp)
CLUSTER BY (_domain)

*/


TRUNCATE TABLE `x-marketing.thunder.db_sf_email_opps_bombora`;
INSERT INTO `x-marketing.thunder.db_sf_email_opps_bombora` (
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
    opp_id,
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
    _no_of_engagement,
    amount_per_engagement
)

WITH bombora_account AS (
    SELECT
        _company_domain,
        COUNT(_email) AS  `GB_placeholder`,
        "Yes" AS `_bombora_account`
    FROM `x-marketing.thunder_mysql.db_thunder_purchased_contacts_bombora`
    WHERE _company_domain IS NOT NULL
    GROUP BY _company_domain
),
combined_campaigns AS (
    SELECT 
        opp_id, 
        COUNT(_prospectID) AS `Count of Prospects`,
        MAX(CASE 
            WHEN _campaignID IN (
                '565633',   /* 20231128-PipelineAcc-EM-EB-Your_One_Stop_Solution_To_Salesforce_Success-W1 */
                '569329',   /* 20231214-PipelineAcc-EM-WC-Thunder_Amplify-W1 */
                '564229',   /* 20231115-PipelineAcc-EM-WC-Thunder_Will_Make_You_Love_Change_Management-W1 */
                '566980',   /* 20231205-PipelineAcc-EM-WC-Thunder_Activation-W1 */
                '574781',   /* 20240108-PipelineAcc-EM-WC-Its-Time-To-AMP-UP-Your-Success-With-Amplify-W2 */
                '574250',   /* 20240103-PipelineAcc-EM-WC-Its-Time-To-AMP-UP-Your-Success-With-Amplify-W1 */
                '568294',   /* 20231211-PipelineAcc-EM-WC-Thunder_Activation-W2 */
                '576947',   /* 20240117-PipelineAcc-EM-WC-Thunder-Salesforce-Partnership-Real-Results */
                '570587',   /* 20231219-PipelineAcc-EM-WC-Thunder_Amplify-W2 */
                '577892',   /* 20240122-PipelineAcc-EM-WC-Thunder-Salesforce-Partnership-Real-Results-W2 */
                '566584',   /* 20231201-PipelineAcc-EM-EB-Your_One_Stop_Solution_To_Salesforce_Success-W2 */
                '567823', '567838', '576482', '579707', '576479', '568105', '567811' /* Email Campaign - Pipeline Acceleration */
                ) THEN 'Yes' 
            END) AS `_2x_campaign_pa`,
        MAX(CASE 
            WHEN _campaignID IN (
                '560875','577922','577919','579698','577925','577913',  /* Email Campaign - Pipeline Generation CPQ & Billing FY25 */
                '590557',   /* Email Campaign - Pipeline Generation Service Cloud FY25 */
                '632221',   /* Email Campaign - Pipeline Generation Marketing Cloud FY25 */
                '569776',   /* 20231214-PipelineGen-EM-WC-Give-PEACE-A-Chance-With-CPQ-Solutions-W1 */
                '570590',   /* 20231219-PipelineGen-EM-WC-Three-Tips-For-Using-Salesforce-Billing */
                '575072',   /* 20240109-PipelineGen-EM-WC-Complex-Pricing-Salesforce-CPQ-Solution-For-Hyphen-So */
                '576458',   /* 20240116-PipelineGen-EM-WC-Billing-Solution-For-Education-Tech-Platform-SchooLin */
                '577931'    /* 20240123-PipelineGen-EM-WC-Accelerating-Revenue-Cloud-Success-With-Thunder */
                ) THEN 'Yes'    
            END) AS `_2x_campaign_pg`
    FROM `x-marketing.thunder.db_email_opps_combined`
    WHERE _engagement IN ('Opened', 'Clicked') 
        AND DATE_DIFF(opp_created_date, _timestamp, DAY) BETWEEN 0 AND 90
        AND opp_id IS NOT NULL
        AND _campaignID IN (
        -- Pipeline Acceleration Campaigns
            '565633', '569329', '564229', '566980', '574781', '574250', '568294', '576947', '570587', '577892', '566584', 
            '567823', '567838', '576482', '579707', '576479', '568105', '567811',
            
            -- Pipeline Generation Campaigns
            '560875', '577922', '577919', '579698', '577925', '577913', '590557', '632221', '569776', '570590', '575072', '576458', '577931')
    GROUP BY opp_id
),
opps_w_bombora_campaign AS (
    SELECT 
        email_opps._campaignID,
        email_opps._engagement,
        email_opps._email,
        email_opps._prospectID,
        email_opps._timestamp,
        email_opps._description,
        email_opps._list_email_id,
        email_opps._name,
        email_opps._phone,
        email_opps._jobtitle,
        email_opps._seniority,
        email_opps._company,
        email_opps._domain,
        email_opps._industry,
        email_opps._country,
        email_opps._city,
        email_opps._annualrevenue,
        email_opps._employees,
        email_opps._subject,
        email_opps._landingPage,
        email_opps._utm_source,
        email_opps._utmcampaign,
        email_opps._utm_medium,
        email_opps._contentID,
        email_opps._contentTitle,
        email_opps._state,
        email_opps._function,
        email_opps._utm_content,
        email_opps._campaignSentDate,
        email_opps._subCampaign,
        email_opps._stage,
        email_opps._lists,
        email_opps._contenttype,
        email_opps._createdby,
        email_opps._emailname,
        email_opps._form_id,
        email_opps._account_owner,
        email_opps._email_template_id,
        email_opps._email_template_name,
        email_opps._campaign_name_gs,
        email_opps._email_type_gs,
        email_opps.opp_id,
        email_opps.domain,
        email_opps.opp_type,
        email_opps.opp_name,
        email_opps.opp_stage,
        email_opps.opp_amount,
        email_opps.opp_close_date,
        email_opps.owner_id,
        email_opps.owner_name,
        email_opps.products,
        email_opps.opp_created_date,
        email_opps.lead_source,
        email_opps.account_annual_revenue,
        email_opps.account_employee_count,
        email_opps.account_name,
        email_opps.account_id,
        email_opps.account_sector,
        IFNULL(bombora._bombora_account, "No") AS `_bombora_account`,
        IFNULL(campaigns._2x_campaign_pa, "No") AS `_2x_campaign_pa`,
        IFNULL(campaigns._2x_campaign_pg, "No") AS `_2x_campaign_pg`
    FROM `x-marketing.thunder.db_email_opps_combined` AS email_opps
    LEFT JOIN bombora_account AS bombora
        ON email_opps.domain = bombora._company_domain
    LEFT JOIN combined_campaigns AS campaigns
        ON email_opps.opp_id = campaigns.opp_id
),
unique_opps AS(
    SELECT
        opp_id,
        COUNT(opp_id) AS `_no_of_engagement`
    FROM opps_w_bombora_campaign
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
    opps_w_bombora_campaign.opp_id,
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
    unique_opps._no_of_engagement,
    (opps_w_bombora_campaign.opp_amount / unique_opps._no_of_engagement) AS `amount_per_engagement`
FROM opps_w_bombora_campaign
LEFT JOIN unique_opps 
    ON opps_w_bombora_campaign.opp_id = unique_opps.opp_id;