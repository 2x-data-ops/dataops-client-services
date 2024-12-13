CREATE OR REPLACE TABLE `x-marketing.sandler_network.db_all_hubspot_leads` AS
WITH companies AS (

    SELECT  

        companyid AS company_id_cmp,
        property_name.value AS company_cmp,
        property_country.value AS country_cmp,
        property_child_portal_id.value AS franchise_child_hs_portal_id,
        property_eligible_for_lead_distro.value AS franchise_eligible_for_lead_distribution,
        property_marketing_code.value AS marketing_code,
        property_nbc_franchise_email_name.value AS nbc_lead_assignment_referral_poc_email,
        property_nbc_franchise_poc_name.value AS nbc_lead_assignment_referral_poc_name,
        property_zip.value AS postal_code_cmp,
        property_state.value AS state_cmp    

    FROM 
        `x-marketing.sandler_network_hubspot.companies`
 

),

-- Get all contacts from Hubspot
contacts AS (

    SELECT  
        vid AS contact_id,
        REPLACE(TRIM(REPLACE(property_network_contact_opportunity_status_setby_portal.value, ';', ' ')), ' ', ', ') AS active_opp_network_contact_status_portal_ids,
        property_age.value AS age,
        property_hs_lifecyclestage_customer_date.value AS became_customer_date,
        property_hs_lifecyclestage_lead_date.value AS became_lead_date,
        property_hs_lifecyclestage_marketingqualifiedlead_date.value AS became_mql_date,
        property_hs_lifecyclestage_salesqualifiedlead_date.value AS became_sql_date,
        property_hs_lifecyclestage_subscriber_date.value AS became_subscriber_date,
        property_hs_lifecyclestage_evangelist_date.value AS became_evangelist_date,
        property_hs_lifecyclestage_opportunity_date.value AS became_opportunity_date,
        property_hs_lifecyclestage_other_date.value AS became_other_date,
        property_became_customer_in_assigned_franchise_portal.value AS became_customer_in_assigned_franchise_portal,
        --TIMESTAMP_MILLIS(CAST(property_became_network_contact_date.value AS INT64)) AS became_network_contact_date,
        TIMESTAMP_MILLIS(SAFE_CAST(property_became_network_contact_date.value AS INT64)) AS became_network_contact_date,
        property_city.value AS city,
        associated_company.company_id AS company_id,
        property_company.value AS company,
        property_hubspot_owner_id.value AS contact_owner,
        property_contact_type.value AS contact_type,
        property_hs_is_unworked.value AS contact_unworked,
        property_country__picklist_.value AS country_picklist,
        property_country.value AS country,
        property_createdate.value AS created_date,
        property_hs_v2_cumulative_time_in_lead.value AS cumulative_time_in_lead,
        property_hs_v2_cumulative_time_in_marketingqualifiedlead.value AS cumulative_time_in_mql,
        property_current_assigned_nbc_franchise_business_name.value AS current_assigned_nbc_franchise_business_name,
        property_current_assigned_nbc_franchise_poc_email.value AS current_assigned_nbc_franchise_poc_email,
        property_current_assigned_nbc_franchise_poc_name.value AS current_assigned_nbc_franchise_poc_name,
        property_current_assigned_nbc_franchise_portal_id.value AS current_assigned_nbc_franchise_portal_id,
        property_current_assigned_nbc_franchise_shortcode_id.value AS current_assigned_nbc_franchise_shortcode_id,
        property_currently_active_opp_in_assigned_franchise_portal.value AS currently_active_opp_in_assigned_franchise_portal,
        property_hs_sequences_is_enrolled.value as currently_in_sequence,
        property_currentlyinworkflow.value AS currently_in_workflow,
        REPLACE(TRIM(REPLACE(property_network_contact_customer_status_setby_portal.value, ';', ' ')), ' ', ', ') AS customer_network_contact_status_portal_ids,
        property_desired_franchise_city.value AS desired_franchise_city,
        property_desired_franchise_location_zip_code.value AS desired_franchise_location_zip_code,
        property_desired_franchise_state.value AS desired_franchise_state,
        property_desired_franchise_zip_code.value AS desired_franchise_zip_code,
        property_ead_assignment.value AS ead_contact,
        property_email.value AS email,
        property_hs_email_domain.value AS email_domain,
        property_enterprise_contact_status.value AS enterprise_contact_status,
        property_first_conversion_event_name.value AS first_conversion,
        property_first_conversion_date.value AS first_conversion_date,
        property_firstname.value AS first_name,
        TIMESTAMP_MILLIS(CAST(property_first_nbc_lead_assigned_date.value AS INT64)) AS first_nbc_lead_assigned_date,
        property_hs_analytics_first_touch_converting_campaign.value AS first_touch_converting_campaign, -- Concern
        property_industry.value AS industry,
        property_job_function.value AS job_function,
        property_jobtitle.value AS job_title,
        property_notes_last_updated.value AS last_activity_date,
        property_notes_last_contacted.value AS last_contacted,
        property_hs_last_sales_activity_timestamp.value AS last_engagement_date,
        property_lastmodifieddate.value AS last_modified_date,
        property_lastname.value AS last_name,
        property_hs_latest_sequence_ended_date.value AS last_sequence_ended_date,
        property_hs_latest_sequence_enrolled.value AS last_sequence_enrolled,
        property_hs_latest_sequence_enrolled_date.value AS last_sequence_enrolled_date,
        TIMESTAMP_MILLIS(CAST(property_latest_nbc_lead_assigned_date.value AS INT64)) AS latest_nbc_lead_assigned_date,
        property_hs_latest_source.value AS latest_source,
        property_hs_latest_source_timestamp.value AS latest_source_date,
        property_hs_latest_source_data_1.value AS latest_source_drill_down_1,
        property_hs_latest_source_data_2.value AS latest_source_drill_down_2,
        property_hs_v2_latest_time_in_lead.value AS latest_time_in_lead,
        property_hs_v2_latest_time_in_marketingqualifiedlead.value AS latest_time_in_mql,
        REPLACE(TRIM(REPLACE(property_network_contact_lead_status_setby_portal.value, ';', ' ')), ' ', ', ') AS lead_network_contact_status_portal_ids,
        property_lead_assignment_lastsynctonetworkdate.value AS lead_assignment_last_sync_to_franchise_portal_date,
        property_nbc_assignment_synctonetwork.value AS lead_assignment_sync_to_network,
        property_hs_lead_status.value AS lead_status,
        property_shub_left_company.value AS left_company,
        property_lifecyclestage.value AS lifecycle_stage,
        REPLACE(TRIM(REPLACE(property_lms_all_hsportalids.value, ';', ' ')), ' ', ', ') AS lms_all_franchise_hs_portal_ids,
        property_hs_marketable_status.value AS marketing_contact_status,
        property_hs_marketable_reason_id.value AS marketing_contact_status_source_name,
        property_hs_marketable_reason_type.value AS marketing_contact_status_source_type,
        property_shub_marketing_lead_source.value AS marketing_lead_source,
        property_mobilephone.value AS mobile_phone,
        
        REPLACE(TRIM(REPLACE(property_all_assigned_hsportalids.value, ';', ' ')), ' ', ', ') AS nbc_assignment_all_franchise_hs_portal_ids,
        split_nbc_assignment_all_franchise_hs_portal_ids,
        ROW_NUMBER() OVER (PARTITION BY vid) AS _franchise_index,

        property_nbc_campaign___recent_mql_qualifying_event.value AS nbc_campaign_recent_mql_qualifying_event,
        property_network_contact_status.value AS network_contact_status,
        TIMESTAMP_MILLIS(CAST(property_network_contact_status_first_became_lead_date.value AS INT64)) AS network_contact_status_first_became_lead_date,
        TIMESTAMP_MILLIS(CAST(property_network_contact_status___last_became_lead_date.value AS INT64)) AS network_contact_status_last_became_lead_date,
        property_shub_lms_userstatus.value AS new_lms_user_status,
        property_lms_parenttype.value AS new_lms_parent_org_type,
        property_notes_next_activity_date.value AS next_activity_date,
        property_num_conversion_events.value AS number_of_form_submissions,
        property_number_of_nbc_lead_assignments.value AS number_of_nbc_lead_assignemnts,
        property_organization_size.value AS organization_size,
        property_hs_analytics_source.value AS orginal_source,
        property_hs_analytics_source_data_1.value AS orginal_source_drill_down_1,
        property_hs_analytics_source_data_2.value AS orginal_source_drill_down_2,
        property_hubspot_owner_assigneddate.value AS owner_assigned_date,
        property_hs_persona.value AS persona,
        property_phone.value AS phone_number,
        property_zip.value AS postal_code,
        property_shub_prospective_owner_description.value AS prospective_owner_description,
        property_ready_for_lead_assignment_lookups.value AS ready_for_franchisee_lead_assignment_lookups,
        property_ready_for_lead_assignment_notifications.value AS ready_for_lead_assignment_notifications,
        property_recent_assigned_nbc_franchise_shortcode_id.value AS recent_app_assigned_nbc_shortcode_id,
        property_recent_enterprise_conversion_name.value AS recent_enterprise_conversion_name,
        TIMESTAMP_MILLIS(SAFE_CAST(property_recent_enterprise_conversion_date.value AS INT64)) AS recent_enterprise_eligible_conversion_date,
        property_hs_object_id.value AS record_id,
        property_shub_rejected_nbc_lead_reason.value AS rejected_nbc_lead_reason_sandler,
        property_send_to_sandler_rotator_app.value AS send_to_sandler_rotator_app,
        property_state__picklist_.value AS state_picklist,
        property_state.value AS state,
        property_address.value AS street_address,
        property_utm_campaign.value AS utm_campaign,
        property_utm_medium.value AS utm_medium,
        property_utm_source.value AS utm_source,
        property_website.value AS website_url,
        1 AS dummy_colum
    FROM 
        `x-marketing.sandler_network_hubspot.contacts` 
    CROSS JOIN UNNEST(SPLIT(property_all_assigned_hsportalids.value, ';')) AS split_nbc_assignment_all_franchise_hs_portal_ids

),

-- Tie contacts with companies based on franchise portal ID
combined_data AS (

    SELECT

        main.*,
        side.*

    FROM 
        contacts AS main 

    LEFT JOIN 
        companies AS side 
    
    ON 
        main.split_nbc_assignment_all_franchise_hs_portal_ids = side.franchise_child_hs_portal_id

),

-- Get leads and mqls in a long format form; each row has one status
all_leads_and_mqls AS (

    -- Get all lead dates
    SELECT 
        vid AS contact_id, 
        property_createdate.value AS status_date,
        'Lead' AS status
        
    FROM 
        `x-marketing.sandler_network_hubspot.contacts` 

    WHERE 
        property_createdate.value IS NOT NULL

    UNION ALL

    -- Get all MQL dates
    SELECT 
        vid AS contact_id, 
        property_hs_lifecyclestage_marketingqualifiedlead_date.value AS status_date,
        'MQL' AS status
        
    FROM 
        `x-marketing.sandler_network_hubspot.contacts` 

    WHERE 
        property_hs_lifecyclestage_marketingqualifiedlead_date.value IS NOT NULL
    
),

-- Duplicate the main table by the status
duplicate_rows AS (

    SELECT

        main.*,
        side.* EXCEPT(contact_id)

    FROM 
        combined_data AS main 

    JOIN 
        all_leads_and_mqls AS side 
    
    USING(contact_id)

)

SELECT * FROM duplicate_rows;
