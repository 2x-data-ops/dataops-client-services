WITH prospect_info AS (
    SELECT 
        _id AS _prospectid,
        _email,
        _name,
        _domain,
        _jobtitle,
        _function,
        _seniority,
        _phone,
        _company,
        _revenue,
        _industry,
        _city,
        _state,
        _country,
        _persona,
        _lifecycleStage
    FROM 
        `x-marketing.duckcreek.db_icp_database_log`

)