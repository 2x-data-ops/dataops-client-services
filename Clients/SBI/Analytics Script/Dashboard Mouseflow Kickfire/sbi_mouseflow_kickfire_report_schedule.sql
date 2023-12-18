-- CREATE OR REPLACE TABLE `x-marketing.sbi.web_engagement_new_snapshot` AS 
INSERT INTO `x-marketing.sbi.web_engagement_new_snapshot`
WITH new_web_engagements AS (
    SELECT  
        DATE(_timestamp) AS _engagement_date,
        TIMESTAMP(DATETIME(_timestamp,'Asia/Kuala_Lumpur')) AS _timestamp,
        _name AS _companyname,
        _domain AS _companydomain,
        CASE
            WHEN _location = ''
            THEN 'N/A'
            ELSE COALESCE(_location,"N/A") 
        END AS _location,
        CASE
            WHEN _revenue = ''
            THEN 'N/A'
            ELSE COALESCE(_revenue,"N/A") 
        END AS _revenue,
        CASE
            WHEN _industry = ''
            THEN 'N/A'
            ELSE COALESCE(_industry,"N/A") 
        END AS _industry,
        CASE
            WHEN _phone = ''
            THEN 'N/A'
            ELSE COALESCE(_phone,"N/A") 
        END AS _phone,
        _webActivity AS _engagements,
        CURRENT_DATE('America/Chicago') AS extract_date,
        TIMESTAMP(CURRENT_DATETIME('Asia/Kuala_Lumpur')) AS run_date
    FROM `x-marketing.sbi.dashboard_mouseflow_kickfire`
    WHERE
        _domain IS NOT NULL
        AND _domain <> 'brightcove.com'
        AND _domain <> '2x.marketing'
        AND _domain <> 'sbigrowth.com'
        AND _domain <> ''
)
SELECT 
    * 
FROM new_web_engagements 
WHERE extract_date NOT IN (
    SELECT DISTINCT
        extract_date
    FROM `x-marketing.sbi.web_engagement_new_snapshot`
);


-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.sbi.web_engagement_new_export` AS 
SELECT 
    * 
FROM `x-marketing.sbi.web_engagement_new_snapshot`
-- Only consider yesterday's data
-- WHERE CAST(run_date AS DATE) = CURRENT_DATE('Asia/Kuala_Lumpur')-1
WHERE
    -- _engagement_date = CURRENT_DATE('America/New_York')-1
    _engagement_date = CURRENT_DATE('America/Chicago')










    