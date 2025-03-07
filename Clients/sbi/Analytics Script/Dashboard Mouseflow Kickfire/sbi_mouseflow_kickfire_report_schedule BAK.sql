-- CREATE OR REPLACE TABLE `x-marketing.sbi.web_engagement_new_snapshot` AS 
INSERT INTO `x-marketing.sbi.web_engagement_new_snapshot`
WITH new_web_engagements AS (
    SELECT  
        DATE(_timestamp) AS _engagement_date,
        -- CONCAT(_name,"\n",_domain,"\n\n","Location:","\n",COALESCE(_location,"N/A"),"\n\n","Revenue:","\n",COALESCE(_revenue,"N/A"),"\n\n","Industry:","\n",COALESCE(_industry,"N/A"),"\n\n","General Phone:","\n",COALESCE(_phone,"N/A")) AS _company_information,
        -- TIMESTAMP(DATETIME(_timestamp,'America/Chicago')) AS _timestamp,
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
),
unique_years_involved AS (
    SELECT
        EXTRACT(YEAR FROM _engagement_date) AS year
    FROM new_web_engagements
    UNION DISTINCT
    SELECT
        EXTRACT(YEAR FROM extract_date) AS year
    FROM new_web_engagements
),
all_holiday_dates AS (
    -- [1] New Year's Day (Jan 1)
    SELECT
        "New Year's Day" AS holiday_name,
        DATE(year, 1, 1) AS holiday_date
    FROM unique_years_involved
    UNION ALL
    -- [2] Martin Luther King's Day (Third Monday in Jan)
    SELECT 
        "Martin Luther King's Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 1, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved
    UNION ALL
    -- [3] Presidents' Day (Third Monday in Feb)
    SELECT 
        "Presidents' Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 2, 1),
                    INTERVAL 14 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved
    UNION ALL
    -- [4] Memorial Day (Last Monday in May)
    SELECT 
        "Memorial Day" AS holiday_name,
        DATE_TRUNC(
            LAST_DAY(DATE(year, 5, 1), MONTH), 
            WEEK(MONDAY)
        ) AS holiday_date
    FROM unique_years_involved
    UNION ALL 
    -- [5] Juneteenth (Jun 19)
    SELECT 
        "Juneteenth" AS holiday_name, 
        DATE(year, 6, 19) AS holiday_date
    FROM unique_years_involved 
    UNION ALL
    -- [6] Independence Day (Jul 4)
    SELECT 
        "Independence Day" AS holiday_name,  
        DATE(year, 7, 4) AS holiday_date 
    FROM unique_years_involved     
    UNION ALL
    -- [7] Labor Day (First Monday in Sep)
    SELECT 
        "Labor Day" AS holiday_name, 
        DATE_TRUNC(
            DATE_ADD(
                DATE(year, 9, 1), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved
    UNION ALL
    -- [8] Columbus Day (Second Monday in Oct)
    SELECT 
        "Columbus Day" AS holiday_name,
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 10, 1),
                    INTERVAL 7 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(MONDAY)
        ) AS holiday_date 
    FROM unique_years_involved
    UNION ALL
    -- [9] Veterans Day (Nov 11)
    -- SELECT 
    --     "Veterans Day" AS holiday_name, 
    --     DATE(year, 11, 11) AS holiday_date 
    -- FROM unique_years_involved 

    -- UNION ALL

    -- [10] Thanksgiving (Fourth Thursday in Nov)
    SELECT 
        "Thanksgiving" AS holiday_name,  
        DATE_TRUNC(
            DATE_ADD(
                DATE_ADD(
                    DATE(year, 11, 1),
                    INTERVAL 21 DAY
                ), 
                INTERVAL 6 DAY
            ), 
            WEEK(THURSDAY)
        ) AS holiday_date 
    FROM unique_years_involved 
    UNION ALL   
    -- [11] Christmas Day (Dec 25)
    SELECT 
        "Christmas Day" AS holiday_name,
        DATE(year, 12, 25) AS holiday_date 
    FROM unique_years_involved 
),
add_filler_info AS (
    SELECT 
        EXTRACT(YEAR FROM holiday_date) AS year,
        ROW_NUMBER() OVER(
            PARTITION BY EXTRACT(YEAR FROM holiday_date)
            ORDER BY holiday_date
        ) AS holiday_order,
        holiday_name,
        holiday_date,
        FORMAT_DATE('%A', holiday_date) AS day_of_holiday
    FROM all_holiday_dates
),
replacement_holiday_dates AS (
    SELECT
        *,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN 'Friday'
            WHEN day_of_holiday = 'Sunday' THEN 'Monday'
        END AS replacement_day,
        CASE
            WHEN day_of_holiday = 'Saturday' THEN DATE_SUB(holiday_date, INTERVAL 1 DAY)
            WHEN day_of_holiday = 'Sunday' THEN DATE_ADD(holiday_date, INTERVAL 1 DAY)
        END AS replacement_date
    FROM add_filler_info
),
actual_holiday_dates AS (
    SELECT
        *,
        COALESCE(replacement_date, holiday_date) AS actual_holiday_date
    FROM replacement_holiday_dates
),
cross_join_leads_with_holidays AS (
    SELECT 
        main.*,
        side.actual_holiday_date
    FROM new_web_engagements AS main
    CROSS JOIN actual_holiday_dates AS side
),
count_total_days_between_date_range AS (
    SELECT
        *,
        DATE_DIFF(extract_date, _engagement_date, DAY)  AS total_days
    FROM cross_join_leads_with_holidays
),
count_total_weekends_between_date_range AS (
    SELECT
        *, 
        (
            -- Get the number of weekend days in between
            (DATE_DIFF(extract_date, _engagement_date, WEEK) * 2)
            + 
            -- If start date was Sunday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM _engagement_date) = 1 THEN 1 ELSE 0 END 
            + 
            -- If end date was Saturday, it won't add to weekends, so add it
            CASE WHEN EXTRACT(DAYOFWEEK FROM extract_date) = 7 THEN 1 ELSE 0 END
        ) AS total_weekends
    FROM count_total_days_between_date_range
),
count_total_holidays_between_date_range AS (
    SELECT
        * EXCEPT(actual_holiday_date, in_date_range),
        COALESCE(SUM(in_date_range), 0) AS total_holidays
    FROM (
        SELECT
            *,
            CASE
                WHEN actual_holiday_date BETWEEN _engagement_date AND extract_date
                THEN 1
            END in_date_range
        FROM count_total_weekends_between_date_range
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),
calculate_days_in_new_stage AS (
    SELECT
        *,
        (total_days - total_weekends - total_holidays) AS days_in_new_stage
    FROM count_total_holidays_between_date_range
)
SELECT 
    * 
FROM calculate_days_in_new_stage 
WHERE extract_date NOT IN (
    SELECT DISTINCT
        extract_date
    FROM `x-marketing.sbi.web_engagement_new_snapshot`
)
ORDER BY 14 DESC;


-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.sbi.web_engagement_new_export` AS 
SELECT 
    * EXCEPT(days_in_new_stage)
    -- _engagement_date,
    -- CAST(run_date AS DATE) AS run_date,
    -- _company_information,
    -- _engagements
FROM `x-marketing.sbi.web_engagement_new_snapshot`
-- Only consider yesterday's data
-- WHERE CAST(run_date AS DATE) = CURRENT_DATE('Asia/Kuala_Lumpur')-1
WHERE
    -- _engagement_date = CURRENT_DATE('America/New_York')-1
    _engagement_date = CURRENT_DATE('America/Chicago')-1
-- Exclude running on weekends
    AND EXTRACT(DAYOFWEEK FROM run_date) - 1 NOT IN (0, 6)
-- Exclude running on public holidays
/*AND CAST(run_date AS DATE) NOT IN (
    SELECT actual_holiday_date 
    FROM `x-marketing.pcs.current_year_public_holidays` 
)*/
-- Include only those in 3 or 7 days
/*AND days_in_new_stage IN(1, 2)
AND lead_email <> "vimal.ramanathan@2x.marketing"*/