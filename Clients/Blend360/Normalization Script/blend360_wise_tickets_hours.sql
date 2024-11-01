-- CREATE OR REPLACE TABLE `x-marketing.blend360.wise_tickets_hours` AS

TRUNCATE TABLE `x-marketing.blend360.wise_tickets_hours`;
INSERT INTO `x-marketing.blend360.wise_tickets_hours`
WITH
  tickets AS (

    SELECT
      DISTINCT _date AS date, 
      _user_logged_hours AS name, 
      _logged_hours AS hours,
      _ticket_id AS wise_id, 
      _ticket_title AS ticket_title,
      _requester_name AS requester_name, 
      _ticket_created AS created_date,
      _ticket_deadline AS due_date, 
      _ticket_stage AS ticket_stage,
      _ticket_category AS ticket_category, 
      _ticket_type AS ticket_type
    FROM
      `x-marketing.wise.wise_request_project`
    WHERE
      _client = 'blend360'
  ),
  project_details AS (
    SELECT
        DISTINCT 
        details._projectID AS wise_id,
        -- details._client,
        REGEXP_REPLACE(TRIM(_field), "-", "_") AS _field,
        _val,
    FROM
        `x-marketing.wise_internal.request_project_details` details
    /* GROUP BY
        1,
        2 */
    WHERE
      _client = 'blend360'
      AND TRIM(_field) IN (
          'groups',
          'location',
          'industries',
          'domains',
          'partner',
          'marketing-core-campaigns',
          'themes-keywords',
          'marketing-outputs',
          'type')
  ),
  ticket_details AS (
  SELECT 
    * 
  FROM project_details
    PIVOT (
    MAX(_val) FOR _field IN (
            'groups',
            'location',
            'industries',
            'domains',
            'partner',
            'marketing_core_campaigns',
            'themes_keywords',
            'marketing_outputs',
            'type')
        )
  )
SELECT
  *
FROM
  tickets
LEFT JOIN 
  ticket_details
  USING(wise_id)
;