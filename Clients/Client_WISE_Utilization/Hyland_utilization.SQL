-- TRUNCATE TABLE `x-marketing.hyland.hyland_request_project` ;
-- INSERT INTO `x-marketing.hyland.hyland_request_project` 
CREATE OR REPLACE TABLE `x-marketing.hyland.hyland_request_project` AS
WITH 
-- queries to all the data sources
user AS (
   SELECT
      DISTINCT 
      wise_user._wgEmail,
      wise_user._id,
      wise_user._name,
      hyland._function,
      wise_user._image,
      wise_user._LIimage,
      wise_user._client
   FROM
   `x-marketing.wise_internal.portal_users` wise_user
   LEFT JOIN
   `x-marketing.hyland_mysql.db_airtable_hyland_functions` hyland
   ON wise_user._name = hyland._name
   WHERE LOWER(wise_user._client) = 'hyland'
),
service AS (
   SELECT
      _id,
      _title AS _ticket_type,
      _category AS _ticket_category,
      _client
   FROM
      `x-marketing.wise_internal.request_services`
   WHERE
      _status = 1
),
tickets AS (
   SELECT
      TIMESTAMP_SECONDS(_created) AS _ticket_created,
      details._id AS _ticket_id,
      _projectTitle AS _ticket_title,
      _createdBy AS _requester_id,
      _name AS _requester_name,
      _ownerID AS _owner_id,
      _ownedBy AS _ticket_owner,
      _lastUpdated AS _ticket_last_updated,
      _type AS _ticket_type_id,
      TIMESTAMP_SECONDS(_deadline) AS _ticket_deadline,
      _stage AS _ticket_stage,
      _status AS _ticket_removed,
      details._client,
      CONCAT(
            'https://',
            details._client,
            '.wise-portal.com/request-summary.php?id=',
            details._id
      ) AS _ticket_url,
      service.* EXCEPT (_id, _client),
      user._function
   FROM
      `x-marketing.wise_internal.request_project` details
   LEFT JOIN user 
      ON details._createdBy = user._id AND details._client = user._client
   LEFT JOIN service 
      ON details._type = service._id AND details._client = service._client
   WHERE
      _status = 1 --AND _client = 'truelearn'
   ORDER BY
      TIMESTAMP_SECONDS(_created) DESC
),
stage_ticket AS (
   SELECT 
      details.* 
   FROM (
      SELECT
         _projectID,
         stage._client,
         MAX(
            CASE
               WHEN stage._stage = 0 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage0,
         MAX(
            CASE
               WHEN stage._stage = 10 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage10,
         MAX(
            CASE
               WHEN stage._stage = 30 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage30,
         MAX(
            CASE
               WHEN stage._stage = 50 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage50,
         MAX(
            CASE
               WHEN stage._stage = 80 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage80,
         MAX(
            CASE
               WHEN stage._stage = 100 THEN TIMESTAMP_SECONDS(_datetime)
            END
         ) AS _stage100
      FROM
         `x-marketing.wise_internal.request_project_stage` stage 
      GROUP BY 1,2
   ) details 
), 
ticket_details AS (
   SELECT
      details._projectID,
      details._client,
      MAX(
            CASE
               WHEN _field = 'data-type' THEN _val
            END
      ) AS data_type, 
      MAX(
            CASE
               WHEN _field = 'urgency' THEN _val
            END
      ) AS _ticket_urgency,
      MAX(
            CASE
               WHEN _field = 'comments' THEN _val
            END
      ) AS _details
   FROM
      `x-marketing.wise_internal.request_project_details` details
   GROUP BY 1,2
),
latest_discussions AS (
   SELECT
      * EXCEPT(_order)
   FROM (
      SELECT
         details._projectID,
         details._client,
         _comment,
         _userID,
         TIMESTAMP_SECONDS(_datetime) AS _commenttime,
         _name AS _commenter,
         ROW_NUMBER() OVER(PARTITION BY details._projectid, details._client ORDER BY details._sequence DESC) AS _order
      FROM
         `x-marketing.wise_internal.request_discussion` details
      LEFT JOIN user ON details._userID = user._id
      WHERE
         details._status = 1
   )
   WHERE
      _order = 1
),
time_entry AS (
   SELECT
      DISTINCT _hours AS _logged_hours,
      _wiseid AS _ticket_id,
      LOWER(times._client) AS _client,
      _userid,
      _date,
      -- _function AS _function,
      _group,
      _title AS _ticket_title,
      times._name AS _user_logged_hours,
      _expectedHoursMonth
   FROM
      `x-marketing.webtrack_ipcompany.dashboard_summary_time_entry` times
   LEFT JOIN user ON user._id = times._userid AND user._client = times._client 
),
-- transformation
get_ticket_details AS (
   SELECT
      tickets.*,
      CASE 
         WHEN data_type LIKE '%Troubleshooting%' THEN 'Troubleshooting'
         WHEN data_type LIKE '%New Requirements/Iterations%' THEN 'New Requirements/Iterations'
         WHEN data_type LIKE '%Initial Build%' THEN 'Initial Build'
         ELSE 'Empty'
      END AS _dataops_data_type,
      ticket_details._ticket_urgency,
      ticket_details._details,
      latest_discussions._commenter,
      latest_discussions._commenttime,
      latest_discussions._comment,
      stage_ticket._stage0,
      stage_ticket._stage10,
      stage_ticket._stage30,
      stage_ticket._stage50,
      stage_ticket._stage80,
      stage_ticket._stage100,
      CASE
         WHEN _stage10 IS NULL THEN 'Not Start'
         ELSE 'Start Stage'
      END AS _stagenotstart10,
      CASE
         WHEN _stage30 IS NULL THEN 'Not Start'
         ELSE 'Start Stage'
      END AS _stagenotstart30,
      CASE
         WHEN _stage50 IS NULL THEN 'Not Start'
         ELSE 'Start Stage'
      END AS _stagenotstart50,
      CASE
         WHEN _stage80 IS NULL THEN 'Not Start'
         ELSE 'Start Stage'
      END AS _stagenotstart80,
      CASE
         WHEN _stage100 IS NULL THEN 'Pending from DA Close'
         ELSE 'Ticket Complete'
      END AS _stagenotstart100,
      DATE_DIFF(_stage0, _ticket_created, DAY) AS _days_in_stage0,
      DATE_DIFF(_stage10, _ticket_created, DAY) AS _days_in_stage10,
      DATE_DIFF(_stage30, _ticket_created, DAY) AS _days_in_stage30,
      DATE_DIFF(_stage50, _ticket_created, DAY) AS _days_in_stage50,
      DATE_DIFF(_stage80, _ticket_created, DAY) AS _days_in_stage80,
      DATE_DIFF(_stage100, _ticket_created, DAY) AS _days_in_stage100,
      CASE
         WHEN 
            _stage0 IS NOT NULL
            AND _stage10 IS NULL
            AND _stage30 IS NULL
            AND _stage50 IS NULL
            AND _stage80 IS NULL
            AND _stage100 IS NULL 
         THEN "Open"
         WHEN 
            _stage0 IS NOT NULL
            AND _stage10 IS NOT NULL
            AND _stage30 IS NOT NULL
            AND _stage50 IS NOT NULL
            AND _stage80 IS NOT NULL
            AND _stage100 IS NULL 
         THEN "Open"
         ELSE "TIcket Close"
      END AS _open_ticket,
      CASE
         WHEN 
            _stage0 IS NOT NULL
            AND _stage10 IS NOT NULL
            AND _stage30 IS NOT NULL
            AND _stage50 IS NOT NULL
            AND _stage80 IS NULL
            AND _stage100 IS NULL 
         THEN "Pending"
      END AS _pending_ticket,
      CASE
         WHEN DATE_DIFF(_stage80, _ticket_created, DAY) > 2
         AND _ticket_urgency = 'High (within 4 Hours)' THEN "Overdue"
         WHEN DATE_DIFF(_stage80, _ticket_created, DAY) > 4
         AND _ticket_urgency = 'Medium (within 24 Hours)' THEN "Overdue"
         WHEN DATE_DIFF(_stage80, _ticket_created, DAY) > 7
         AND _ticket_urgency = 'Low' THEN "Overdue"
      END AS _overdue_ticket
   FROM
      tickets
   LEFT JOIN
      ticket_details
      ON CONCAT(tickets._ticket_id, tickets._client) = CONCAT(ticket_details._projectid, ticket_details._client )
   LEFT JOIN
      latest_discussions
      ON CONCAT(tickets._ticket_id, tickets._client) = CONCAT(latest_discussions._projectid, latest_discussions._client)
   LEFT JOIN stage_ticket ON CONCAT(tickets._ticket_id, tickets._client) = CONCAT(stage_ticket._projectid, stage_ticket._client)
),
get_ticket_utiliztion AS (
   SELECT
      time_entry.*,
      get_ticket_details.* EXCEPT(_client,_ticket_id, _ticket_title)
   FROM
      time_entry
   LEFT JOIN   
      get_ticket_details
      ON CONCAT(time_entry._ticket_id, time_entry._client) = CONCAT(get_ticket_details._ticket_id, get_ticket_details._client)
)
SELECT 
  * 
FROM 
  get_ticket_utiliztion
WHERE lower(_client) = 'hyland';