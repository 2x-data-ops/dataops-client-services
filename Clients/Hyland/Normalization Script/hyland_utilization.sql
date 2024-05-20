CREATE OR REPLACE TABLE `x-marketing.hyland.hyland_time_entry` AS
WITH 
user AS (
   SELECT
      DISTINCT 
      wise_user._wgEmail,
      wise_user._id,
      wise_user._name,
      hyland._function,
      hyland._fte,
      hyland._status,
      hyland._recordtype,
      _client
   FROM
   `x-marketing.wise_internal.portal_users` wise_user
   JOIN
   `x-marketing.hyland_mysql.db_airtable_hyland_functions` hyland
   ON wise_user._name = hyland._name
),
time_entry AS (
   SELECT
      DISTINCT 
      _hours AS _logged_hours,
      _wiseid AS _ticket_id,
      LOWER(times._client) AS _client,
      _userid,
      _date,
      _function AS _function,
      _group,
      _title AS _ticket_title,
      times._name AS _user_logged_hours,
      _expectedHoursMonth,
      _fte,
      _status,
      _recordtype
   FROM
      `x-marketing.webtrack_ipcompany.dashboard_summary_time_entry` times
   INNER JOIN user ON user._name = times._name 
   WHERE LOWER(times._client) IN ('hyland','2x')
   -- AND _active = 'Active'
)

SELECT * FROM time_entry