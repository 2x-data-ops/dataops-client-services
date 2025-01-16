CREATE OR REPLACE TABLE `x-marketing.televerde.televerde_db_calling_activity`  AS
WITH original_data AS (
  SELECT * EXCEPT(_contactcreated, _sdc_received_at, _sdc_sequence, _id, _batchid, _sdc_batched_at, _rownumber, _lastcall,_sdc_table_version,_appointmentdatec),
  CASE WHEN _lastcall = "" THEN NULL ELSE CAST( _contactcreated AS DATE) END AS _contactcreated,
  CASE WHEN _lastcall = "" THEN NULL ELSE CAST(_lastcall AS DATE) END AS _lastcall,
  CASE WHEN _appointmentdatec = "" THEN NULL ELSE CAST(_appointmentdatec AS DATE) END AS _appointmentdatec ,
  FROM `x-marketing.televerde_mysql_2.db_all_calling_activity_tel_2024` 
  WHERE CAST(_callingactivityextractiondate AS DATE) = ( SELECT DISTINCT MAX(CAST(_callingactivityextractiondate AS DATE))
  FROM `x-marketing.televerde_mysql_2.db_all_calling_activity_tel_2024` )
)

SELECT * 
--EXCEPT (date,time), TIMESTAMP_ADD(date, INTERVAL (EXTRACT(HOUR FROM time)) HOUR) AS _appointmentdatec
FROM original_data