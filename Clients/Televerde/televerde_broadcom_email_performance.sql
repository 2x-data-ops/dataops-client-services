CREATE OR REPLACE TABLE `x-marketing.televerde_broadcom.db_email_performance` AS

SELECT 
  * EXCEPT(_sent, _clicked, _opened, _delivered, _bounced, _unsubscribed, _numemployeesrange, _revenuerange, _sdc_table_version, _sdc_batched_at, _sdc_received_at, _sdc_sequence, _batchid, _id),
  CASE
    WHEN _sent = 'Yes' THEN 1
    ELSE 0
  END AS _sent,
  CASE
    WHEN _clicked = 'Yes' THEN 1
    ELSE 0
  END AS _clicked,
  CASE
    WHEN _opened = 'Yes' THEN 1
    ELSE 0
  END AS _opened,
  CASE
    WHEN _delivered = 'Yes' THEN 1
    ELSE 0
  END AS _delivered,
  CASE
    WHEN _unsubscribed = 'Yes' THEN 1
    ELSE 0
  END AS _unsubscribed,
  CASE
    WHEN _bounced = 'Yes' THEN 1
    ELSE 0
  END AS _bounced,
  CASE
    WHEN _revenuerange = '1B+' THEN '1B +'
    ELSE _revenuerange
  END AS _revenuerange,
  CASE
  WHEN _numemployeesrange = '44935' THEN '20,000+'
  WHEN _numemployeesrange = '45223' THEN '20,000+'
  ELSE _numemployeesrange
END AS _numemployeesrange

FROM 
  `x-marketing.televerde_mysql_2.db_email_performance` 

WHERE 
  /*
  _batchid = (
    SELECT MAX(_batchid) FROM 
    `x-marketing.televerde_mysql_2.db_email_performance`
    )
  AND
  */
  (_emailaddress IS NOT NULL AND _emailaddress != '')