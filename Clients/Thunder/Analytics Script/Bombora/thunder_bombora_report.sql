
-- This is the exact MySQL table without Stitch related fields
-- The score fields have been converted to integers instead of the default strings

CREATE OR REPLACE TABLE `thunder.bombora_report` AS

SELECT  

    * EXCEPT(
        _sdc_table_version,
        _sdc_received_at,
        _sdc_sequence,
        _id,
        _batchid,
        _sdc_batched_at,
        _rownumber,
        _countrycompositescore,
        _metrocompositescore,
        _compositescore,
        _statecompositescore
    ),

    CASE
        WHEN _countrycompositescore != ''
        THEN CAST(_countrycompositescore AS INT64)
        ELSE CAST(NULL AS INT64)
    END  
    AS _countrycompositescore,

    CASE
        WHEN _metrocompositescore != ''
        THEN CAST(_metrocompositescore AS INT64)
        ELSE CAST(NULL AS INT64)
    END  
    AS _metrocompositescore,

    CASE
        WHEN _compositescore != ''
        THEN CAST(_compositescore AS INT64)
        ELSE CAST(NULL AS INT64)
    END  
    AS _compositescore,

    CASE
        WHEN _statecompositescore != ''
        THEN CAST(_statecompositescore AS INT64)
        ELSE CAST(NULL AS INT64)
    END  
    AS _statecompositescore

FROM 
    `thunder_mysql.db_thunder_bombora_comprehensive_report_data`;

