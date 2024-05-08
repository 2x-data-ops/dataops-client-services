
-- latest iteration for demand council dashboard
CREATE OR REPLACE TABLE `x-marketing.syniti.demand_council` 
-- PARTITION BY DATE(_extractDate)
CLUSTER BY _opportunityid
OPTIONS(description="Clustered by _opportunityid") AS


-- main data source, doing filtering for max date here
WITH alldata AS (
  SELECT * FROM `syniti_mysql.syniti_db_demand_council`
  WHERE _extractDate = (SELECT MAX(_extractDate) FROM `syniti_mysql.syniti_db_demand_council`)
),
-- figuring out how to distinct the oppoortunityid to find duplicate
TotalCount AS (
    SELECT
        _opportunityid,
        _campaignID,
        _campaignStatus,
        _extractDate AS datedate,
        _fiscalPeriod,
        _opportunityAmountConverted,
        _opportunityCreatedDate,
        _projectName,
        COUNT(*) AS count_per_opportunityid
    FROM
    alldata
    GROUP BY ALL
)
-- based on distinct opps found above, divide them with the opportunityamountconverted
-- new opps amount is _avg_opp_amount
SELECT
    alldata.*,
    COALESCE(TotalCount.count_per_opportunityid, 0) AS count_per_opportunityid,
    (CAST(alldata._opportunityamountconverted AS FLOAT64) / NULLIF(COALESCE(TotalCount.count_per_opportunityid, 0), 0)) AS _avg_opp_amount
FROM
    alldata
LEFT JOIN
    TotalCount ON alldata._opportunityid = TotalCount._opportunityid
    AND alldata._campaignID = TotalCount._campaignID
    AND alldata._campaignStatus = TotalCount._campaignStatus
    AND alldata._extractDate = TotalCount.datedate
    AND alldata._fiscalPeriod = TotalCount._fiscalPeriod
    AND alldata._opportunityAmountConverted = TotalCount._opportunityAmountConverted
    AND alldata._opportunityCreatedDate = TotalCount._opportunityCreatedDate
    AND alldata._projectName = TotalCount._projectName

