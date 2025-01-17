CREATE OR REPLACE TABLE `sdi.db_opportunity_stage_history` AS
WITH new_sales_opps AS (

    SELECT
        *
    FROM (

        SELECT 

            CAST(dealid AS STRING) AS _opportunityID,
            property_dealname.value AS _opportunityName,
            property_hs_date_entered_104827410.value AS stage_0_date,
            property_hs_date_entered_104827411.value	AS stage_1_date,
            property_hs_date_entered_104827412.value AS stage_2_date,
            property_hs_date_entered_104827413.value AS stage_3_date,
            property_hs_date_entered_104827414.value AS stage_4_date,
            property_hs_date_entered_104827415.value	AS stage_5_date,
            property_hs_date_entered_104827416.value AS stage_6_date,
            property_hs_date_entered_105783115.value AS stage_7_date

        FROM 
            `x-marketing.sdi_hubspot.deals`
        WHERE 
            property_pipeline.value = '51522144'

    )
    CROSS JOIN (

        SELECT DISTINCT 

            pipelineid AS _pipelineID,
            label AS _pipelineName,
            stages.value.stageid,
            stages.value.label AS _stage,
            stages.value.probability AS _probability

        FROM 
            `x-marketing.sdi_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages
        WHERE 
            pipelineid = '51522144'

    )

), 

unpivot_new_sales_opps AS (

    SELECT

        _opportunityID,
        _opportunityName,
        _pipelineID,
        _pipelineName,
        _stage,
        _probability,

        CASE
            WHEN _stage = 'Closed lost' THEN 6
            WHEN _stage = 'Discovery' THEN 1
            WHEN _stage = 'Presentation' THEN 2
            WHEN _stage = 'Proposal' THEN 3
            WHEN _stage = 'Awaiting Decision' THEN 4
            WHEN _stage = 'Agreement' THEN 5
            WHEN _stage = 'Closed won' THEN 6 
            WHEN _stage = 'On Hold' THEN 7 
        END 
        AS _rank,

        CASE
            WHEN _stage = 'Discovery' THEN stage_0_date
            WHEN _stage = 'Presentation' THEN stage_1_date
            WHEN _stage = 'Proposal' THEN stage_2_date
            WHEN _stage = 'Awaiting Decision' THEN stage_3_date
            WHEN _stage = 'Agreement' THEN stage_4_date
            WHEN _stage = 'Closed won' THEN stage_5_date 
            WHEN _stage = 'Closed lost' THEN stage_6_date
            WHEN _stage = 'On Hold' THEN stage_7_date
        END 
        AS _timestamp

    FROM 
        new_sales_opps

),
expansion_ops AS (
    SELECT
        *
    FROM (

        SELECT 

            CAST(dealid AS STRING) AS _opportunityID,
            property_dealname.value AS _opportunityName,
            property_hs_date_entered_163922041.value AS stage_0_date,
            property_hs_date_entered_163922042.value	AS stage_1_date,
            property_hs_date_entered_163922043.value AS stage_2_date,
            property_hs_date_entered_163922044.value AS stage_3_date,

        FROM 
            `x-marketing.sdi_hubspot.deals`
        WHERE 
            property_pipeline.value = '88235347'

    )
    CROSS JOIN (

        SELECT DISTINCT 

            pipelineid AS _pipelineID,
            label AS _pipelineName,
            stages.value.stageid,
            stages.value.label AS _stage,
            stages.value.probability AS _probability

        FROM 
            `x-marketing.sdi_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages
        WHERE 
            pipelineid = '88235347'

    )

) ,
unpivot_expansion_opps AS (
    SELECT

        _opportunityID,
        _opportunityName,
        _pipelineID,
        _pipelineName,
        _stage,
        _probability,

        CASE
            WHEN _stage = 'Closed lost' THEN 6
            WHEN _stage = 'Opportunity Identified' THEN 1
            WHEN _stage = 'Qualified to buy' THEN 2
            WHEN _stage = 'Presentation scheduled' THEN 3
            WHEN _stage = 'Decision Maker Bought-In' THEN 4
            WHEN _stage = 'Closed won' THEN 6 
            WHEN _stage = 'On Hold' THEN 7 
        END 
        AS _rank,

        CASE
            --WHEN _stage = 'Closed lost' THEN stage_5_date
            WHEN _stage = 'Opportunity Identified' THEN stage_0_date
            WHEN _stage = 'Qualified to buy' THEN stage_1_date
            WHEN _stage = 'Presentation scheduled' THEN stage_2_date
            WHEN _stage = 'Agreed' THEN stage_3_date
            --WHEN _stage = 'Closed won' THEN stage_6_date 
            --WHEN _stage = 'On Hold' THEN stage_7_date
        END 
        AS _timestamp

    FROM 
        expansion_ops
), 
renewal_ops AS (
    SELECT
        *
    FROM (

        SELECT 

            CAST(dealid AS STRING) AS _opportunityID,
            property_dealname.value AS _opportunityName,
            property_hs_date_entered_104967982.value AS stage_0_date,
            property_hs_date_entered_104967983.value	AS stage_1_date,
            property_hs_date_entered_104967984.value AS stage_2_date,
            --property_hs_date_entered_104967988.value AS stage_3_date,

        FROM 
            `x-marketing.sdi_hubspot.deals`
        WHERE 
            property_pipeline.value = '51616802'

    )
    CROSS JOIN (

        SELECT DISTINCT 

            pipelineid AS _pipelineID,
            label AS _pipelineName,
            stages.value.stageid,
            stages.value.label AS _stage,
            stages.value.probability AS _probability

        FROM 
            `x-marketing.sdi_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages
        WHERE 
            pipelineid = '51616802'

    )
) , 
unpivot_renewal_opps AS ( 
    SELECT

        _opportunityID,
        _opportunityName,
        _pipelineID,
        _pipelineName,
        _stage,
        _probability,

        CASE
            WHEN _stage = 'Closed lost' THEN 6
            WHEN _stage = 'Renewal Discussion' THEN 1
            WHEN _stage = 'Scoping Updates' THEN 2
            WHEN _stage = 'Proposal' THEN 3
            WHEN _stage = 'Closed won' THEN 6 
            WHEN _stage = 'On Hold' THEN 7 
        END 
        AS _rank,

        CASE
            --WHEN _stage = 'Closed lost' THEN stage_5_date
            WHEN _stage = 'Renewal Discussion' THEN stage_0_date
            WHEN _stage = 'Scoping Updates' THEN stage_1_date
            WHEN _stage = 'Proposal' THEN stage_2_date
            --WHEN _stage = 'Closed won' THEN stage_6_date 
            --WHEN _stage = 'On Hold' THEN stage_7_date
        END 
        AS _timestamp

    FROM 
        renewal_ops
), co_op_opp AS (
    SELECT
        *
    FROM (

        SELECT 

            CAST(dealid AS STRING) AS _opportunityID,
            property_dealname.value AS _opportunityName,
            property_hs_date_entered_81546511.value AS stage_0_date,
            property_hs_date_entered_81546512.value	AS stage_1_date,
            property_hs_date_entered_81546514.value AS stage_2_date,
            property_hs_date_entered_81546515.value AS stage_3_date,
            property_hs_date_entered_143996188.value AS stage_4_date,
            property_hs_date_entered_81546516.value AS stage_5_date,
            property_hs_date_entered_81546517.value AS stage_6_date,


        FROM 
            `x-marketing.sdi_hubspot.deals`
        WHERE 
            property_pipeline.value = '37943328'

    )
    CROSS JOIN (

        SELECT DISTINCT 

            pipelineid AS _pipelineID,
            label AS _pipelineName,
            stages.value.stageid,
            stages.value.label AS _stage,
            stages.value.probability AS _probability

        FROM 
            `x-marketing.sdi_hubspot.deal_pipelines`, 
            UNNEST(stages) AS stages
        WHERE 
            pipelineid = '37943328'

    )
) , 
unpivot_co_op_opps AS (  
    SELECT

        _opportunityID,
        _opportunityName,
        _pipelineID,
        _pipelineName,
        _stage,
        _probability,

        CASE
            WHEN _stage = 'Closed lost' THEN 6
            WHEN _stage = 'Appointment scheduled' THEN 1
            WHEN _stage = 'Qualified to buy' THEN 2
            WHEN _stage = 'Decision Maker Bought-In' THEN 3
            WHEN _stage = 'Contract sent' THEN 4
            WHEN _stage = 'Contract Signed' THEN 5
            WHEN _stage = 'Closed won' THEN 6 
            WHEN _stage = 'On Hold' THEN 7 
        END 
        AS _rank,

        CASE
            WHEN _stage = 'Closed lost' THEN stage_5_date
            WHEN _stage = 'Appointment scheduled' THEN stage_0_date
            WHEN _stage = 'Qualified to buy' THEN stage_1_date
            WHEN _stage = 'Decision Maker Bought-In' THEN stage_2_date
            WHEN _stage = 'Contract sent' THEN stage_3_date
            WHEN _stage = 'Contract Signed' THEN stage_4_date
            WHEN _stage = 'Closed won' THEN stage_6_date 
            --WHEN _stage = 'On Hold' THEN stage_7_date
        END 
        AS _timestamp

    FROM 
        co_op_opp
    ) , 
combined_data AS (

    SELECT * FROM unpivot_new_sales_opps WHERE _timestamp IS NOT NULL 
    UNION ALL 
    SELECT * FROM unpivot_co_op_opps WHERE _timestamp IS NOT NULL
    UNION ALL 
    SELECT * FROM unpivot_renewal_opps WHERE _timestamp IS NOT NULL
    UNION ALL 
    SELECT * FROM unpivot_expansion_opps WHERE _timestamp IS NOT NULL
    

)

SELECT * FROM combined_data ORDER BY _opportunityID DESC, _probability DESC;
