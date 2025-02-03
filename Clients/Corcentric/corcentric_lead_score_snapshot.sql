
-- Get snapshot of marketo lead scores

INSERT INTO `x-marketing.corcentric.marketo_lead_score_snapshot` 

SELECT 
    mkt.id, 
    mkt.leadscore, 
    mkt.relativescore, 
    mkt.behavior_score__c, 
    mkt.demographic_score__c, 
    CURRENT_DATE() AS extract_date
FROM 
    `x-marketing.corcentric_marketo.leads` mkt
;

