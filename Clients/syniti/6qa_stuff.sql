## testing 6qa first


SELECT 
    DISTINCT
    account6qastartdate6sense__c,
    true AS _is_6qa,
    name AS _account_name,
    website AS _domain,
    COALESCE(shippingcountry, billingcountry) AS _country
FROM `syniti_salesforce.Account`
WHERE isdeleted = false
AND accountupdatedate6sense__c IS NOT NULL