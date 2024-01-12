--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Web Performance Script --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

# Web Metrics
/* 
    Get all web visits data and tying with ipaddress to get company's domain of a visitor.
    Data stored in this table is log based.
*/

TRUNCATE TABLE `x-marketing.sbi.db_web_engagements_log`;

INSERT INTO `x-marketing.sbi.db_web_engagements_log` (
    _recordingurl,
    _recordingid,
    _visitorid,
    _viewid,
    _userstatus,
    _page,
    _pagegroup,
    _fullurl,
    _cleanpage, 
    _nextpage,
    _totalsessionviews,
    -- Engagement time is in the seconds unit
    _engagementtime,
    _timespent,
    _timestamp,
    -- Stage is set here
    _stage, 
    _goalcompletion,
    _utmsource,
    _utmcampaign,
    _utmmedium,
    _utmcontent,
    _utmterm,
    _ipaddr,
    _domain,
    _name,
    _city,
    _region,
    _country
)
WITH
    mouseflow AS (
        SELECT 
            DISTINCT 
            CASE
                WHEN _recordingurl LIKE "%api-%"
                THEN REGEXP_REPLACE(_recordingurl, r'api-', '')
                WHEN _recordingurl LIKE "%app%"
                THEN REGEXP_REPLACE(_recordingurl, r'app', 'us')
                ELSE _recordingurl
            END AS _recordingurl,
            _recordingid,
            _visitorid,
            _viewid,
            CAST(NULL AS STRING) AS _userstatus,
            _page,
            CAST(NULL AS STRING) AS _pagegroup,
            CASE
                -- Relabel the root page
                WHEN _page = '/' THEN 'https://www.sbi.com/'
                ELSE _fullurl
            END AS _fullurl,
            _cleanpage, 
            _nextpage,
            CAST(_totalsessionviews AS INT) AS _totalsessionviews,
            -- Engagement time is in the seconds unit
            CAST(_engagementtime AS DECIMAL) AS _engagementtime,
            CAST(_timespent AS FLOAT64) AS _timespent,
            PARSE_TIMESTAMP('%F %T', _starttime) AS _timestamp,
            -- Stage is set here
            'Awareness' AS _stage, 
            CAST(NULL AS BOOL) AS _goalcompletion,
            _source AS _utmsource,
            _campaign AS _utmcampaign,
            _medium AS _utmmedium,
            _content AS _utmcontent,
            _term AS _utmterm,
            _ipaddr,
            -- supp._domain AS _domain,
            -- _name,
            -- _city,
            -- _region,
            _country
        FROM 
            `x-marketing.sbi_mysql.mouseflow_pageviews` main
        
    ),
    ip_company AS (
        SELECT 
            DISTINCT _website AS _domain, 
            _name,
            _ipaddr,
            _city,
            _region,
            _country 
        FROM 
            `webtrack_ipcompany.webtrack_ipcompany_6sense`
        WHERE
            _isisp = 0
    ),
    mapped AS (
        SELECT 
            *EXCEPT(_country),
            COALESCE(mouseflow._country, ip_company._country) AS _country
        FROM
            mouseflow
        LEFT JOIN
            ip_company USING(_ipaddr)
    )
SELECT
    DISTINCT 
    _recordingurl,
    _recordingid,
    _visitorid,
    _viewid,
    _userstatus,
    _page,
    _pagegroup,
    _fullurl,
    _cleanpage, 
    _nextpage,
    _totalsessionviews,
    -- Engagement time is in the seconds unit
    _engagementtime,
    _timespent,
    _timestamp,
    -- Stage is set here
    _stage, 
    _goalcompletion,
    COALESCE(_utmsource, REGEXP_EXTRACT(_fullurl, r'[?&]utm_source=([^&]+)') )  AS _utmsource,
    COALESCE(_utmcampaign, REGEXP_EXTRACT(_fullurl, r'[?&]utm_campaign=([^&]+)') ) AS _utmcampaign,
    COALESCE(_utmmedium, REGEXP_EXTRACT(_fullurl, r'[?&]utm_medium=([^&]+)') ) AS _utmmedium,
    COALESCE(_utmcontent, REGEXP_EXTRACT(_fullurl, r'[?&]utm_content=([^&]+)') ) AS _utmcontent,
    _utmterm,
    _ipaddr,
    _domain,
    _name,
    _city,
    _region,
    _country
FROM 
    mapped
;




# Label new and returning users 
/*  
    Those whose visitor id appears several times are returning users.
    Those whose visitor id appears once are new users.
*/
UPDATE `x-marketing.sbi.db_web_engagements_log` origin
SET origin._userstatus = scenario._userstatus
FROM (
    WITH visitorid_count AS (
        SELECT
            _visitorid,
            COUNT(*) AS _visitorid_count
        FROM `x-marketing.sbi.db_web_engagements_log`
        GROUP BY 1
    ),
    label_user_status AS (
        SELECT
            _visitorid,
            CASE 
                WHEN _visitorid_count = 1 THEN 'New'
                WHEN _visitorid_count > 1 THEN 'Returning'
            END AS _userstatus
        FROM visitorid_count
    )
    SELECT * FROM label_user_status
) scenario
WHERE origin._visitorid = scenario._visitorid;

# Label page group
/*  
    Categorize web page using the subpage name in the URL
*/
UPDATE `x-marketing.sbi.db_web_engagements_log` origin
SET origin._pagegroup = scenario._pagegroup
FROM (
    SELECT DISTINCT
        _fullurl,
        CASE
            WHEN _fullurl LIKE '%data-discovery%' THEN 'Data Discovery'
            WHEN _fullurl LIKE '%aip-mip%' THEN 'AIP MIP'
            WHEN _fullurl LIKE '%data-definitions%' THEN 'Data Definitions'
            WHEN _fullurl LIKE '%data-classification%' THEN 'Data Classification'
            WHEN _fullurl LIKE '%classification-checklist%' THEN 'Classification Checklist'
            WHEN _fullurl LIKE '%core-capabilities%' THEN 'Core Capabilities'
            WHEN _fullurl LIKE '%customer-services%' THEN 'Customer Services'
            WHEN _fullurl LIKE '%customer-stories%' THEN 'Customer Stories'
            WHEN _fullurl LIKE '%dark-data%' THEN 'Dark Data'
            WHEN _fullurl LIKE '%data-breach%' THEN 'Data Breach'
            WHEN _fullurl LIKE '%data-footprint-reduction%' THEN 'Data Footprint Reduction'
            WHEN _fullurl LIKE '%data-lifecycle-management%' THEN 'Data Lifecycle Management'
            WHEN _fullurl LIKE '%data-loss-prevention%' THEN 'Data Loss Prevention'
            WHEN _fullurl LIKE '%data-remediation%' THEN 'Data Remediation'
            WHEN _fullurl LIKE '%data-retention%' THEN 'Data Retention'
            WHEN _fullurl LIKE '%data-risk-assessment%' THEN 'Data Risk Assessment'
            WHEN _fullurl LIKE '%de-identification-tonic%' THEN 'De-Identification Tonic'
            WHEN _fullurl LIKE '%expanding-it-frontier%' THEN 'Expanding IT Frontier'
            WHEN _fullurl LIKE '%faqs-for-console-admins%' THEN 'FAQs For Console Admins'
            WHEN _fullurl LIKE '%forrester-zero-trust%' THEN 'Forrester Zero Trust'
            WHEN _fullurl LIKE '%glba-safeguards%' THEN 'GLBA Safeguards'
            WHEN _fullurl LIKE '%governance-suite%' THEN 'Governance Suite'
            WHEN _fullurl LIKE '%higher-ed%' THEN 'Higher Ed'
            WHEN _fullurl LIKE '%major-player-idc%' THEN 'Major Player Idc'
            WHEN _fullurl LIKE '%nist-privacy-framework%' THEN 'NIST Privacy Framework'
            WHEN _fullurl LIKE '%partner-program%' THEN 'Partner Program'
            WHEN _fullurl LIKE '%platform-overview%' THEN 'Platform Overview'
            WHEN _fullurl LIKE '%privacy-compliance-hardships%' THEN 'Privacy Compliance Hardships'
            WHEN _fullurl LIKE '%privacy-grade%' THEN 'Privacy Grade'
            WHEN _fullurl LIKE '%customer-newsletter%' THEN 'Customer Newsletter'
            WHEN _fullurl LIKE '%ransomware-attacks%' THEN 'Ransomware Attacks'
            WHEN _fullurl LIKE '%request-an-evaluation%' THEN 'Request An Evaluation'
            WHEN _fullurl LIKE '%sd-finder%' THEN 'SD Finder'
            WHEN _fullurl LIKE '%sd-platform%' THEN 'SD Platform'
            WHEN _fullurl LIKE '%security-use-cases%' THEN 'Security Use Cases'
            WHEN _fullurl LIKE '%sensitive-data-discovery%' THEN 'Sensitive Data Discovery'
            WHEN _fullurl LIKE '%sensitive-data-governance-framework%' THEN 'Sensitive Data Governance Framework'
            WHEN _fullurl LIKE '%sbi-step-one%' THEN 'sbi Step One'
            WHEN _fullurl LIKE '%state-local%' THEN 'State Local'
            WHEN _fullurl LIKE '%wbn-seclore-sbi%' THEN 'WBN Seclore sbi'
            WHEN _fullurl LIKE '%wp-nist%' THEN 'WP NIST'
            WHEN _fullurl LIKE '%blog%' THEN 'Blog'
            WHEN _fullurl LIKE '%ccpa%' THEN 'CCPA'
            WHEN _fullurl LIKE '%company%' THEN 'Company'
            WHEN _fullurl LIKE '%comply%' THEN 'Comply'
            WHEN _fullurl LIKE '%contact%' THEN 'Contact'
            WHEN _fullurl LIKE '%cyberhaven%' THEN 'Cyberhaven'
            WHEN _fullurl LIKE '%discover%' THEN 'Discover'
            WHEN _fullurl LIKE '%firewall%' THEN 'Firewall'
            WHEN _fullurl LIKE '%licensing%' THEN 'Licensing'
            WHEN _fullurl LIKE '%platforms%' THEN 'Platforms'
            WHEN _fullurl LIKE '%products%' THEN 'Products'
            WHEN _fullurl LIKE '%resources%' THEN 'Resources'
            WHEN _fullurl LIKE '%solutions%' THEN 'Solutions'
            WHEN _fullurl LIKE '%data%' THEN 'Other Data Related'
            ELSE 'Non-Grouped'
        END AS _pagegroup
    FROM `x-marketing.sbi.db_web_engagements_log`
) scenario
WHERE origin._fullurl = scenario._fullurl;

# Label goal completion
/*  
    Goal completion depends on whether a particular page has been reached by the user
*/
UPDATE `x-marketing.sbi.db_web_engagements_log` 
SET _goalcompletion = true
WHERE _pagegroup = 'Contact';


