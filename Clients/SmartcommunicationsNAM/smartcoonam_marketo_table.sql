TRUNCATE TABLE `x-marketing.smartcommnam.activities_delete_lead`; 
INSERT INTO `x-marketing.smartcommnam.activities_delete_lead`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_delete_lead` ;

TRUNCATE  TABLE `x-marketing.smartcommnam.activities_click_email`; 
INSERT INTO `x-marketing.smartcommnam.activities_click_email`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_click_email` ;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_email_bounced`; 
INSERT INTO `x-marketing.smartcommnam.activities_email_bounced`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_email_bounced` ;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_email_bounced_soft`;
INSERT INTO `x-marketing.smartcommnam.activities_email_bounced_soft` 
SELECT * FROM `x-marketing.smartcomm_marketo.activities_email_bounced_soft` ;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_fill_out_form`; 
INSERT INTO  `x-marketing.smartcommnam.activities_fill_out_form`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_fill_out_form`;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_open_email`; 
INSERT INTO `x-marketing.smartcommnam.activities_open_email`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_open_email`;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_send_email`; 
INSERT INTO `x-marketing.smartcommnam.activities_send_email`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_send_email` ;


TRUNCATE TABLE `x-marketing.smartcommnam.activities_unsubscribe_email`; 
INSERT INTO `x-marketing.smartcommnam.activities_unsubscribe_email`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_unsubscribe_email` ;

TRUNCATE TABLE `x-marketing.smartcommnam.activities_visit_webpage`;
INSERT INTO  `x-marketing.smartcommnam.activities_visit_webpage`
SELECT * FROM `x-marketing.smartcomm_marketo.activities_visit_webpage` ;

TRUNCATE TABLE `x-marketing.smartcommnam.campaigns`;
INSERT INTO `x-marketing.smartcommnam.campaigns`
SELECT * FROM `x-marketing.smartcomm_marketo.campaigns` ;

TRUNCATE TABLE `x-marketing.smartcommnam.leads`;
INSERT INTO`x-marketing.smartcommnam.leads`
SELECT * FROM `x-marketing.smartcomm_marketo.leads` ;

TRUNCATE TABLE `x-marketing.smartcommnam.programs`; 
INSERT INTO `x-marketing.smartcommnam.programs`
SELECT * FROM `x-marketing.smartcomm_marketo.programs` ;

TRUNCATE TABLE `x-marketing.smartcommnam.lists`;
INSERT INTO `x-marketing.smartcommnam.lists`
SELECT * FROM `x-marketing.smartcomm_marketo.lists` ;