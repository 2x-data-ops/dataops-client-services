
// For getting unique values in arrays

function onlyUnique(value, index, self) {

    // Find value using index to see if already exist
    return self.indexOf(value) === index;

}

// For reading data in spreadsheet

function readSheetData() {

    // Get the sheet
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("New MQL Export");

    // Get the values (appears row by row)
    var values = sheet.getDataRange().getValues();
    // Logger.log('All rows :');
    // Logger.log(values);

    // Remove the header row
    values.shift();
    // Logger.log('All rows without header row :');
    // Logger.log(values);

    // Return headerless rows
    return values;

}

// For getting all leads tied to an owner

function groupLeadsByOwners() {

    // Get sheet data
    values = readSheetData();

    // Check if sheet is empty 
    if (values.length == 0) {

        // Skip the rest of the function
        return;

    }

    /////////////////////////////////////////////////////////////////
    // PART 1: GET LEADS AS OBJECT
    /////////////////////////////////////////////////////////////////

    // Initialize arrays for storage
    var leads = [];
    var temp_owners = [];

    // Loop each row, place data into objects, and store in arrays
    values.forEach(function(value) {

        // Create lead object
        var lead = {};

        // Add lead properties and values
        lead.name = value[0];
        lead.email = value[1];
        lead.link = value[2];
        lead.company = value[3];
        lead.territory = value[4];
        lead.total_days = value[5];
        lead.owner = value[6];

        // Place lead object into array
        leads.push(lead);

        // Combine owner name and email
        var owner = value[6] + '; ' + value[7];

        // Add combination to array
        temp_owners.push(owner);

    }) 

    // Check arrays
    // Logger.log('Leads :');
    // Logger.log(leads);
    // Logger.log('Owners (Array Form) :');
    // Logger.log(temp_owners);

    /////////////////////////////////////////////////////////////////
    // PART 2: GET OWNERS AS OBJECT
    /////////////////////////////////////////////////////////////////

    // Get unique owners
    var unique_owners = temp_owners.filter(onlyUnique);
    // Logger.log('Unique Owners :');
    // Logger.log(unique_owners);

    // Initialize array for storage
    var owners = [];

    // Convert owners array into object
    unique_owners.forEach(function(value) {

        // Split info combination
        var temp = value.split("; ");

        // Create owner object
        var owner = {};

        // Add owner properties and values
        owner.name = temp[0];
        owner.email = temp[1];

        // Place owner object into array
        owners.push(owner);

    }) 

    // Check object
    // Logger.log('Owners (Object Form) :');
    // Logger.log(owners);

    /////////////////////////////////////////////////////////////////
    // PART 3: PAIR ALL LEADS TO THEIR OWNER
    /////////////////////////////////////////////////////////////////

    // Loop through each owner
    for (let owner of owners) {

        // Initialize array for storage
        matched = [];

        // Loop through each lead
        for (let lead of leads) {

            // Check if owner name matches
            if (lead.owner == owner.name) {

                // Add this lead object if owner detail matches
                matched.push(lead);
                // Logger.log('Inside the matched condition');

            }

        }

        // Set all matched leads for this owner
        owner.leads = matched;
        // Logger.log(matched);

    }

    // Check updated object
    Logger.log(owners);

    // Return updated object
    return owners;

}

// For getting the email template

function getEmailHtml(owner, leads) {

    // Obtain HTML file
    var template = HtmlService.createTemplateFromFile("Index");
  
    // Insert data into template
    template.owner = owner;
    template.leads = leads;

    // Process the data in template
    var template_with_data = template.evaluate().getContent();
  
    // Return template with data
    return template_with_data;

}

// For sending the email

function sendEmail() {

    // Get the owners data
    var owners = groupLeadsByOwners();
    // Logger.log('Owners Data :');
    // Logger.log(owners);

    // Check if there is data
    if (owners == null) {

        // Exit
        return;

    }

    // Check number of owners
    Logger.log('Total Owners : ' + owners.length);

    // Loop through each owner
    for (let owner of owners) {

        // Check owner data
        Logger.log((owners.indexOf(owner) + 1) + ') Current Owner Data :');
        Logger.log(owner);

        // Get owner name
        var name = owner.name;
        Logger.log('Owner Name : ' + name);

        // Get owner email
        var email = owner.email;
        Logger.log('Owner Email : ' + email);

        /////////////////////////////////////////////////////////////////
        // SETTING OF RECIPIENT EMAIL
        /////////////////////////////////////////////////////////////////

        // Initialize holder
        var receiver_email = ''

        // Set the receiver email based on the owner name
        switch (name) {
            case 'Samuel Weiss':
                receiver_email = 'samuel.weiss@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Jill Zeleznik':                                         // Merged email
                receiver_email = 'jill.zeleznik@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Chad Azara':                                            // Merged email
                receiver_email = 'jill.zeleznik@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Andrew Frangos':
                receiver_email = 'andrew.frangos@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Reno Regalbuto':                                        // Merged email
                receiver_email = 'max.halberstadt@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Max Halberstadt':                                       // Merged email
                receiver_email = 'max.halberstadt@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Marc Gueriera':
                receiver_email = 'marc.gueriera@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Connor Castro':
                receiver_email = 'connor.castro@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Sean Smith':
                receiver_email = 'sean.smith@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Benjamin Trujillo':
                receiver_email = 'benjamin.trujillo@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            case 'Chris Mautz':
                receiver_email = 'chris.mautz@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
                break;
            default:
                receiver_email = 'max.halberstadt@pcsretirement.com';
                Logger.log('Receiver Email : ' + receiver_email);
        }

        /////////////////////////////////////////////////////////////////
        // SETTING OF RECIPIENT LIST
        /////////////////////////////////////////////////////////////////

        // Initialize email list
        var email_list = [];

        // Set the email list based on Max Halberstadt
        if (receiver_email == 'max.halberstadt@pcsretirement.com') {
            email_list = [
                receiver_email,
                "kimberlie.wee@2x.marketing",
                "hwachian.seng@2x.marketing",
                "abby.tan@2x.marketing",
                "joel.wong@2x.marketing",
                "nazzatul.nazziri@2x.marketing"
            ];
            Logger.log('Max List = Max is the receiver, dont need to add him.');
        }
        else {
            email_list = [
                receiver_email,
                "max.halberstadt@pcsretirement.com",
                "kimberlie.wee@2x.marketing",
                "hwachian.seng@2x.marketing",
                "abby.tan@2x.marketing",
                "joel.wong@2x.marketing",
                "nazzatul.nazziri@2x.marketing"
            ];
            Logger.log('Non Max List = Max is not the receiver, need to add him.');
        }

        // Get owner's leads
        var leads = owner.leads;
        Logger.log('Owner Leads :');
        Logger.log(leads);

        // Check number of leads
        Logger.log('Total Leads : ' + leads.length);

        // Get email template
        var template = getEmailHtml(name, leads);
        // Logger.log(template);

        // Send email using Gmail
        GmailApp.sendEmail(

            recipient = email_list,
            subject = "REMINDER: Action Required on Leads", 
            body = 'If you are seeing this text, please view the email on a browser for proper rendering of the email.', 
            options = {
              'name': '2X Marketing',
              'htmlBody': template
            }

        );

    }

}

// For sending the test email

function sendTestEmail() {

    // Get the owners data
    var owners = groupLeadsByOwners();
    // Logger.log('Owners Data :');
    // Logger.log(owners);

    // Check if there is data
    if (owners == null) {

        // Exit
        return;

    }

    // Check number of owners
    Logger.log('Total Owners : ' + owners.length);

    // Loop through each owner
    for (let owner of owners) {

        // Check owner data
        Logger.log((owners.indexOf(owner) + 1) + ') Current Owner Data :');
        Logger.log(owner);

        // Get owner name
        var name = owner.name;
        Logger.log('Owner Name : ' + name);

        // Get owner email
        var email = owner.email;
        Logger.log('Owner Email : ' + email);

        // Get owner's leads
        var leads = owner.leads;
        Logger.log('Owner Leads :');
        Logger.log(leads);

        // Check number of leads
        Logger.log('Total Leads : ' + leads.length);

        // Get email template
        var template = getEmailHtml(name, leads);
        // Logger.log(template);

        // Send email using Gmail
        GmailApp.sendEmail(

            recipient = [
              "joel.wong@2x.marketing"
            ],
            subject = "REMINDER: Action Required on Leads", 
            body = 'If you are seeing this text, please view the email on a browser for proper rendering of the email.', 
            options = {
              'name': '2X Marketing',
              'htmlBody': template
            }

        );

        // Stop at first owner
        // To avoid sending too many emails
        // break;

    }

}

