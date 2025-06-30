import os
from flask import Flask, render_template, request, flash, redirect, url_for, send_file
import pandas as pd
import re
from io import BytesIO
from dateutil.parser import parse
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = 'Uploads'
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls'}

corrected_file = BytesIO()
output_format = 'xlsx'  # Default output format
original_filename = ''  # Store original filename globally

# Expected headers for mapping
REQUIRED_HEADERS = [
    'LAST_NAME', 'FIRST_NAME', 'DATE_OF_BIRTH', 'RECORD_TYPE', 'E_APP_NUMBER',
    'EXCHANGE_ID', 'CLIENT_APP_ID', 'MEMBER_COUNT', 'GROUP_NUMBER',
    'ACCOUNT_NUMBER', 'POLICY_STATUS', 'PRODUCT_TYPE', 'PLAN_NAME', 'SOURCE', 'APTC',
    'RENEWAL_INDICATOR', 'PAID_TO_DATE', 'TERMED', 'PRODUCER_NAME',
    'NINE_DIGIT_PRODUCER_NUMBER', 'CLIENT_ADDRESS_1', 'CLIENT_ADDRESS_2',
    'CITY', 'STATE', 'ZIP_CODE', "CLIENT'S_PRIMARY_PHONE", 'EMAIL',
    'FILE_RECORD_BIRTH_DATE'
]

# Header mapping based on SIMweeklyFilesName_ColumnHeaders.xlsx
EXTENDED_HEADER_MAPPING = {
    'LAST_NAME': [
        'Last Name', 'Insured Last Name', 'Subscriber Last Name', 'Member Last Name',
        'Primary Last Name', 'Member Name', 'Client Name', 'Subscriber Last Name',
        'Member_First_Name', 'Mem Last Name'
    ],
    'FIRST_NAME': [
        'First Name', 'Insured First Name', 'Subscriber First Name', 'Member First Name',
        'Primary First Name', 'Agent First Name', 'Broker_First_Name', 'Member_First_Name',
        'Mem First Name', 'Subscriber First Name'
    ],
    'DATE_OF_BIRTH': [
        'Date of Birth', 'Member Date Of Birth', 'Subscriber Date of Birth', 'Birth Date',
        'Member DOB', 'dob', 'Date of birth', 'Member Date of Birth'
    ],
    'RECORD_TYPE': [
        'Record Type', 'Enrollment Type', 'Coverage Status', 'AR Policy Type'
    ],
    'E_APP_NUMBER': [
        'E-App Number', 'Application Number', 'App_ID_and_Origin', 'Application Id',
        'FFM Application ID', 'IFP - FFM APP ID', 'Application_ID'
    ],
    'EXCHANGE_ID': [
        'Exchange ID', 'Exchange Subscriber ID', 'HIX_ID', 'Subscriber_ID',
        'Exchange_Member_ID', 'Member Number ACA Issuer Id', 'EMB_Issuer_Member_ID',
        'UMV_Issuer_Member_ID'
    ],
    'CLIENT_APP_ID': [
        'Client App ID', 'Policy Number', 'Contract ID', 'Subscriber ID (Detail Case #)',
        'Customer Number (Case ID)', 'Member ID', 'UMV_Issuer_Policy_ID', 'Member FB_UID'
    ],
    'MEMBER_COUNT': [
        'Member Count', 'Number of Members', 'Group Size', 'Covered Lives',
        'Contract Member Count', 'Lives'
    ],
    'GROUP_NUMBER': [
        'Group Number', 'Employer Group Name'
    ],
    'ACCOUNT_NUMBER': [
        'Account Number', 'Member Number', 'MRN', 'Subscriber Number'
    ],
    'POLICY_STATUS': [
        'Status', 'Policy Status', 'Contract Status', 'Member Status', 'Plan Status',
        'Detailed_Finance_Status', 'Agent Status', 'APO Status', 'Account creation status'
    ],
    'PRODUCT_TYPE': [
        'Product Type', 'Product', 'Product Line', 'IFP - upset - Plan Type', 'Product_Type'
    ],
    'PLAN_NAME': [
        'Plan Name', 'Case_Plan_Marketing_Name', 'Plan Description', 'Plan Code',
        'Plan', 'Renewal Plan', 'Exchange_Plan_ID'
    ],
    'SOURCE': [
        'Source', 'On/Off Exchange', 'Exchange', 'State_Exchange', 'Exchange Indicator',
        'On exchange'
    ],
    'APTC': [
        'APTC', 'APTC Flag', 'APTC Amount', 'APTC subsidy', 'Subsidy', 'APTC_Amount'
    ],
    'RENEWAL_INDICATOR': [
        'Renewal Indicator', 'New Business', 'IFP - Renewal', 'Renewal enrollment type',
        'Reinstatement Indicator', 'New_Renew_Status'
    ],
    'PAID_TO_DATE': [
        'Paid To Date', 'Paid Through Date', 'Finance_Paid_Through_Date', 'Paid_Through_Date'
    ],
    'TERMED': [
        'Termed', 'Policy Term Date', 'Coverage Expiration Date', 'Cancellation Date',
        'End_Date', 'Termination Date', 'Scheduled_Term_Date', 'PolicyTermDate',
        'Coverage end date', 'Broker Term Date'
    ],
    'PRODUCER_NAME': [
        'Producer Name', 'Broker Name', 'Writing Agent', 'Agent Full Name',
        'Broker_First_Name', 'Broker_Last_Name', 'Agent Name', 'Payable Agent',
        'Agent Last Name', 'Writing agent', 'Broker_Name'
    ],
    'NINE_DIGIT_PRODUCER_NUMBER': [
        'Nine Digit Producer Number', 'Broker NPN', 'Agent NPN', 'Writing Agent NPN',
        'Broker_NPN', 'NPN', 'agentNpn', 'Writing TIN', 'Paid TIN', 'Parent TIN',
        'Reporting TIN', 'IRS Number', 'Agent_NPN'
    ],
    'CLIENT_ADDRESS_1': [
        'Client Address 1', 'Address1', 'memberAddress1', 'Mailing address',
        'Best Available Address'
    ],
    'CLIENT_ADDRESS_2': [
        'Client Address 2', 'Address2', 'memberAddress2', 'Best Available Address Line 2'
    ],
    'CITY': [
        'City', 'memberCity', 'Best Available City Name', 'Address3'
    ],
    'STATE': [
        'State', 'memberState', 'Best Available State Name', 'App State'
    ],
    'ZIP_CODE': [
        'Zip Code', 'Zip', 'memberZip', 'Best Available Postal Code'
    ],
    "CLIENT'S_PRIMARY_PHONE": [
        "Client's Primary Phone", 'Member Phone Number', 'Customer Phone Number',
        'Phone Number', 'Member Phone', 'Member_Bussiness_Phone',
        'Subscriber Mobile Phone Number', 'Subscriber Correspondence Phone Number'
    ],
    'EMAIL': [
        'Email', 'Member Email', 'Customer Email Address', 'Member Email Address',
        'agentEmail', 'memberEmail'
    ],
    'FILE_RECORD_BIRTH_DATE': []
}

# Expected headers per file based on SIMweeklyFilesName_ColumnHeaders.xlsx
EXPECTED_HEADERS_BY_FILE = {
    'BCBS_TX_Weekly_AHM': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'BCBS_TX_Weekly_MHA': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'BCBS_TX_Weekly_NPA': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'BCBS_TX_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_Ambetter_Weekly_Feed': [
        'Broker Name', 'Broker NPN', 'Policy Number', 'Plan Name', 'Insured First Name',
        'Insured Last Name', 'Broker Effective Date', 'Broker Term Date',
        'Policy Effective Date', 'Policy Term Date', 'Paid Through Date',
        'Member Responsibility', 'Monthly Premium Amount', 'County', 'State',
        'On/Off Exchange', 'Exchange Subscriber ID', 'Member Phone Number',
        'Member Email', 'Member Date Of Birth', 'Autopay', 'Eligible for Commission',
        'Number of Members', 'Payable Agent', 'AR Policy Type', 'ICHRA Indicator',
        'Employer Group Name', 'Employer Start Date', 'Employer Subsidy Amount',
        'Employer Subsidy Type'
    ],
    'Ambetter_Weekly_Coverage_Effective_Date_Patch': [
        'EMB_Issuer_Member_ID', 'UMV_Issuer_Member_ID', 'UMV_Issuer_Policy_ID',
        'Exchange_Member_ID', 'App_ID_and_Origin', 'Effective_Date', 'End_Date',
        'Finance_Paid_Through_Date', 'Binder_Payment', 'New_Renew_Status', 'State',
        'State_Exchange', 'Exchange_Plan_ID', 'Case_Plan_Marketing_Name',
        'Metal_Level', 'Product_Line', 'Broker_NPN', 'Broker_Name',
        'Broker_Agency_ID', 'Broker_Agency_Name', 'Detailed_Finance_Status'
    ],
    'SIM_Ambetter_Weekly_Feed_Delinquent': [
        'Broker Name', 'Broker NPN', 'Policy Number', 'Plan Name', 'Insured First Name',
        'Insured Last Name', 'Broker Effective Date', 'Broker Term Date',
        'Policy Effective Date', 'Policy Term Date', 'Paid Through Date',
        'Member Responsibility', 'Monthly Premium Amount', 'County', 'State',
        'On/Off Exchange', 'Exchange Subscriber ID', 'Member Phone Number',
        'Member Email', 'Member Date Of Birth', 'Autopay', 'Eligible for Commission',
        'Number of Members', 'Payable Agent', 'AR Policy Type', 'ICHRA Indicator',
        'Employer Group Name', 'Employer Start Date', 'Employer Subsidy Amount',
        'Employer Subsidy Type'
    ],
    'SIM_Ambetter_Weekly_Feed_Unpaid': [
        'Broker Name', 'Broker NPN', 'Policy Number', 'Plan Name', 'Insured First Name',
        'Insured Last Name', 'Broker Effective Date', 'Broker Term Date',
        'Policy Effective Date', 'Policy Term Date', 'Paid Through Date',
        'Member Responsibility', 'Monthly Premium Amount', 'County', 'State',
        'On/Off Exchange', 'Exchange Subscriber ID', 'Member Phone Number',
        'Member Email', 'Member Date Of Birth', 'Autopay', 'Eligible for Commission',
        'Number of Members', 'Payable Agent', 'AR Policy Type', 'ICHRA Indicator',
        'Employer Group Name', 'Employer Start Date', 'Employer Subsidy Amount',
        'Employer Subsidy Type'
    ],
    'SIM_Anthem_Weekly_Feed': [
        'Client Name', 'Client ID', 'Market', 'Status', 'State', 'Exchange',
        'Effective Date', 'Original Effective Date', 'Cancellation Date', 'Product',
        'Plan Name', 'New Business', 'Bill Status', 'Bill Due Date', 'Renewal Month',
        'ACA', 'Family ID', 'Group Size', 'Writing Agent', 'Writing TIN',
        'Paid Agent', 'Paid TIN', 'Parent Agent', 'Parent TIN', 'Reporting Agent',
        'Reporting TIN', 'Funding Type'
    ],
    'SIM_BCBS_IL_AHM_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_BCBS_IL_MHA_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_BCBS_IL_SIM_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_BCBS_OK_AHM_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_BCBS_OK_MHA_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_BCBS_OK_SIM_Weekly_Feed': [
        'Last Name', 'First Name', 'Date of Birth', 'Record Type', 'E-App Number',
        'Exchange ID', 'Client App ID', 'Member Count', 'Group Number', 'Account Number',
        'Status', 'Product Type', 'Plan Name', 'Source', 'APTC', 'Renewal Indicator',
        'Coverage Effective Date', 'Paid To Date', 'Termed', 'Producer Name',
        'Nine Digit Producer Number', 'Client Address 1', 'Client Address 2',
        'City', 'State', 'Zip Code', "Client's Primary Phone", 'Email'
    ],
    'SIM_Cigna_Weekly_Feed': [
        'Subscriber ID (Detail Case #)', 'Customer Number (Case ID)', 'Primary Last Name',
        'Primary First Name', 'Writing Agent NPN', 'Writing Agent', 'Product Type',
        'ON/OFF Exchange', 'Subsidy', 'Total Premium', 'APTC',
        'Premium - Customer Responsibility', 'Plan Name', 'Policy Status',
        'Effective Date', 'Date Application Received', 'Renewal Month',
        'Paid Through Date', 'Termination Date', 'State', 'Customer Email Address',
        'Customer Phone Number', 'Application Id', 'Coverage Status',
        'Agent Start Date', 'Agent End Date'
    ],
    'SIM_FHCP_Weekly_Feed': [
        'MRN', 'Subscriber Number', 'Relation', 'Member Name', 'Start Date',
        'Birth Date', 'Phone Number', 'Email Address', 'Address1', 'Address2',
        'Address3', 'City', 'County', 'Covered Lives', 'Agent NPN',
        'Agent First Name', 'Agent Last Name', 'Broker ID', 'Broker Name', 'GA',
        'Vendor Number', 'IRS Number', 'Member Number ACA Issuer Id', 'HIOSNumber',
        'Plan Code', 'Plan Description', 'Plan Premium', 'APTC'
    ],
    'SIM_FloridaBlue_Weekly_Feed': [
        'Agency ID', 'Agent ID', 'Agent NPN', 'Agent Full Name', 'Plan Name',
        'Product', 'Contract ID', 'Contract Member Count', 'Member Relationship',
        'Member First Name', 'Member Last Name', 'Member DOB', 'County Name',
        'Agent Contract Start Date', 'Agent Contract Term Date', 'Exchange Indicator',
        'Member Phone Number', 'Member Email Address', 'Member FB_UID',
        'MWS Registration', 'Product Type', 'Agent Status', 'Reinstatement Indicator',
        'Member Age-In Indicator', 'Member Age-Out Indicator', 'Total Rewards Earned',
        'Total Rewards Applied to Premium', 'Total Rewards Applied to Gift Cards',
        'Member Wellness Eligibility', 'Member Participation Wellness Program',
        'Member Special Eligibility', 'Member Special Participation',
        'Master Agency Name', 'Master Agency ID', 'Product ID', 'Member Provider Name',
        'Provider Group', 'PCP Assignment', 'Best Available Address',
        'Best Available Address Line 2', 'Best Available City Name',
        'Best Available State Name', 'Best Available Postal Code', 'County Code',
        'FFM Reg Status', 'Cancellation Reason Code', 'Cancellation Reason',
        'APTC Expired Days', 'Coverage Effective Date', 'Coverage Expiration Date',
        'Member Original Effective Date', 'Metal Level', 'Contract Status',
        'Premium Amount', 'Scheduled Payment Date', 'APTC Amount',
        'Member Responsibility', 'Inconsistency Indicator',
        'Total Rewards Available Balance'
    ],
    'SIM_FloridaBlue_Weekly_Feed_Delinquent': [
        'Agency ID', 'Agent ID', 'Agent NPN', 'Agent Full Name', 'Contract ID',
        'Subscriber First Name', 'Subscriber Last Name', 'FB_UID', 'Product Type',
        'Days Delinquent', 'APTC Expired Days', 'Account Balance',
        'Member Email Address', 'APTC Flag', 'Master Agency Name', 'Agency Name',
        'Plan Name', 'Product ID', 'Product', 'Subscriber Date of Birth',
        'Coverage Effective Date', 'Coverage Expiration Date', 'Exchange Indicator',
        'Agency Type', 'APTC Effective Date', 'APTC End Date', 'Delinquent Days',
        'APTC Amount', 'Paid Thru Date', 'Member Phone Number',
        'Subscriber Mobile Phone Number', 'Subscriber Correspondence Phone Number',
        'APO Status', 'APO Failure Reason', 'Latest Payment History 1',
        'Latest Payment History 2', 'Latest Payment History 3'
    ],
    'SIM_FloridaBlue_Weekly_Feed_PaymentAssistance': [
        'Agency ID', 'Agent ID', 'Agent NPN', 'Contract ID', 'Enrollment Type',
        'Coverage Effective Date', 'Product Type', 'Subscriber First Name',
        'Subscriber Last Name', 'Member Phone Number', 'Best Available Postal Code',
        'Payment Scheduled', 'FB_UID', 'APTC Expired Days', 'Scheduled Date',
        'Plan Name', 'Product', 'Application ID', 'Member Email Address',
        'Agency Name', 'Agent Full Name', 'County Name', 'Application Received Date'
    ],
    'SIM_Molina_Weekly_Feed': [
        'Broker_NPN', 'Broker_First_Name', 'Broker_Last_Name', 'Member_First_Name',
        'Member_Last_Name', 'Address1', 'Address2', 'City', 'State', 'Zip',
        'State', 'dob', 'Gender', 'Application_Date', 'Effective_date', 'Product',
        'End_Date', 'Status', 'Member_Premium', 'Total_Premium', 'Paid_Through_Date',
        'Net_Due_Amount', 'Scheduled_Term_Date', 'HIX_ID', 'Subscriber_ID',
        'Member_Count', 'Member_Bussiness_Phone', 'Original_Effective_Date',
        'Broker_Start_Date', 'Broker_End_Date'
    ],
    'SIM_Oscar_Weekly_Feed': [
        'NPN', 'Writing number', 'Writing agent', 'Broker agency', 'Member ID',
        'Member name', 'Date of birth', 'Account creation status', 'Email',
        'Phone number', 'Mailing address', 'State', 'Enrollment type', 'On exchange',
        'FFM application ID', 'Plan', 'Premium amount', 'APTC subsidy', 'Lives',
        'Coverage start date', 'Coverage end date', 'Policy status',
        'Renewal enrollment type', 'Renewal plan', 'Renewal premium amount',
        'Renewal APTC subsidy', 'Renewal date', 'Autopay'
    ],
    'SIM_UHC_Active_Weekly_Feed': [
        'agentId', 'agentIdStatus', 'agentName', 'agentEmail', 'agentNpn',
        'memberFirstName', 'memberLastName', 'dateOfBirth', 'memberEmail',
        'memberPhone', 'memberAddress1', 'memberAddress2', 'memberCity',
        'memberState', 'memberZip', 'memberStatus', 'memberCounty', 'memberNumber',
        'policyEffectiveDate', 'Agent of Record', 'contract', 'pbp', 'segmentId',
        'applicationNumber', 'policyTermDate', 'product', 'termReasonCode',
        'individualID', 'householdID', 'memberNumber', 'mbiNumber',
        'secondaryPhoneNum', 'planStatus', 'planName', 'IFP - Plan Type',
        'IFP - GRGRID', 'IFP - Renewal', 'secondaryAddressLine1',
        'secondaryAddressLine2', 'secondaryAddressMemberCity',
        'secondaryAddressMemberState', 'secondaryAddressMemberZip',
        'secondaryAddressMemberCounty', 'IFP - FFM APP ID',
        'IFP Subscriber ICHRA/QSEHRA Status', 'nmaName80', 'nmaWidStatus80',
        'nmaNpn80', 'nmaName70', 'nmaWidStatus70', 'nmaNpn70', 'nmaName60',
        'nmaWidStatus60', 'nmaNpn60', 'fmoName50', 'fmoWidStatus50', 'fmoNpn50',
        'mgaName40', 'mgaWidStatus40', 'mgaNpn40', 'gaName30', 'gaWidStatus30',
        'gaNpn30', 'agentName20', 'agentWidStatus20', 'agentWid20', 'agentNpn20',
        'solicitorName10', 'solicitorWidStatus10', 'solicitorWid10', 'solicitorNpn10'
    ],
    'SIM_UHC_Inactive_Weekly_Feed': [
        'agentId', 'agentIdStatus', 'agentName', 'agentEmail', 'agentNpn',
        'memberFirstName', 'memberLastName', 'dateOfBirth', 'memberEmail',
        'memberPhone', 'memberAddress1', 'memberAddress2', 'memberCity',
        'memberState', 'memberZip', 'memberStatus', 'memberCounty', 'memberNumber',
        'policyEffectiveDate', 'Agent of Record', 'contract', 'pbp', 'segmentId',
        'applicationNumber', 'policyTermDate', 'product', 'termReasonCode',
        'individualID', 'householdID', 'memberNumber', 'mbiNumber',
        'secondaryPhoneNum', 'planStatus', 'planName', 'IFP - Plan Type',
        'IFP - GRGRID', 'IFP - Renewal', 'secondaryAddressLine1',
        'secondaryAddressLine2', 'secondaryAddressMemberCity',
        'secondaryAddressMemberState', 'secondaryAddressMemberZip',
        'secondaryAddressMemberCounty', 'IFP - FFM APP ID',
        'IFP Subscriber ICHRA/QSEHRA Status', 'nmaName80', 'nmaWidStatus80',
        'nmaNpn80', 'nmaName70', 'nmaWidStatus70', 'nmaNpn70', 'nmaName60',
        'nmaWidStatus60', 'nmaNpn60', 'fmoName50', 'fmoWidStatus50', 'fmoNpn50',
        'mgaName40', 'mgaWidStatus40', 'mgaNpn40', 'gaName30', 'gaWidStatus30',
        'gaNpn30', 'agentName20', 'agentWidStatus20', 'agentWid20', 'agentNpn20',
        'solicitorName10', 'solicitorWidStatus10', 'solicitorWid10', 'solicitorNpn10'
    ],
    'SIM_UHC_PreEnroll_Weekly_Feed': [
        'Agency ID', 'Agency Name', 'Agent ID', 'Agent ID Status', 'Agent Name',
        'Plan Level', 'Plan Nbr', 'Plan Name', 'App State', 'Received Date',
        'Effective Date', 'Mem First Name', 'Mem Last Name', 'Member DOB',
        'App Status'
    ]
}

# File name mapping
FILE_NAME_MAPPING = {
    'BCBS_TX_Weekly_AHM': 'SIM_BCBS_TX_AHM_MHA_NPA_Weekly_FEED_PRD',
    'BCBS_TX_Weekly_MHA': 'SIM_BCBS_TX_AHM_MHA_NPA_Weekly_FEED_PRD',
    'BCBS_TX_Weekly_NPA': 'SIM_BCBS_TX_AHM_MHA_NPA_Weekly_FEED_PRD',
    'BCBS_TX_Weekly_Feed': 'SIM_BCBS_TX_WEEKLY_FEED_PRD',
    'SIM_Ambetter_Weekly_Feed': 'SIM_AMBETTER_WEEKLY_FEED_PRD',
    'Ambetter_Weekly_Coverage_Effective_Date_Patch': 'SIM_AMBETTER_CED_PATCH_WEEKLY_FEED_PRD',
    'SIM_Ambetter_Weekly_Feed_Delinquent': 'SIM_AMBETTER_WEEKLY_FEED_DELINQUENT_PRD',
    'SIM_Ambetter_Weekly_Feed_Unpaid': 'SIM_AMBETTER_WEEKLY_FEED_UNPAID_PRD',
    'SIM_Anthem_Weekly_Feed': 'SIM_ANTHEM_WEEKLY_FEED_PRD',
    'SIM_BCBS_IL_AHM_Weekly_Feed': 'SIM_BCBS_IL_MHA_AHM_WEEKLY_FEED_PRD',
    'SIM_BCBS_IL_MHA_Weekly_Feed': 'SIM_BCBS_IL_MHA_AHM_WEEKLY_FEED_PRD',
    'SIM_BCBS_IL_SIM_Weekly_Feed': 'SIM_BCBS_IL_WEEKLY_FEED_PRD',
    'SIM_BCBS_OK_AHM_Weekly_Feed': 'SIM_BCBS_OK_MHA_AHM_WEEKLY_FEED_PRD',
    'SIM_BCBS_OK_MHA_Weekly_Feed': 'SIM_BCBS_OK_MHA_AHM_WEEKLY_FEED_PRD',
    'SIM_BCBS_OK_SIM_Weekly_Feed': 'SIM_BCBS_OK_WEEKLY_FEED_PRD',
    'SIM_Cigna_Weekly_Feed': 'SIM_CIGNA_WEEKLY_FEED_PRD',
    'SIM_FHCP_Weekly_Feed': 'SIM_FHCP_WEEKLY_FEED_PRD',
    'SIM_FloridaBlue_Weekly_Feed': 'SIM_FLORIDABLUE_WEEKLY_FEED_PRD',
    'SIM_FloridaBlue_Weekly_Feed_Delinquent': 'SIM_FLORIDABLUE_DELINQENT_WEEKLY_FEED_PRD',
    'SIM_FloridaBlue_Weekly_Feed_PaymentAssistance': 'SIM_FLORIDABLUE_PAYMENT_ASSISTANCE_WEEKLY_FEED_PRD',
    'SIM_Molina_Weekly_Feed': 'SIM_MOLINA_WEEKLY_FEED_PRD',
    'SIM_Oscar_Weekly_Feed': 'SIM_OSCAR_WEEKLY_FEED_PRD',
    'SIM_UHC_Active_Weekly_Feed': 'SIM_UHC_WEEKLY_FEED_PRD',
    'SIM_UHC_Inactive_Weekly_Feed': 'SIM_UHC_WEEKLY_FEED_PRD',
    'SIM_UHC_PreEnroll_Weekly_Feed': 'SIM_UHC_PRE_ENROLLMENT_WEEKLY_FEED_PRD'
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def is_blank(value):
    if pd.isna(value) or str(value).strip().lower() in ('na', ''):
        return True
    return False

def clean_phone(value):
    if is_blank(value): return ''
    try:
        value = str(value).strip()
        return re.sub(r'\D', '', value)
    except:
        return ''

def clean_date(value):
    if is_blank(value): return ''
    try:
        value = str(value).strip()
        dt = parse(value, dayfirst=False, fuzzy=True)
        return dt.strftime("%m/%d/%Y")
    except:
        return ''

def clean_alphabets(value):
    if is_blank(value): return ''
    try:
        value = str(value).strip()
        return re.sub(r'[^A-Za-z\s]', '', value).strip()
    except:
        return ''

def clean_digits(value):
    if is_blank(value): return ''
    try:
        value = str(value).strip()
        return re.sub(r'\D', '', value)
    except:
        return ''

def correct_row(row, validation_date, available_headers):
    corrected = {}
    for header in available_headers:
        value = row.get(header, pd.NA)
        if header == 'FILE_RECORD_BIRTH_DATE':
            corrected[header] = validation_date
        elif header in ['LAST_NAME', 'FIRST_NAME', 'RECORD_TYPE', 'POLICY_STATUS', 'PRODUCT_TYPE', 'STATE']:
            corrected[header] = clean_alphabets(value)
        elif header in ['DATE_OF_BIRTH', 'PAID_TO_DATE', 'TERMED']:
            corrected[header] = clean_date(value)
        elif header in ['E_APP_NUMBER', 'EXCHANGE_ID', 'MEMBER_COUNT', 'GROUP_NUMBER', 'ACCOUNT_NUMBER', 'NINE_DIGIT_PRODUCER_NUMBER', 'ZIP_CODE']:
            corrected[header] = clean_digits(value)
        elif header == "CLIENT'S_PRIMARY_PHONE":
            corrected[header] = clean_phone(value)
        else:
            corrected[header] = '' if is_blank(value) else str(value).strip()
    return corrected

def stringify_large_number(val):
    if is_blank(val): return ''
    try:
        val_str = str(val).strip()
        if re.match(r'^\d+\.\d+E\+\d+$', val_str):
            return str(int(float(val_str)))
        return val_str
    except:
        return str(val).strip()

def get_output_filename(input_filename, output_format):
    base_name = input_filename.rsplit('.', 1)[0]
    base_name = re.sub(r'_\d{8}', '', base_name)  # Remove date like _20250623
    base_name = re.sub(r'_(Active|Termed|Grace Period|Unpaid|Delinquent|PaymentAssistance)$', '', base_name, flags=re.IGNORECASE)
    for pattern, output_name in FILE_NAME_MAPPING.items():
        if base_name == pattern:
            return f"{output_name}.{output_format}"
    return f"corrected_file.{output_format}"

def normalize_header(header):
    return re.sub(r'\s+', '_', header.strip()).upper()

def get_expected_headers(input_filename):
    base_name = input_filename.rsplit('.', 1)[0]
    base_name = re.sub(r'_\d{8}', '', base_name)  # Remove date like _20250623
    base_name = re.sub(r'_(Active|Termed|Grace Period|Unpaid|Delinquent|PaymentAssistance)$', '', base_name, flags=re.IGNORECASE)
    return EXPECTED_HEADERS_BY_FILE.get(base_name, [])

@app.route('/', methods=['GET', 'POST'])
def index():
    global corrected_file, output_format, original_filename
    if request.method == 'POST':
        file = request.files.get('file')
        output_format = request.form.get('output_format', 'xlsx')
        original_filename = file.filename if file else ''
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('index'))

        if not allowed_file(file.filename):
            flash('Invalid file type. Use CSV or Excel files.', 'error')
            return redirect(url_for('index'))

        try:
            ext = file.filename.rsplit('.', 1)[1].lower()
            df = pd.read_csv(file) if ext == 'csv' else pd.read_excel(file)

            # Get expected headers
            expected_headers = get_expected_headers(original_filename)
            if not expected_headers:
                flash(f"No expected headers defined for file type: {original_filename}", 'error')
                return redirect(url_for('index'))

            uploaded_cols = list(df.columns)
            missing_headers = []
            extra_headers = []

            # Check for missing headers
            for header in expected_headers:
                found = False
                for col in uploaded_cols:
                    if col.lower() == header.lower():
                        found = True
                        break
                    # Check if the input header maps to the expected header via EXTENDED_HEADER_MAPPING
                    for required, possible_headers in EXTENDED_HEADER_MAPPING.items():
                        if header.lower() in [h.lower() for h in possible_headers]:
                            if col.lower() in [h.lower() for h in possible_headers]:
                                found = True
                                break
                    if found:
                        break
                if not found:
                    missing_headers.append(header)

            # Check for extra headers
            for col in uploaded_cols:
                found = False
                for header in expected_headers:
                    if col.lower() == header.lower():
                        found = True
                        break
                    # Check if the input header maps to the expected header
                    for required, possible_headers in EXTENDED_HEADER_MAPPING.items():
                        if header.lower() in [h.lower() for h in possible_headers]:
                            if col.lower() in [h.lower() for h in possible_headers]:
                                found = True
                                break
                    if found:
                        break
                if not found:
                    extra_headers.append(col)

            # If there are missing or extra headers, stop processing
            if missing_headers or extra_headers:
                error_parts = []
                if missing_headers:
                    error_parts.append(f"Missing headers: {', '.join(missing_headers)}")
                if extra_headers:
                    error_parts.append(f"Extra headers: {', '.join(extra_headers)}")
                flash(f"Header validation failed: {'; '.join(error_parts)}", 'error')
                return redirect(url_for('index'))

            # Map headers
            renamed = {}
            for col in uploaded_cols:
                norm_col = normalize_header(col)
                found = False
                for required, possible_headers in EXTENDED_HEADER_MAPPING.items():
                    if required == 'FILE_RECORD_BIRTH_DATE':
                        continue
                    if col.lower() in [h.lower() for h in possible_headers] or norm_col == required:
                        renamed[col] = required
                        found = True
                        break
                if not found:
                    renamed[col] = norm_col  # Keep unmapped headers as uppercase with underscores

            df.rename(columns=renamed, inplace=True)

            validation_date = datetime.now().strftime("%m/%d/%Y")
            if 'FILE_RECORD_BIRTH_DATE' not in df.columns:
                df['FILE_RECORD_BIRTH_DATE'] = validation_date

            if 'COVERAGE_END_DATE' in df.columns:
                df = df.drop(columns=['COVERAGE_END_DATE'])

            # Include only input headers (mapped or unmapped) plus FILE_RECORD_BIRTH_DATE
            available_headers = [col for col in df.columns if col == 'FILE_RECORD_BIRTH_DATE' or col in renamed.values()]
            corrected_rows = [correct_row(row, validation_date, available_headers) for _, row in df.iterrows()]
            corrected_df = pd.DataFrame(corrected_rows, columns=available_headers)

            columns_to_fix = ['E_APP_NUMBER', 'EXCHANGE_ID', 'CLIENT_APP_ID', 'NINE_DIGIT_PRODUCER_NUMBER', 'ACCOUNT_NUMBER']
            for col in columns_to_fix:
                if col in corrected_df.columns:
                    corrected_df[col] = corrected_df[col].apply(stringify_large_number)

            corrected_file = BytesIO()
            if output_format == 'xlsx':
                corrected_file.ext = 'xlsx'
                with pd.ExcelWriter(corrected_file, engine='xlsxwriter') as writer:
                    corrected_df.to_excel(writer, index=False, sheet_name='Sheet1')
                    workbook = writer.book
                    worksheet = writer.sheets['Sheet1']
                    bold_format = workbook.add_format({'bold': True})
                    num_format = workbook.add_format({'num_format': '0'})
                    for col_num, value in enumerate(corrected_df.columns):
                        worksheet.write(0, col_num, value, bold_format)
                    for idx, col in enumerate(corrected_df.columns):
                        worksheet.set_column(idx, idx, 25, num_format if col in columns_to_fix else None)
            else:
                corrected_file.ext = 'txt'
                corrected_df.to_csv(corrected_file, sep='\t', index=False)

            corrected_file.seek(0)
            flash("✅ File validated successfully. Download available.", 'success')
            return redirect(url_for('download', original_filename=original_filename))

        except Exception as e:
            flash(f"Error processing file: {str(e)}", 'error')
            return redirect(url_for('index'))

    return render_template('index.html')

@app.route('/download')
def download():
    global corrected_file, output_format, original_filename
    if corrected_file is None or corrected_file.getbuffer().nbytes == 0:
        flash('No validated file available to download.', 'error')
        return redirect(url_for('index'))

    corrected_file.seek(0)
    filename = get_output_filename(original_filename, output_format)
    mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if output_format == 'xlsx' else 'text/plain'
    return send_file(corrected_file, as_attachment=True, download_name=filename, mimetype=mimetype)

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)