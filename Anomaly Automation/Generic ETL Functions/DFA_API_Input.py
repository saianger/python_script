#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# SonyYT_Input.py
#
#   Paramter file for ec2_s3_redshift.py ETL script
#   Copy data into redshift cluster
#
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone who can assist you.
# 3. If you still don't know, you will LEAVE IT ALONE.
#
# Copyright (c) 2015 Anomaly
# Author Tony Edward
#----------------------------------------
#     I M P O R T A N T  N O T E S
#
#      Read the above Assumptions
#----------------------------------------

#*# Machine alterable code below this line.

DATEFORMAT1 = 'auto';
TIMEFORMAT1 = 'MM/DD/YYYY HH24:MI';

# File System

SRC_DIR = '/mnt/data/data-collection/DFA_API/scripts';

# list of input files

FILE_PROC_LIST = \
[
  'StandardDatafeed_Conversions',
  'StandardDatafeed_Metrics',
  'StandardDatafeed_DynamicCreative_conversions',
  'StandardDatafeed_DynamicCreative_Metrics',
  'StandardDatafeed_Lookup',
  'StandardDataFeed_Booked',
];

# List of redshift tables must correspond with above list of input files

REDSHIFT_TABLE_LIST = \
[
  'standarddatafeed_conversions',
  'standarddatafeed_metrics',
  'standarddatafeed_dynamiccreative_conversions',
  'standarddatafeed_dynamiccreative_metrics',
  'standarddatafeed_lookup',
  'standarddatafeed_booked',
];

# S3 path to storage (also known as redshift KEY)

S3_PATH = '/data-processing/%s/' % anom.JENKINS_ENV['JOB_NAME'];

# PostgreSQL/Redshift

DBNAME = 'dsaf';
STAGING = 'staging_';
SCHEMA_NAME = 'dfa.';
ERROR_COUNT = 30;
SQL_DEL = ',';

# Staging SQL statements (encapsulated in a function that can return a list of dynamically created SQL statements)

#--------------- W A R N I N G -------------------
#       The Magic of Dynamic SQL at work
#
# Any placeholder '%s' that is loaded with a '{0}'
# in the list of string substitution parameters
# will be populated at runtime with the current
# element from REDSHIFT_TABLE_LIST.
# Make sure you understand the mechanism of the
# SQL_LIST list prior to making any changes.
#
# If unsure of the process, ask someone who knows.
# If the Anomaly WIKI is working, consult the WIKI
# for a list of knowledgable boffins who do.
#-------------------------------------------------

BEGIN_TRANSACTION = \
  "BEGIN TRANSACTION;";

END_TRANSACTION = \
  "END TRANSACTION;";

DELETE_FROM_TABLE = \
  "DELETE " + \
  "FROM %s%s " % (SCHEMA_NAME, '{0}') + \
  "WHERE " + \
  "ddate >= (SELECT MIN(ddate) FROM %s%s%s);" % (SCHEMA_NAME, STAGING, '{0}');

STANDARDDATAFEED_CONVERSIONS = \
  "INSERT " + \
  "INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT " + \
  "TO_DATE(A.ddate,'YYYY-MM-DD'), " + \
  "A.placement_id, " + \
  "A.ad_id, " + \
  "A.creative_id, " + \
  "A.activity, " + \
  "A.click_through_conversions, " + \
  "A.click_through_revenue, " + \
  "A.view_through_conversions, " + \
  "A.view_through_revenue, " + \
  "A.total_conversions, " + \
  "A.total_revenue " + \
  "FROM " + \
  "(" + \
  "SELECT " + \
  "CASE WHEN " + \
  "ddate LIKE '%/%' " + \
  "THEN " + \
  "TO_CHAR(TO_DATE(ddate,'DD/MM/YYYY'),'YYYY-MM-DD') " + \
  "ELSE " + \
  "ddate " + \
  "END, " + \
  "placement_id, " + \
  "ad_id, " + \
  "creative_id, " + \
  "activity, " + \
  "click_through_conversions, " + \
  "click_through_revenue, " + \
  "view_through_conversions, " + \
  "view_through_revenue, " + \
  "total_conversions, " + \
  "total_revenue " + \
  "FROM %s%s%s " % (SCHEMA_NAME, STAGING, '{0}') + \
  "WHERE " + \
  "ddate IS NOT NULL) A;";

STANDARDDATAFEED_METRICS = \
  "INSERT " + \
  "INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT " + \
  "TO_DATE(A.ddate,'YYYY-MM-DD'), " + \
  "A.placement_id, " + \
  "A.ad_id, " + \
  "A.creative_id, " + \
  "A.impressions, " + \
  "A.html5_impressions, " + \
  "A.clicks, " + \
  "A.video_plays, " + \
  "A.video_first_quartile_completions, " + \
  "A.video_midpoints, " + \
  "A.video_third_quartile_completions, " + \
  "A.video_completions, " + \
  "A.video_mutes, " + \
  "A.video_pauses, " + \
  "A.expansions, " + \
  "A.total_interactions, " + \
  "A.MEDIA_COST " + \
  "FROM " + \
  "(" + \
  "SELECT " + \
  "CASE WHEN " + \
  "ddate LIKE '%/%' " + \
  "THEN " + \
  "TO_CHAR(TO_DATE(ddate,'DD/MM/YYYY'),'YYYY-MM-DD') " + \
  "ELSE " + \
  "ddate " + \
  "END, " + \
  "placement_id, " + \
  "ad_id, " + \
  "creative_id, " + \
  "impressions, " + \
  "html5_impressions, " + \
  "clicks, " + \
  "video_plays, " + \
  "video_first_quartile_completions, " + \
  "video_midpoints, " + \
  "video_third_quartile_completions, " + \
  "video_completions, " + \
  "video_mutes, " + \
  "video_pauses, " + \
  "expansions, " + \
  "total_interactions, " + \
  "media_cost " + \
  "FROM %s%s%s " % (SCHEMA_NAME, STAGING, '{0}') + \
  "WHERE " + \
  "ddate IS NOT NULL) A;";

STANDARDDATAFEED_DYNAMICCREATIVE_CONVERSIONS = \
  "INSERT " + \
  "INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT " + \
  "TO_DATE(A.ddate,'YYYY-MM-DD'), " + \
  "A.placement_id, " + \
  "A.ad_id, " + \
  "A.creative_id, " + \
  "A.activity, " + \
  "A.dynamic_lement, " + \
  "A.dynamic_lement_value_id, " + \
  "A.dynamic_lement_value, " + \
  "A.dynamic_field_value_1, " + \
  "A.dynamic_field_value_2, " + \
  "A.dynamic_field_value_3, " + \
  "A.dynamic_field_value_4, " + \
  "A.dynamic_field_value_5, " + \
  "A.dynamic_field_value_6, " + \
  "A.dynamic_profile, " + \
  "A.dynamic_profile_id, " + \
  "A.dynamic_element_click_through_conversions, " + \
  "A.dynamic_element_view_through_conversions, " + \
  "A.dynamic_element_total_conversions " + \
  "FROM " + \
  "(" + \
  "SELECT " + \
  "CASE WHEN " + \
  "ddate LIKE '%/%' " + \
  "THEN " + \
  "TO_CHAR(TO_DATE(ddate,'DD/MM/YYYY'),'YYYY-MM-DD') " + \
  "ELSE " + \
  "ddate " + \
  "END, " + \
  "placement_id, " + \
  "ad_id, " + \
  "creative_id, " + \
  "activity, " + \
  "dynamic_lement, " + \
  "dynamic_lement_value_id, " + \
  "dynamic_lement_value, " + \
  "dynamic_field_value_1, " + \
  "dynamic_field_value_2, " + \
  "dynamic_field_value_3, " + \
  "dynamic_field_value_4, " + \
  "dynamic_field_value_5, " + \
  "dynamic_field_value_6, " + \
  "dynamic_profile, " + \
  "dynamic_profile_id, " + \
  "dynamic_element_click_through_conversions, " + \
  "dynamic_element_view_through_conversions, " + \
  "dynamic_element_total_conversions " + \
  "FROM %s%s%s " % (SCHEMA_NAME, STAGING, '{0}') + \
  "WHERE " + \
  "ddate IS NOT NULL) A;";

STANDARDDATAFEED_DYNAMICCREATIVE_METRICS = \
  "INSERT " + \
  "INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT " + \
  "TO_DATE(A.ddate,'YYYY-MM-DD'), " + \
  "A.placement_id, " + \
  "A.ad_id, " + \
  "A.creative_id, " + \
  "A.activity, " + \
  "A.dynamic_element, " + \
  "A.dynamic_element_value_id, " + \
  "A.dynamic_element_value, " + \
  "A.dynamic_field_value_1, " + \
  "A.dynamic_field_value_2, " + \
  "A.dynamic_field_value_3, " + \
  "A.dynamic_field_value_4, " + \
  "A.dynamic_field_value_5, " + \
  "A.dynamic_field_value_6, " + \
  "A.dynamic_profile, " + \
  "A.dynamic_profile_id, " + \
  "A.dynamic_element_impressions, " + \
  "A.dynamic_element_clicks, " + \
  "A.dynamic_element_click_through_conversions, " + \
  "A.dynamic_element_view_through_conversions, " + \
  "A.dynamic_element_total_conversions " + \
  "FROM " + \
  "(" + \
  "SELECT " + \
  "CASE WHEN " + \
  "ddate LIKE '%/%' " + \
  "THEN " + \
  "TO_CHAR(TO_DATE(ddate,'DD/MM/YYYY'),'YYYY-MM-DD') " + \
  "ELSE " + \
  "ddate " + \
  "END, " + \
  "placement_id, " + \
  "ad_id, " + \
  "creative_id, " + \
  "activity, " + \
  "dynamic_element, " + \
  "dynamic_element_value_id, " + \
  "dynamic_element_value, " + \
  "dynamic_field_value_1, " + \
  "dynamic_field_value_2, " + \
  "dynamic_field_value_3, " + \
  "dynamic_field_value_4, " + \
  "dynamic_field_value_5, " + \
  "dynamic_field_value_6, " + \
  "dynamic_profile, " + \
  "dynamic_profile_id, " + \
  "dynamic_element_impressions, " + \
  "dynamic_element_clicks, " + \
  "dynamic_element_click_through_conversions, " + \
  "dynamic_element_view_through_conversions, " + \
  "dynamic_element_total_conversions " + \
  "FROM %s%s%s " % (SCHEMA_NAME,STAGING, '{0}') + \
  "WHERE " + \
  "ddate IS NOT NULL) A;";

STANDARDDATAFEED_LOOKUP_UPDATE = \
  "UPDATE %s%s " % (SCHEMA_NAME, '{0}') + \
  "SET " + \
  "advertiser=B.advertiser, " + \
  "campaign=B.campaign, " + \
  "campaign_id=B.campaign_id, " + \
  "site=B.site, " + \
  "package_roadblock=B.package_roadblock, " + \
  "package_roadblock_id=B.package_roadblock_id, " + \
  "placement=B.placement, " + \
  "placement_strategy=B.placement_strategy, " + \
  "placement_id=B.placement_id, " + \
  "placement_start_date=TO_DATE(CASE WHEN B.placement_start_date like '%/%' THEN TO_CHAR(TO_DATE(B.placement_start_date,'DD/MM/YYYY'),'YYYY-MM-DD') ELSE B.placement_start_date END,'YYYY-MM-DD'), " + \
  "placement_end_date=TO_DATE(CASE WHEN B.placement_end_date LIKE '%/%' THEN TO_CHAR(TO_DATE(B.placement_end_date,'DD/MM/YYYY'),'YYYY-MM-DD') ELSE B.placement_end_date END,'YYYY-MM-DD'), " + \
  "ad=B.ad, " + \
  "ad_id=B.ad_id, " + \
  "ad_type=B.ad_type, " + \
  "creative=B.creative, " + \
  "creative_id=B.creative_id, " + \
  "creative_pixel_size=B.creative_pixel_size, " + \
  "creative_type=B.creative_type, " + \
  "dynamic_profile=B.dynamic_profile, " + \
  "dynamic_profile_id=B.dynamic_profile_id, " + \
  "profileid=B.profileid " + \
  "FROM " + \
  "%s%s A, "  % (SCHEMA_NAME, '{0}') + \
  "%s%s%s B " % (SCHEMA_NAME,STAGING, '{0}') + \
  "WHERE " + \
  "TRIM(A.placement_id||A.ad_id||A.creative_id) = TRIM(B.placement_id||B.ad_id||B.creative_id);";

STANDARDDATAFEED_LOOKUP_INSERT = \
  "INSERT INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT DISTINCT " + \
  "A.advertiser, " + \
  "A.campaign, " + \
  "A.campaign_id, " + \
  "A.site, " + \
  "A.package_roadblock, " + \
  "A.package_roadblock_id, " + \
  "A.placement, " + \
  "A.placement_strategy, " + \
  "A.placement_id, " + \
  "TO_DATE(CASE WHEN A.placement_start_date LIKE '%/%' THEN TO_CHAR(TO_DATE(A.placement_start_date,'DD/MM/YYYY'),'YYYY-MM-DD') ELSE A.placement_start_date END,'YYYY-MM-DD' ), " + \
  "TO_DATE(CASE WHEN A.placement_end_date LIKE '%/%' THEN TO_CHAR(TO_DATE(A.placement_end_date,'DD/MM/YYYY'),'YYYY-MM-DD') ELSE A.placement_end_date END,'YYYY-MM-DD' ), " + \
  "A.ad, " + \
  "A.ad_id, " + \
  "A.ad_type, " + \
  "A.creative, " + \
  "A.creative_id, " + \
  "A.creative_pixel_size, " + \
  "A.creative_type, " + \
  "A.dynamic_profile, " + \
  "A.dynamic_profile_id, " + \
  "A.placement_cost_structure, " + \
  "A.profileid " + \
  "FROM " + \
  "%s%s%s A " % (SCHEMA_NAME, STAGING, '{0}') + \
  "WHERE NOT EXISTS " + \
  "(SELECT 1 FROM %s%s B " % (SCHEMA_NAME, '{0}') + \
  "WHERE " + \
  "TRIM(B.placement_id||B.ad_id||B.creative_id) = TRIM(A.placement_id||A.ad_id||A.creative_id));";

STANDARDDATAFEED_BOOKED_UPDATE = \
  "UPDATE %s%s " % (SCHEMA_NAME, '{0}') + \
  "SET " + \
  "advertiser=B.advertiser, " + \
  "advertiser_id=B.advertiser_id, " + \
  "campaign=B.campaign, " + \
  "campaign_id=B.campaign_id, " + \
  "package_roadblock=B.package_roadblock, " + \
  "package_roadblock_id=B.package_roadblock_id, " + \
  "package_roadblock_total_booked_units=B.package_roadblock_total_booked_units, " + \
  "placement_total_planned_media_cost=B.placement_total_planned_media_cost, " + \
  "impressions=B.impressions, " + \
  "clicks=B.clicks, " + \
  "html5_impressions=B.html5_impressions, " + \
  "media_cost=B.media_cost, " + \
  "total_interactions=B.total_interactions, " + \
  "video_plays=B.video_plays, " + \
  "profileid=B.profileid " + \
  "FROM " + \
  "%s%s A, "  % (SCHEMA_NAME, '{0}') + \
  "%s%s%s B " % (SCHEMA_NAME,STAGING, '{0}') + \
  "WHERE " + \
  "TRIM(A.package_roadblock_id) = TRIM(B.package_roadblock_id);";

STANDARDDATAFEED_BOOKED_INSERT = \
  "INSERT INTO %s%s " % (SCHEMA_NAME, '{0}') + \
  "SELECT DISTINCT " + \
  "A.advertiser, " + \
  "A.advertiser_id, " + \
  "A.campaign, " + \
  "A.campaign_id, " + \
  "A.package_roadblock, " + \
  "A.package_roadblock_id, " + \
  "A.package_roadblock_total_booked_units, " + \
  "A.placement_total_planned_media_cost, " + \
  "A.impressions, " + \
  "A.clicks, " + \
  "A.html5_impressions, " + \
  "A.media_cost, " + \
  "A.total_interactions, " + \
  "A.video_plays, " + \
  "A.profileid " + \
  "FROM " + \
  "%s%s%s A " % (SCHEMA_NAME, STAGING, '{0}') + \
  "WHERE NOT EXISTS " + \
  "(SELECT 1 FROM %s%s B " % (SCHEMA_NAME, '{0}') + \
  "WHERE " + \
  "TRIM(B.package_roadblock_id) = TRIM(A.package_roadblock_id));";

# FIX_PROFILEID = \
#   "ALTER TABLE " + \
#   "%s%s%s " % (SCHEMA_NAME,STAGING, '{0}') + \
#   "ADD COLUMN " + \
#   "profileid varchar(256);";
#
# ADD_PROFILE_ID = \
#   "UPDATE %s%s%s " % (SCHEMA_NAME,STAGING, '{0}') + \
#   "SET " + \
#   "profileid=B.profileid " + \
#   "FROM " + \
#   "%s%s%s A, " % (SCHEMA_NAME, STAGING, '{0}') + \
#   "%s%s B " % (SCHEMA_NAME, 'advertiser_profileid') + \
#   "WHERE " + \
#   "TRIM(A.advertiser) = TRIM(B.advertiser);";
#
# DROP_PROFILEID = \
#   "ALTER TABLE " + \
#   "%s%s%s " % (SCHEMA_NAME,STAGING, '{0}') + \
#   "DROP COLUMN " + \
#   "profileid;";

def staging_sql():

  SQL_LIST = \
  [
    [
      BEGIN_TRANSACTION,
      DELETE_FROM_TABLE,
      STANDARDDATAFEED_CONVERSIONS,
      END_TRANSACTION,
    ],
    [
      BEGIN_TRANSACTION,
      DELETE_FROM_TABLE,
      STANDARDDATAFEED_METRICS,
      END_TRANSACTION,
    ],
    [
      BEGIN_TRANSACTION,
      DELETE_FROM_TABLE,
      STANDARDDATAFEED_DYNAMICCREATIVE_CONVERSIONS,
      END_TRANSACTION,
    ],
    [
      BEGIN_TRANSACTION,
      DELETE_FROM_TABLE,
      STANDARDDATAFEED_DYNAMICCREATIVE_METRICS,
      END_TRANSACTION,
    ],
    [
      BEGIN_TRANSACTION,
      STANDARDDATAFEED_LOOKUP_UPDATE,
      STANDARDDATAFEED_LOOKUP_INSERT,
      END_TRANSACTION,
    ],
    [
      BEGIN_TRANSACTION,
      STANDARDDATAFEED_BOOKED_UPDATE,
      STANDARDDATAFEED_BOOKED_INSERT,
      END_TRANSACTION,
    ],
  ];

  return SQL_LIST;

# Email constants

TO_ADDR = ['tony.edward@anom.com.au', 'leo.li@anom.com.au'];

#------------------------------------
# End of parameter script
#------------------------------------
