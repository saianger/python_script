#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# socFB-extract.py
#
#   Functions file for ec2_s3_redshift.py ETL script
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

FB_METRICS_LIST = \
[
  anom.json_obj['params_actions'],
  anom.json_obj['params_basic_metrics'],
  anom.json_obj['params_video_click_metrics'],
  anom.json_obj['params_video_metrics'],
];

PARAMS_LIST = \
[
  anom.json_obj['my_app_id'],
  anom.json_obj['my_app_secret'],
  anom.json_obj['my_access_token'],
  anom.json_obj['account_id'],
];

def extract(f_list, d_list, jobname):

  result = anom.fb_api_pull(PARAMS_LIST, f_list, FB_METRICS_LIST);
  return result;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
