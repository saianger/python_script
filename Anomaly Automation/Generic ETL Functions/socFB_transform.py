import json;
import csv;
import shutil;

TRANSFORM_LIST = [0, 1, 2, 3,]

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform function
# 1 . files = a file or list of files to process
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform(f, i):

  if anom.options.stop_process == 1: anom.err_exit('Debug Process stopped %d' % anom.options.stop_process);
  if (i not in TRANSFORM_LIST):
    return True;

# get list of files

  if len(f) < 1:
    return True;                                                                                                              # nothing to transform

  f_list = [];                                                                                                                # initialise list

  if (type(f) == str):
    f_list.append(f);                                                                                                         # put single filename into list
  else:
    if (type(f) == list):
      f_list = f;                                                                                                             # assign list

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Write your transform code here
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  for file in f_list:

    with open(file, 'rb') as f:
      try:
        data = f.readline();
        json.loads(data);
      except ValueError:
        anom.do_nothing('Already Transformed %s' % file);
        continue;

# Read json object records into list

    try:
      with open(file, 'rb') as inpfil:
        exec 'rows = transform_%s(inpfil)' % i;                                                                               # execute loop based function
    except IOError as e:
      anom.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;

# Write rows to output file

    try:
      outname = "%s%s" % (file, anom.TMPEXT);
      with open(outname, 'wb') as outfil:
        csvwriter = csv.writer(outfil);
        csvwriter.writerows(rows);                                                                                            # write rows returned from function
      shutil.move(outname, file);
      anom.wrt_log('Transformed %s' % f);
    except IOError as e:
      anom.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;

  return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform actions
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform_0(f):

  rows = [];
  row = \
  [
    'date_start',
    'date_stop',
    'account_name',
    'campaign_group_id',
    'campaign_group_name',
    'campaign_id',
    'campaign_name',
    'adgroup_id',
    'adgroup_name',
    'action type',
    'value',
  ];
  rows.append(row);

  for j_row in f:

# Append data to rows list

    j = json.loads(j_row);
    for action in j["actions"]:
      row = \
      [
        j["date_start"],
        j["date_stop"],
        j["account_name"],
        'ID'+str(j["campaign_group_id"]),
        j["campaign_group_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        'ID'+str(j["campaign_id"]),
        j["campaign_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        'ID'+str(j["adgroup_id"]),
        j["adgroup_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        action["action_type"],
        action["value"],
      ];
      rows.append(row);

  return rows;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform Campaign_basic_metrics
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform_1(f):

  rows = [];
  row = \
  [
    'date_start',
    'date_stop',
    'account_name',
    'campaign_group_id',
    'campaign_group_name',
    'campaign_id',
    'campaign_name',
    'adgroup_id',
    'adgroup_name',
    'reach',
    'frequency',
    'clicks',
    'impressions',
    'spend',
    'actions',
  ];
  rows.append(row);

  for j_row in f:

# Append data to rows list

    j = json.loads(j_row);
    row = \
    [
      j["date_start"],
      j["date_stop"],
      j["account_name"],
      'ID'+str(j["campaign_group_id"]),
      j["campaign_group_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
      'ID'+str(j["campaign_id"]),
      j["campaign_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
      'ID'+str(j["adgroup_id"]),
      j["adgroup_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
      j["reach"],
      j["frequency"],
      j["clicks"],
      j["impressions"],
      j["spend"],
      j["actions"],
    ];
    rows.append(row);

  return rows;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform Clicks to play
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform_2(f):

  rows = [];
  row = \
  [
    'date_start',
    'date_stop',
    'account_name',
    'campaign_group_id',
    'campaign_group_name',
    'campaign_id',
    'campaign_name',
    'adgroup_id',
    'adgroup_name',
    'action_video_type',
    'value',
  ];
  rows.append(row);

  for j_row in f:

# Append data to rows list

    j = json.loads(j_row);
    for action in j["actions"]:
      if action['value'] > 0 :
        row = \
        [
          j["date_start"],
          j["date_stop"],
          j["account_name"],
          'ID'+str(j["campaign_group_id"]),
          j["campaign_group_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
          'ID'+str(j["campaign_id"]),
          j["campaign_name"],
          'ID'+str(j["adgroup_id"]),
          j["adgroup_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
          action["action_video_type"],
          action["value"],
        ];
        rows.append(row);

  return rows;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform VIDEO metrics
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform_3(f):

  rows = [];
  row = \
  [
    'date_start',
    'date_stop',
    'account_name',
    'campaign_group_id',
    'campaign_group_name',
    'campaign_id',
    'campaign_name',
    'adgroup_id',
    'adgroup_name',
    'video_p25_watched_actions',
    'video_p50_watched_actions',
    'video_p75_watched_actions',
    'video_p95_watched_actions',
    'video_p100_watched_actions',
    'video_complete_watched_actions',
    'video_avg_sec_watched_actions',
    'video_avg_pct_watched_actions',
  ];
  rows.append(row);
  for j_row in f:

# Append data to rows list

    j = json.loads(j_row);

    if type(j['video_p25_watched_actions']) is list:
      if len(j['video_p25_watched_actions']) < 1:
        j['video_p25_watched_actions'] = 0;

    if type(j['video_p50_watched_actions']) is list:
      if len(j['video_p50_watched_actions']) < 1:
        j['video_p50_watched_actions'] = 0;

    if type(j['video_p75_watched_actions']) is list:
      if len(j['video_p75_watched_actions']) < 1:
        j['video_p75_watched_actions'] = 0;

    if type(j['video_p95_watched_actions']) is list:
      if len(j['video_p95_watched_actions']) < 1:
        j['video_p95_watched_actions'] = 0;

    if type(j['video_p100_watched_actions']) is list:
      if len(j['video_p100_watched_actions']) < 1:
        j['video_p100_watched_actions'] = 0;

    if type(j['video_complete_watched_actions']) is list:
      if len(j['video_complete_watched_actions']) < 1:
        j['video_complete_watched_actions'] = 0;

    if \
    (
      j['video_p25_watched_actions'] + \
      j['video_p50_watched_actions'] + \
      j['video_p75_watched_actions'] + \
      j['video_p95_watched_actions'] + \
      j['video_p100_watched_actions'] + \
      j['video_complete_watched_actions'] != 0
    ):
      row = \
      [
        j["date_start"],
        j["date_stop"],
        j["account_name"],
        'ID'+str(j["campaign_group_id"]),
        j["campaign_group_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        'ID'+str(j["campaign_id"]),
        j["campaign_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        'ID'+str(j["adgroup_id"]),
        j["adgroup_name"].replace(u'\u2019',u'\u0027').replace('\\u2019',"'"),
        j["video_p25_watched_actions"],
        j["video_p50_watched_actions"],
        j["video_p75_watched_actions"],
        j["video_p95_watched_actions"],
        j["video_p100_watched_actions"],
        j["video_complete_watched_actions"],
        j["video_avg_sec_watched_actions"],
        j["video_avg_pct_watched_actions"],
      ];
      rows.append(row);

  return rows;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of transform code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
