#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Gupta-extract.py
#
#   Functions file for ec2_s3_redshift.py ETL script
#
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone hwo can assist you.
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

import csv;                                                                                                                   # csv module
import inspect;                                                                                                               # inspect module
import json;                                                                                                                  # json module
import urllib;                                                                                                                # urllib module
campaign_ids = [];                                                                                                            # initialze list of campaign ids

def extract(f_list, d_list=[], jobname=''):

# Set time now

  anom.curdat();

# Get dates from CLI

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

  if start_date == '':
    anom.Errmsg = "No start date specified. set --start-date YYYY-MM-DD in command line options";
    return False;

  if end_date == '': end_date = (anom.datval).strftime("%Y-%m-%d");                                                           # today

  result = False;                                                                                                             # initialize return result

  for file in f_list:
    exec 'result = extract_%s(file, start_date, end_date, campaign_ids)' % file;                                                                    # execute the extract functions in sequence of file type
    if not result: return False;

  return True;

def extract_campaignSummaries(f_name, start, end, ids):

# result = urllib.urlopen(anom.get_gupta(gupta_obj, '2014-10-31', '2015-09-08', 0))

  global campaign_ids;                                                                                                        # use global campaign_ids object
  COLUMNS = anom.json_obj['Columns'][f_name];                                                                                     # get the column headers
  Columns = [];
  for column in COLUMNS: Columns.append(column[0]);
  Artists = anom.json_obj['BlankArtists'];                                                                                        # get list of known blank artists with no '-' in name

  result = urllib.urlopen(anom.get_gupta(anom.json_obj, start, end, 0));                                                          # get the data form the gupta api

  code = result.code;                                                                                                         # get the result code
  if code != 200:
    anom.Errmsg = "Error {0}: {1} function:{2} module:{3}".format(code, anom.HTTPERROR[code], inspect.stack()[0][3], __name__);
    return False;

  PAYLOAD = anom.json_dict(json.loads(result.read()));
  if PAYLOAD.has_key("returnValue"):
    RECORDS = PAYLOAD["returnValue"];
  else:
    anom.Errmsg = "No records found: function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
    return False;

  if len(RECORDS) < 1:
    anom.Errmsg = "Empty result set from API: function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
    return False;

  anom.wrt_log('%d %s records retrieved' % (len(RECORDS), f_name));

  try:
    out_file = '{0}_{1}_{2}{3}'.format(f_name, start, end, anom.CSVEXT);
    with open(out_file, 'wb') as Csvout:                                                                                      # create output file
      writer = csv.DictWriter(Csvout, fieldnames=Columns);                                                                    # create a dictionary for output rows
      writer.writerow(dict((fn,fn) for fn in Columns));                                                                       # write the header record
      for row in RECORDS:
        for key in row.keys():
          if type(row[key]) in [str, unicode]:
            row[key] = str(row[key]);                                                                                         # force to type str
        if row['artist'] == anom.NULCHR:
          for artist in Artists:
            if (not '-' in row['name']) and (artist in row['name']):
              row['artist'] = artist;                                                                                         # set from known blank artists list
              break;
          anom.strtok(row['name'], '-', False);
          row['artist'] = anom.lstval[0].strip();                                                                             # set artist if blank
        if row['id'] not in campaign_ids: campaign_ids.append(row['id']);                                                     # save the campaign id make sure no duplicates
      writer.writerows(RECORDS);                                                                                              # write the records to the file
    anom.wrt_log('Created %s' % out_file);
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  return True;

def extract_campaignPerformance(f_name, start, end, ids):

  global campaign_ids;                                                                                                        # use global campaign_ids object
  COLUMNS = anom.json_obj['Columns'][f_name];                                                                                     # get the column headers
  Columns = [];
  for column in COLUMNS: Columns.append(column[0]);
  rows = [];
  for id in ids:
    result = urllib.urlopen(anom.get_gupta(anom.json_obj, start, end, 1, id));                                                    # get the data form the gupta api

    code = result.code;                                                                                                       # get the result code
    if code != 200:
      anom.Errmsg = "Error {0}: {1} function:{2} module:{3}".format(code, anom.HTTPERROR[code], inspect.stack()[0][3], __name__);
      return False;

    PAYLOAD = anom.json_dict(json.loads(result.read()));
    if PAYLOAD.has_key("returnValue"):
      RECORDS = PAYLOAD["returnValue"];
    else:
      anom.wrt_log("No records found for Campaign ID {0} function:{1} module:{2}".format(id, inspect.stack()[0][3], __name__));
      continue;

    if len(RECORDS) < 1:
      anom.Errmsg = "Empty result set from API for Campaign ID {0} function:{1} module:{2}".format(id, inspect.stack()[0][3], __name__);
      continue;

    rows.extend(RECORDS);

  anom.wrt_log('%d %s records retrieved' % (len(rows), f_name));

  try:
    out_file = '{0}_{1}_{2}{3}'.format(f_name, start, end, anom.CSVEXT);
    with open(out_file, 'wb') as Csvout:                                                                                      # create output file
      writer = csv.DictWriter(Csvout, fieldnames=Columns);                                                                    # create a dictionary for output rows
      writer.writerow(dict((fn,fn) for fn in Columns));                                                                       # write the header record
      for row in rows:
        for key in row.keys():
          if type(row[key]) in [str, unicode]:
            row[key] = str(row[key]);                                                                                         # force to type str
      writer.writerows(rows);                                                                                              # write the records to the file
    anom.wrt_log('Created %s' % out_file);
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  return True;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
