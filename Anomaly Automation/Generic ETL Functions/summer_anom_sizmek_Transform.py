#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# summer_anom_sizmek_Transform.py
#
#   Transform functions script ETL_Control.py script
#   Copy data into redshift cluster
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

import csv;
import inspect;
import gc;
from dateutil.parser import *;
import re;

TRANSFORM_LIST = [0, 1,]

# Input file columns

TABLE_STRUCT = anom.json_obj['TableStruct'];
TABLES = anom.json_obj['Tables'];

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform function
# 1 . files = a file or list of files to process
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def transform(f, i):

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

  for file in f_list:
    data_ts = parse(re.search(r'\d{4}\d{2}\d{2}', file).group()).strftime('%Y-%m-%d');                                        # get date from filename
    Columns = TABLE_STRUCT[TABLES[i]]['Columns'];                                                                                  # get column names for file
    try:
      with open(file, 'rb') as Csvfil:
        dialect = csv.Sniffer().sniff(Csvfil.readline(),[',','|']);                                                           # determine delimiter value (could be run a second time)
        anom.wrt_log('%s is delimited with %s' % (file, dialect.delimiter));
        Csvfil.seek(0);
        records = csv.DictReader(Csvfil, delimiter=dialect.delimiter);                                                        # read file into dictionary
        rows = [];                                                                                                            # initialize row buffer
        for row in records:
          row['data_ts'] = data_ts;
          for r in row.keys():
            if not (r in Columns): del row[r];                                                                                # delete any artifact keys from source
          rows.append(row);                                                                                                   # add to row buffer
        records = None;
      with open(file, 'wb') as Csvfil:                                                                                        # create output file
        writer = csv.DictWriter(Csvfil, fieldnames=Columns);                                                                  # create a dictionary for output rows
        writer.writerow(dict((fn,fn) for fn in Columns));                                                                     # write the header record
        for row in rows:
          writer.writerow(row);                                                                                               # write row to output
        rows = [];
        anom.wrt_log('%s Transformed and Garbage Collection %d' % (file, gc.collect()));
      return True;
    except Exception as e:
      anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, ', '.join(e.args), inspect.stack()[0][3], __name__);
      return False;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of transform code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
