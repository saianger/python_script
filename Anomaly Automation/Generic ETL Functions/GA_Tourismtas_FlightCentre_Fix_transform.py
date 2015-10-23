#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# GA_Tourismtas_FlightCentre_Fix_transform.py
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
import shutil;

TRANSFORM_LIST = [0]

# Input file columns

COLUMNS = anom.json_obj['Columns'];
FILES = anom.json_obj['Files'];
INPUT_PARAMS = anom.json_obj['input_params'];
ADDFIELDS = INPUT_PARAMS['addfields'];

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

  Columns = COLUMNS[FILES[i]];                                                                                                # get column names for file type
  rows = [];                                                                                                                  # initialize row buffer

  for file in f_list:
    tmpfile = '%s%s' % (file, anom.TMPEXT);
    try:
      with open(file, 'rb') as Csvfil:
        dialect = csv.Sniffer().sniff(Csvfil.readline(),[',','|']);                                                           # determine delimiter value (could be run a second time)
        anom.wrt_log('%s is delimited with "%s"' % (file, dialect.delimiter));
        Csvfil.seek(0);
        records = csv.DictReader(Csvfil, delimiter=dialect.delimiter);                                                        # read file into dictionary
        with open(tmpfile, 'wb') as Csvout:                                                                                   # create output file
          writer = csv.DictWriter(Csvout, fieldnames=Columns);                                                                # create a dictionary for output rows
          rows.append(dict((fn,fn) for fn in Columns));                                                                       # write the header record
          for row in records:
            for key in ADDFIELDS.keys():
              row[key] = ADDFIELDS[key]
            rows.append(row);                                                                                                 # add to row buffer
            if len(rows) == 100000:
              writer.writerows(rows);                                                                                         # write block of rows to output
              rows = [];
          writer.writerows(rows);                                                                                             # write final rows to output
        rows = [];
      anom.wrt_log('Garbage Collection %d' % gc.collect());
      shutil.move(tmpfile, file);
      return True;
    except Exception as e:
      anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, ', '.join(e.args), inspect.stack()[0][3], __name__);
      return False;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of transform code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
