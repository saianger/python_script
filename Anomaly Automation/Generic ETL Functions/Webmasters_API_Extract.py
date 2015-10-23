#!/usr/bin/python

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Redhat old Crypto warning bypass
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from Crypto.pct_warnings import PowmInsecureWarning;
import warnings;
warnings.simplefilter("ignore", PowmInsecureWarning);

import datetime;
from dateutil.parser import *;
import httplib2;
import os;
import sys;
import glob;
import csv;
import urllib;
import lxml;
from lxml import etree;
import inspect;
import tldextract
import json;

#from apiclient.discovery import build;
#from oauth2client.file import Storage;
#from oauth2client.client import flow_from_clientsecrets;
#from oauth2client.tools import run;

# CLIENT_SECRETS_FILE, name of a file containing the OAuth 2.0 information for
# this application, including client_id and client_secret. You can acquire an
# ID/secret pair from the API Access tab on the Google APIs Console
#   http://code.google.com/apis/console#access
# For more information about using OAuth2 to access Google APIs, please visit:
#   https://developers.google.com/accounts/docs/OAuth2
# For more information about the client_secrets.json file format, please visit:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
# Please ensure that you have enabled the YouTube Data & Analytics APIs for your project.

CLIENT_SECRETS_FILE = anom.json_obj['client_secrets'];

# We will require read-only access to the Webmasters API.

SCOPES = anom.json_obj['scope'];
API_SERVICE_NAME = anom.json_obj['api'].keys()[0];
API_VERSION = anom.json_obj['api'][API_SERVICE_NAME];
TOKEN_FILE_NAME = anom.json_obj['token_file_name'];
METRICS_LIST = anom.json_obj['metrics_list'];
DIMENSIONS_LIST = anom.json_obj['dimensions_list'];
COLUMNS_PREFIX = anom.json_obj['columns_prefix'];
SKIP = anom.json_obj['SkipClients'];
VALIDATION = anom.json_obj['Validation'];
MAX_ROWS = anom.json_obj['MaxRows'];
UNVERIFIED = anom.json_obj['Unverified'];
SITE_LIST = [];

# Helpful message to display if the CLIENT_SECRETS_FILE is missing.

MISSING_CLIENT_SECRETS_MESSAGE = 'Missing %s' % os.path.abspath(os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_FILE));

# Get list of sites that we have access to

def get_sites(service):

  global SITE_LIST;                                                                                                           # initialize site list

  try:
    response = service.sites().list().execute();                                                                              # get list of sites from API
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  if response.has_key('siteEntry'):
    response = anom.json_dict(response);
    rows = response['siteEntry'];                                                                                             # get rows from response
  else:
    anom.Errmsg = "Invalid response to site list request: {0}".format(response);
    return False;

  for row in rows:
    if row.has_key('siteUrl'):
      if row['siteUrl'] not in SKIP:
        SITE_LIST.append(row);
    else:
      anom.Errmsg = "Row {0} is missing 'siteUrl' key {1}".format(rows.index(row)+1, row);
      return False;

  return True;

# Get a json object response to service request

def get_request(service, site, request):

  try:
    response = service.searchanalytics().query(siteUrl=site, body=request).execute();                                         # execute service request
    response = anom.json_dict(response);                                                                                      # sanitize the response
    return response;
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return None;

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);

# Read json object from unverified file

  try:
    os.stat(UNVERIFIED);
    old_sites = json.load(open(UNVERIFIED, 'rb'));
    clients = old_sites['clients'];
    anom.wrt_log("{0}".format(clients));
  except Exception as e:
    anom.wrt_log("Error {0} reading {1}".format(e, UNVERIFIED));
    clients = {};

# Get command line parameters

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdtm();
  start_tms = anom.tmsval;                                                                                                    # capture timestamp for monitoring

  anom.curdat();                                                                                                              # initialise date
  date_today = anom.datstr(Revers=True, Sep="-");                                                                             # get date as YYYY-MM-DD
  datmrk = False;
  rolling_days = anom.options.rolling_days;                                                                                   # get rolling business logic

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Date Business Rules
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. --start-date only = API pull only that date to current
# 2. --end-date only = API pull that date only
# 3. --start-date and --end-date = API pull for that date range
# 4. no dates specified = API pull from today and back for rolling number of days

  if (len(start_date) + len(end_date)) < 1:
    end_date = date_today;                                                                                                    # force end date to today
    start_date = (parse(end_date) - datetime.timedelta(days=rolling_days)).strftime("%Y-%m-%d");                              # set start date
  else:
    if len(end_date) < 1:
      end_date = date_today;
    else:
      start_date = end_date;                                                                                                  # set end date to be rolling number of days prior

  service = anom.initialize_service(TOKEN_FILE_NAME, API_SERVICE_NAME, API_VERSION, CLIENT_SECRETS_FILE, SCOPES);

  row_count = 0;
  metrics_sum_results = 0
  requests = 0;                                                                                                               # initialise request count
  volume = {};                                                                                                                # initialise data volume dictionary

  if not get_sites(service):
    return False;

# create header columns object

  METRICS = [];
  METRICS_TYPE = {};
  for metrics in METRICS_LIST:
    metric = metrics.keys()[0]
    METRICS.append(metric);
    METRICS_TYPE[metric] = anom.TYPE_CAST[metrics[metric]];

  COLUMNS_LIST = \
  [
    COLUMNS_PREFIX,
    DIMENSIONS_LIST,
    METRICS,
  ];
  COLUMNS = [];

  for c in COLUMNS_LIST:
    COLUMNS.extend(c);                                                                                                        # build header list

# Store new sites to json object

  sites = {};
  for SITE in SITE_LIST:
    sites[SITE['siteUrl']] = SITE['permissionLevel'];

  new_sites = {};
  new_sites['clients'] = sites;
  json.dump(new_sites, open(UNVERIFIED, 'wb'));

  anom.wrt_log("{0}".format(new_sites['clients']));

  requests += 1;

# loop through possible dates to extract

  for SITE in SITE_LIST:

    records = [];                                                                                                             # initialize records object
    row =  dict((fn,fn) for fn in COLUMNS);                                                                                   # build header record
    records.append(row);                                                                                                      # add to first row of records

    key = anom.NULCHR;
    site = SITE['siteUrl'];
    client = tldextract.extract(site).domain
    permission = SITE['permissionLevel'];
    if permission not in VALIDATION:
      anom.Missing.append("Webmaster API client {0} verification status is [{1}]. Please contact site administrator to get this corrected.".format(client, permission));
      continue;

# Check for first run with no unverified file

    d = start_date;

    if not clients.has_key(site):
      d = (parse(end_date) - datetime.timedelta(days=100)).strftime("%Y-%m-%d");

    if clients.has_key(site):
      if clients[site] != permission:
        d = (parse(end_date) - datetime.timedelta(days=100)).strftime("%Y-%m-%d");

    request = \
    {
      'startDate': d,
      'endDate': end_date,
      'dimensions': ['date']
    };                                                                                                                        # build request object

    response = get_request(service, site, request);                                                                           # execute API request

    requests += 1;

# build list of valid dates for this site

    dat_list = [];                                                                                                            # initialize date list for this site
    if response.has_key('rows'):
      for row in response['rows']:
        dat_list.append(row['keys'][0]);

    anom.wrt_log("Valid dates for site {0} with permission level {1} are [{2} to {3}]".format(site, permission, dat_list[0], dat_list[-1]));

    if row_count > 0:
      metrics_sum_results += row_count;
      row_count = 0;                                                                                                          # initialize counter

    metrics_file = '%s_%s_%s_%s%s' % (f_list[0], client, dat_list[0], dat_list[-1], anom.CSVEXT);                             # set filename for client

    for d in dat_list:

      request = \
      {
        'startDate': d,
        'endDate': d,
        'dimensions': DIMENSIONS_LIST,
        'rowLimit': MAX_ROWS
      };                                                                                                                      # build request object

      response = get_request(service, site, request);                                                                         # execute API request

      requests += 1;
      if response == None: return False;
      if not response.has_key('rows'):
        continue;
      rows = response['rows'];                                                                                                # get list of rows
      aggregation = response['responseAggregationType'];                                                                      # get aggregation type
      row_count += len(rows);                                                                                                 # increment row counter
      if len(rows) >= MAX_ROWS:
        msg = "Maximum rows limit of {0} reached for {1} on date {2}".format(MAX_ROWS, site, d)
        anom.Missing.append(msg);
        anom.wrt_log(msg);
      key = '%s%s' % (API_SERVICE_NAME, client);
      if not volume.has_key(key):
        volume[key] = [len(str(response)), '%s_%s' % (API_SERVICE_NAME, client), metrics_file, 0, 0];                         # capture data size for monitoring
      else:
        volume[key][0] += len(str(response));

      anom.wrt_log("{0} records extracted for {1} date {2}".format(len(rows), client, d));

# create list of records for file write

      for row in rows:
        record = {};
        record[COLUMNS_PREFIX[0]] = client;
        for i in range(0, len(DIMENSIONS_LIST)):
          record[DIMENSIONS_LIST[i]] = anom.nonasc(row['keys'][i]);                                                           # clean non ascii
        for i in METRICS:
          record[i] = METRICS_TYPE[i](row[i]);
        records.append(record);

    if len(key) > 0:
      volume[key][3] += row_count;
      volume[key][4] = metrics_sum_results;

    metrics_sum_results = 0;

    if (len(records) > 1):
      with open(metrics_file, 'wb') as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS);                                                                    # create a dictionary for output rows
        writer.writerows(records);
        anom.wrt_log('Created %s with %d records' % (metrics_file, len(records)-1));

  if (requests > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Requests', requests);
  if (len(volume) > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Data', volume);

  return True;
