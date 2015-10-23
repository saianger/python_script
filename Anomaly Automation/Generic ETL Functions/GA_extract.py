from Crypto.pct_warnings import PowmInsecureWarning;
import warnings;
warnings.simplefilter("ignore", PowmInsecureWarning);

from datetime import datetime, timedelta;
from dateutil.parser import *;
from apiclient.errors import HttpError
import csv;
import urllib;
import inspect;                                                                                                               # inspect module for errors
import random
import time

pattern = 'ga:%s,';                                                                                                           # set pattern for url parameters

def get_accounts_ids(service):
    accounts = service.management().accounts().list().execute()
    ids = []
    if accounts.get('items'):
        for account in accounts['items']:
            ids.append(account['id'])
    return ids

def get_webproperty_ids(service, account_id):
    webproperties = service.management().webproperties().list(accountId=account_id).execute();
    ids = []
    for properties in webproperties['items']:
      ids.append(properties['id']);
    return ids;

def build_url_params(params, pattern):

    params_list = [];

# force into a list

    if (type(params) in [str, unicode]):
      params_list.append(str(params));
    else:
      if (type(params) == list):
        params_list = params;

    url_params = (pattern * len(params_list)).rstrip(',') % tuple(params_list);                                               # build string of parameters

    if len(url_params) < 1:
      return None;
    else:
      return url_params;

def get_source_group(service, profile_id, start_date, end_date, filters, dimensions, metrics, max_results, start_index):

    metrics_list = [];                                                                                                        # initialise list

    if (type(metrics) == str):
      metrics_list.append(metrics);                                                                                           # put single metric into list
    else:
      if (type(metrics) == list):
        metrics_list = metrics;                                                                                               # assign list

    ids = build_url_params(profile_id, pattern);
    ga_filters = build_url_params(filters, pattern);
    ga_dimensions = build_url_params(dimensions, pattern);
    ga_metrics = build_url_params(metrics, pattern);

    for n in range (0, 5):
      try:
        data = service.data().ga().get(ids=ids, max_results=max_results, start_index=start_index, start_date=start_date, end_date=end_date, filters=ga_filters, metrics=ga_metrics, dimensions=ga_dimensions).execute();
        return data;
      except HttpError as e:
        if 'User Rate Limit Exceeded' in str(e):
          time.sleep((2 ** n) + random.random());
          anom.wrt_log('Rate limiting %d' % n);
        elif 'HttpError 400' in str(e):
          time.sleep((10 ** n) + random.random());
          anom.wrt_log('Error 400 recovery %d' % n);
        elif 'HttpError 503' in str(e):
          time.sleep((10 ** n) + random.random());
          anom.wrt_log('Error 503 recovery %d' % n);
        elif 'HttpError 500' in str(e):
          time.sleep((10 ** n) + random.random());
          anom.wrt_log('Error 500 recovery %d' % n);
        else:
          anom.Errmsg = "{0}: {1} function: {2} module: {3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
          return None;
    anom.Errmsg = "Timeout in Function: {0} Module: {1}".format(inspect.stack()[0][3], __name__);
    return None;

def extract(f_list, d_list, jobname):

# Set time now

  anom.curdtm();
  start_tms = anom.tmsval;                                                                                                    # capture timestamp for monitoring
  anom.curdat();

# Get dates from CLI

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;
  rolling_days = anom.options.rolling_days;

  if end_date == '': end_date = (anom.datval - timedelta(days=1)).strftime("%Y-%m-%d");                                       # yesterday
  if start_date == '': start_date = (anom.datval - timedelta(days=rolling_days)).strftime("%Y-%m-%d");                        # yesterday - rolling days

  date_list = anom.datlst(start_date, end_date, s='-');
  if date_list == None:
    return False;

  anom.wrt_log("%s to %s" % (date_list[0], date_list[-1]));

# Get JSON object for authorization, account definitions and dimensions

  HTTPRETRY = 5;                                                                                                              # inititalise http retry count
  if anom.json_obj.has_key('HttpRetry'):
    HTTPRETRY = anom.json_obj['HttpRetry'];                                                                                   # get from json input file if set
  GRANULAR_DATE = True;                                                                                                       # initialise granular date flag
  if anom.json_obj.has_key('Granular_Date'):
    GRANULAR_DATE = anom.bol(anom.json_obj['Granular_Date']);                                                                 # get from json input file if set
  INPUT_PARAMS = anom.json_obj['input_params'];
  ACCOUNT_LIST = INPUT_PARAMS['account_list'];
  if INPUT_PARAMS.has_key('filters'):
    FILTERS = INPUT_PARAMS['filters'];
  else:
    FILTERS = [];
  DIMENSIONS = INPUT_PARAMS['dimensions'];
  METRICS = INPUT_PARAMS['metrics'];
  if len(METRICS) < 1:
    anom.Errmsg = 'No Metrics specified. Please correct prior to re-running this process';
    return False;
  if INPUT_PARAMS.has_key('conversions'):
    CONVERSIONS = INPUT_PARAMS['conversions'];
  else:
    CONVERSIONS = [];
  CLIENT_SECRETS = anom.json_obj['client_secrets'];
  TOKEN_FILE_NAME = anom.json_obj['token_file_name'];
  SCOPE = anom.json_obj['scope'];
  if (len(anom.json_obj['api']) == 1):
    API = str(anom.json_obj['api'].keys()[0]);
    API_VER = anom.json_obj['api'][API];
  else:
    anom.Errmsg = "%s API object is incorrect. Please correct prior to re-running this process" % anom.options.json_file;
    return False;

# get fully expanded conversions list

  conversions_list = [];

  for conversion in CONVERSIONS:
    if '%d' in conversion:
      for i in range(0, 20):
        conversions_list.append(str(conversion % (i+1)));
    else:
      conversions_list.append(str(conversion));

  anom.wrt_log("Start Date: %s End Date: %s Account list %s Dimensions: %s Metrics: %s Conversions: %s Scope %s" % (start_date, end_date, str(ACCOUNT_LIST), str(DIMENSIONS), str(METRICS), str(conversions_list), SCOPE));
  max_results = 10000;

# Create an authorized service object

  service = anom.initialize_service(TOKEN_FILE_NAME, API, API_VER, CLIENT_SECRETS, SCOPE);

  requests = 0;                                                                                                               # initialise request count
  volume = {};                                                                                                                # initialise data volume dictionary

# Get list of authorised accounts

  account_ids = get_accounts_ids(service);

  requests += 1

  date_start = date_list[0];
  date_end = date_list[-1];

# loop through date range

  if not GRANULAR_DATE: date_list = [date_end];

  for date in date_list:

    if GRANULAR_DATE: date_start = date_end = date;

    anom.wrt_log("Date pulled: %s to %s" % (date_start, date_end));
    timestamp = anom.fildtm();
    metrics_file = '%s%s_%s%s' % (f_list[0], date_start, date_end, anom.CSVEXT);
    if len(conversions_list) > 0:
      conversions_file = '%s%s_%s%s' % (f_list[1], date_start, date_end, anom.CSVEXT);
    else:
      conversions_file = '';

# Build header record

    metrics_header = ['date','profile', 'weburl', 'property_id', 'profile_id'];
    if not GRANULAR_DATE: metrics_header.insert(0, 'from_date');
    for i in DIMENSIONS:
      if (i not in metrics_header):
        if i != anom.NULCHR:
          metrics_header.append(i);                                                                                           # only add dimension if not pre-defined
      else:
        DIMENSIONS.remove(i);                                                                                                 # remove from dimensions list and report warning
        anom.wrt_log('WARNING: [\'%s\'] is already defined, check <%s> dimensions object' % (str(i), str(anom.options.json_file)));
      if i == anom.NULCHR:
        DIMENSIONS.remove(i);                                                                                                 # remove null character from dimensions list
        anom.wrt_log('Removed blank dimension');
    conversions_header = []
    conversions_header.extend(metrics_header);
    for i in METRICS: metrics_header.append(i);
    conversions_header.extend(['key', 'value']);
    metrics_rows = [];
    conversions_rows = [];
    metrics_rows.append(metrics_header);
    conversions_rows.append(conversions_header);

# Initialise results counter

    metrics_sum_results = 0;
    conversions_sum_results = 0;

# Loop through all accounts

    for account_id in account_ids:
      try:

# Only accounts that are in json object

        if (ACCOUNT_LIST.has_key(account_id)):
          anom.wrt_log("Account ID: %s" % account_id);
          web_ids = get_webproperty_ids(service, account_id);                                                                 # get web ids from GA
          requests += 1;
          for webProperty_Id in web_ids:

# Only web properties from json object

            if (ACCOUNT_LIST[account_id][0].has_key(webProperty_Id)):
              anom.wrt_log("Web Property ID: %s" % webProperty_Id);
              profiles = service.management().profiles().list(accountId=account_id, webPropertyId=webProperty_Id).execute();  # get profiles from GA
              requests += 1;
              for profile in profiles['items']:
                profile_id = profile['id'];

# Only profiles from json object

                if (profile_id in ACCOUNT_LIST[account_id][0][webProperty_Id]):
                  anom.wrt_log("Profile ID: %s" % profile_id);
                  name = profile['name'];                                                                                     # get profile name
                  weburl = profile['websiteUrl'];                                                                             # get web url

# Get data in json format from GA for metrics

                  total_results = max_results + 1;                                                                            # initialise total results count
                  start_index = 1;                                                                                            # initialise page index
                  while total_results - (max_results * (start_index-1)) > 0:
                    remainder = total_results - (max_results * (start_index-1));                                              # calculate number of records left
                    data = get_source_group(service, profile_id, date_start, date_end, FILTERS, DIMENSIONS, METRICS, min(max_results, remainder), ((max_results * (start_index-1)) + 1));
                    key = anom.NULCHR;
                    if data is None: return False;
                    total_results = data['totalResults'];
                    requests += 1;
                    start_index += 1;                                                                                         # increment page counter

                    if data.has_key('rows'):                                                                                  # check for empty dataset
                      for row in data['rows']:
                        row_clean = [date, name, weburl, webProperty_Id, profile_id,];                                        # begin row construction
                        if not GRANULAR_DATE: row_clean.insert(0, date_start);                                                # add start date to row if not granular date
                        for field in row:
                          row_clean.append(urllib.unquote_plus(urllib.unquote_plus(field.encode('ascii','ignore'))));         # populate row data
                        metrics_rows.append(row_clean);                                                                       # add to output dataset
                        metrics_sum_results += 1;
                    key = '%s%s' % (API, metrics_file);
                    if not volume.has_key(key):
                      volume[key] = [len(str(data)), API, metrics_file, 0, 0];                                                # capture data size for monitoring
                    else:
                      volume[key][0] += len(str(data));
                  if len(key) > 0:
                    volume[key][3] += total_results;
                    volume[key][4] = metrics_sum_results;

# Get data in json format from GA for conversion

                  for conversion in conversions_list:
                    total_results = max_results + 1;                                                                          # initialise total results count
                    start_index = 1;                                                                                          # initialise page index
                    while total_results - (max_results * (start_index-1)) > 0:
                      remainder = total_results - (max_results * (start_index-1));                                            # calculate number of records left
                      data = get_source_group(service, profile_id, date_start, date_end, FILTERS, DIMENSIONS, conversion, min(max_results, remainder), ((max_results * (start_index-1)) + 1));
                      if data is None: return False;
                      total_results = data['totalResults'];
                      requests += 1;
                      start_index += 1;                                                                                       # increment page counter

                      if data.has_key('rows'):                                                                                # check for empty dataset
                        for row in data['rows']:
                          row_clean = [date, name, weburl, webProperty_Id, profile_id,];                                      # begin row construction
                          if not GRANULAR_DATE: row_clean.insert(0, date_start);                                              # add start date to row if not granular date
                          for field in row:
                            row_clean.append(urllib.unquote_plus(urllib.unquote_plus(field.encode('ascii','ignore'))));       # populate row data
                          value = str(row_clean[-1]);
                          if anom.strdbl(value, True, True):                                                                  # check valid numeric value
                            value = anom.dblval;
                          if value != 0:
                            row_clean.insert(-1, conversion);                                                                 # add kevword prior to last list element
                            conversions_rows.append(row_clean);                                                               # add to output dataset
                            conversions_sum_results += 1;
                      key = '%s%s' % (API, conversions_file);
                      if not volume.has_key(key):
                        volume[key] = [len(str(data)), API, conversions_file, 0, 0];                                          # capture data size for monitoring
                      else:
                        volume[key][0] += len(str(data));
                    if len(key) > 0:
                      volume[key][3] += total_results;
                      volume[key][4] = conversions_sum_results;

      except Exception as e:
        anom.Errmsg = "{0}: {1} function: {2} module: {3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
        if (requests > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Requests', requests);
        if (len(volume) > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Data', volume);
        return False;
    if len(metrics_rows) > 1:
      with open(metrics_file, 'wb') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL);
        writer.writerows(metrics_rows);                                                                                       # output to file
      anom.wrt_log("Total records Retrieved: %d" % metrics_sum_results);
    if len(conversions_rows) > 1:
      with open(conversions_file, 'wb') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL);
        writer.writerows(conversions_rows);                                                                                   # output to file
      anom.wrt_log("Total records Retrieved: %d" % conversions_sum_results);
  anom.wrt_log("Total Requests from API: %d" % requests);                                                                     # log requests count
  if (requests > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Requests', requests);
  if (len(volume) > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Data', volume);
  return True;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
