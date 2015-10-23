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

from apiclient.discovery import build;
from oauth2client.file import Storage;
from oauth2client.client import flow_from_clientsecrets;
from oauth2client.tools import run;

# CLIENT_SECRETS_FILE, name of a file containing the OAuth 2.0 information for
# this application, including client_id and client_secret. You can acquire an
# ID/secret pair from the API Access tab on the Google APIs Console
#   http://code.google.com/apis/console#access
# For more information about using OAuth2 to access Google APIs, please visit:
#   https://developers.google.com/accounts/docs/OAuth2
# For more information about the client_secrets.json file format, please visit:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
# Please ensure that you have enabled the YouTube Data & Analytics APIs for your project.

CLIENT_SECRETS_FILE = anom.json_obj["client_secrets"];

# We will require read-only access to the YouTube Data and Analytics API.

YOUTUBE_SCOPES = anom.json_obj["scope"];
API = anom.json_obj["api"];
YOUTUBE_API_SERVICE_NAME = API[0].keys()[0];
YOUTUBE_API_VERSION = API[0][YOUTUBE_API_SERVICE_NAME];
YOUTUBE_ANALYTICS_API_SERVICE_NAME = API[1].keys()[0];
YOUTUBE_ANALYTICS_API_VERSION = API[1][YOUTUBE_ANALYTICS_API_SERVICE_NAME];
TOKEN_FILE_NAME = anom.json_obj["token_file_name"] % anom.WILDCARD;
TOKEN_FILE_LIST = [];
YOUTUBE_VIDEO_URL = anom.json_obj["youtube_video"];

# Helpful message to display if the CLIENT_SECRETS_FILE is missing.

MISSING_CLIENT_SECRETS_MESSAGE = 'Missing %s' % os.path.abspath(os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_FILE));

metrics_list = anom.json_obj["metrics_list"];
dimensions_list = anom.json_obj["dimensions_list"];
COLUMNS_PREFIX = anom.json_obj["columns_prefix"];
CHANNEL_HEADER = anom.json_obj["channel_header"];
ARTIST_LIST = anom.json_obj["artist_list"];
OFFICIAL_ARTIST_DICT = anom.json_obj["official_artist_dict"];

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);
  service_msg = 'Youtube service {0}, version {1}';
  anom.wrt_log(service_msg.format(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION));
  anom.wrt_log(service_msg.format(YOUTUBE_ANALYTICS_API_SERVICE_NAME, YOUTUBE_ANALYTICS_API_VERSION));

  title_dict = {};                                                                                                            # initialise title dictionary

# Get command line parameters

  no_download = anom.options.setup;                                                                                           # just get credentials

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdat();                                                                                                                # initialise date
  date_today = anom.datstr(Revers=True, Sep="");                                                                                # get date as YYYYMMDD
  dat_list = ['',];
  datmrk = False;

  if end_date == '': end_date = date_today;

# Check for date marker file

  if (not no_download):
    if len(start_date) < 1:
      Date_marker = glob.glob('%s%s' % (anom.WILDCARD, anom.MRKEXT));
      start_date = os.path.splitext(os.path.basename(Date_marker[0]))[0];
      dat_list = anom.datlst(start_date, date_today, s='-');
      datmrk = True;
    else:
      dat_list = anom.datlst(start_date, end_date, s='-');
      if dat_list == None:
        anom.Errmsg = "No dates could be calculated in extract function";
        return False;

  flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, message=MISSING_CLIENT_SECRETS_MESSAGE, scope=YOUTUBE_SCOPES);

  TOKEN_FILE_LIST = glob.glob(TOKEN_FILE_NAME);
  TOKEN_FILE_LIST.sort();

  anom.wrt_log(str(dat_list));

  for d in range(0, len(dat_list)):

    header = COLUMNS_PREFIX;
    rows = [];
    channel_rows = [];
    channel_rows.append(CHANNEL_HEADER);

    anom.wrt_log("Analytics Data for Date: %s " % dat_list[d]);

    for i in range(0, len(TOKEN_FILE_LIST)):

      artist = os.path.splitext(TOKEN_FILE_LIST[i])[0].replace('client_secret_','');
      storage = Storage(TOKEN_FILE_LIST[i]);
      credentials = storage.get();
      httpclient = httplib2.Http(disable_ssl_certificate_validation=True)
      if credentials is None or credentials.invalid:
        credentials = run(flow, storage, httpclient);

      http = credentials.authorize(httplib2.Http());
      youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=http);
      youtube_analytics = build(YOUTUBE_ANALYTICS_API_SERVICE_NAME, YOUTUBE_ANALYTICS_API_VERSION, http=http);

      channels_response = youtube.channels().list(mine=True, part="id,snippet,statistics").execute();

      if no_download: continue;                                                                                               # if in windows for credential updates

      for channel in channels_response.get("items", []):
        channel_row = [];
        channel_id                    = channel["id"];
        channel_title                 = channel["snippet"]["title"];
        channel_publishedat           = channel["snippet"]["publishedAt"];
        channel_viewcount             = int(channel["statistics"]["viewCount"]);
        channel_commentcount          = int(channel["statistics"]["commentCount"]);
        channel_subscribercount       = int(channel["statistics"]["subscriberCount"]);
        channel_videocount            = int(channel["statistics"]["videoCount"]);

# Convert ISO 8601 datetime stamp

        isodate = anom.dtmdtm(parse(channel_publishedat));
        channel_publishedat = anom.datstr(Revers=True, Sep="-");

# Store channel data

        channel_row.append(dat_list[d]);
        channel_row.append(channel_id);
        channel_row.append(channel_title);
        channel_row.append(channel_publishedat);
        channel_row.append(channel_viewcount);
        channel_row.append(channel_commentcount);
        channel_row.append(channel_subscribercount);

# Set options from args

        if len(anom.options.metrics) < 1:
          mets = ('%s,' * len(metrics_list)).rstrip(',') % tuple(metrics_list);
        else:
          mets = anom.options.metrics;

        if len(anom.options.dimensions) < 1:
          dimens = ('%s,' * len(dimensions_list)).rstrip(',') % tuple(dimensions_list);
        else:
          dimens = anom.options.dimensions;

# Request report

        analytics_response = youtube_analytics.reports().query(
          ids="channel==%s" % channel_id,
          metrics=mets,
          dimensions=dimens,
          start_date=dat_list[d],
          end_date=dat_list[d],
          start_index=anom.options.start_index,
          max_results=anom.options.max_results,
          filters=anom.options.filters,
          sort=anom.options.sort
        ).execute();

        if i < 1:
          arh = analytics_response.get("columnHeaders", []);
          types = [];
          for column_header in arh:
            types.append(column_header["dataType"]);
            header.append(column_header["name"]);
          rows.append(header);

        arr = analytics_response.get("rows", []);
        rank = 1;
        for row in arr:
          for el in range(0, len(row)): row[el] = anom.TYPE_CAST[types[el]](row[el]);                                         # force type cast on row element
          row.insert(0, OFFICIAL_ARTIST_DICT[artist][0]);
          row.insert(1, channel_id);
          row.insert(2, dat_list[d]);
          row.insert(3, rank);
          rank += 1;
          video = row[4];
          if (not title_dict.has_key(video)):
            try:
              response = urllib.urlopen("%s" % (YOUTUBE_VIDEO_URL % video));
              title_search = etree.HTML(response.read());                                                                     # get json object
              title = title_search.xpath("//span[@id='eow-title']/@title");
              title_dict[video] = title;
              response.close();
            except Exception as e:
              title = 'Unable to locate Video Title';
              anom.wrt_log(e);
          else:
            title = title_dict[video];
          row.insert(4,''.join(title));
          rows.append(row);

# store videos watched for this date

        channel_row.append(rank - 1);
        channel_rows.append(channel_row);

    if no_download:
      pass;
    else:
      if (len(rows) > 1):
        out_fil = 'Sony_YouTube_Analytics_%s.csv' % dat_list[d];
        with open(out_fil, 'wb') as outfil:
          writer = csv.writer(outfil);
          writer.writerows(rows);
          anom.wrt_log('Created %s' % out_fil);
        out_fil = 'Sony_YouTube_Channels_%s.csv' % dat_list[d];
        with open(out_fil, 'wb') as outfil:
          writer = csv.writer(outfil);
          writer.writerows(channel_rows);
          anom.wrt_log('Created %s' % out_fil);
        if datmrk:
          f_del = Date_marker[0];
          os.remove(f_del);
          f_date = (parse(dat_list[d])+datetime.timedelta(1)).strftime('%Y%m%d');
          Date_marker = ['%s%s' % (f_date, anom.MRKEXT)];
          open(Date_marker[0], "wb").close();
          anom.wrt_log('Date markers changed from [%s] to [%s]' % (os.path.split(f_del)[1], os.path.split(Date_marker[0])[1]));
      else:
        anom.wrt_log('No data found for %s' % (dat_list[d]));
        return True;

  return True;
