#!/usr/bin/python
# Extract historical youtube data
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Redhat old Crypto warning bypass
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from Crypto.pct_warnings import PowmInsecureWarning;
import warnings;
warnings.simplefilter("ignore", PowmInsecureWarning);

from datetime import datetime, timedelta;
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

CLIENT_SECRETS_FILE = "client_secret_204621350102-anom-com-au-youtube.apps.googleusercontent.com.json"

# We will require read-only access to the YouTube Data and Analytics API.

YOUTUBE_SCOPES = "https://www.googleapis.com/auth/youtube.readonly https://www.googleapis.com/auth/yt-analytics.readonly https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_ANALYTICS_API_SERVICE_NAME = "youtubeAnalytics"
YOUTUBE_ANALYTICS_API_VERSION = "v1"
TOKEN_FILE_NAME = 'client_secret_%s.dat' % anom.WILDCARD;
TOKEN_FILE_LIST = [];
YOUTUBE_VIDEO_URL = 'http://www.youtube.com/watch?v=%s';

# Helpful message to display if the CLIENT_SECRETS_FILE is missing.

MISSING_CLIENT_SECRETS_MESSAGE = 'Missing %s' % os.path.abspath(os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_FILE));

metrics_list = \
[
  'views',
  'comments',
  'favoritesAdded',
  'favoritesRemoved',
  'likes',
  'dislikes',
  'shares',
  'averageViewDuration',
  'estimatedMinutesWatched',
  'subscribersGained',
  'subscribersLost',
];

dimensions_list = \
[
  'video',
];

COLUMNS_PREFIX = \
[
  'artist',
  'channel',
  'date',
  'rank',
  'title',
];

CHANNEL_HEADER = \
[
  'date',
  'id',
  'title',
  'publishedat',
  'viewcount',
  'commentcount',
  'subscribercount',
  'videocount',
];

ARTIST_LIST = \
[
  'timomatic',
  'guysebastian',
  'jackieonassis',
  'humannature',
  'gangofyouths',
  'stanwalker',
  'justicecrew',
  'nathanielwillemse',
  'littleseamusic',
  'tonightalive',
  'taylorhenderson',
  'jessicamauboy',
  'johnfarnham',
  'deltagoodrem',
  'samanthajade',
  'jaiwaetford',
  'bonnieanderson',
  'damiim',
];

OFFICIAL_ARTIST_DICT = \
{
  '1975'             : ['1975'                                , 485, 0, 0, 0, '', '', ''],
  'a$aprocky'        : ['A$AP Rocky'                          , 479, 0, 0, 0, '', '', ''],
  'acdc'             : ['ACDC'                                , 449, 0, 0, 0, '', '', ''],
  'bonnieanderson'   : ['Bonnie Anderson'                     , 335, 0, 0, 0, '', '', ''],
  'damiim'           : ['Dami Im'                             , 305, 0, 0, 0, '', '', ''],
  'deanray'          : ['Dean Ray'                            , 341, 0, 0, 0, '', '', ''],
  'deltagoodrem'     : ['Delta Goodrem'                       , 401, 0, 0, 0, '', '', ''],
  'drapht'           : ['Drapht'                              , 347, 0, 0, 0, '', '', ''],
  'gangofyouths'     : ['Gang of Youths'                      , 389, 0, 0, 0, '', '', ''],
  'georgeezra'       : ['George Ezra'                         , 527, 0, 0, 0, '', '', ''],
  'grl'              : ['GRL'                                 , 491, 0, 0, 0, '', '', ''],
  'guysebastian'     : ['Guy Sebastian'                       , 269, 0, 0, 0, '', '', ''],
  'hilaryduff'       : ['Hilary Duff'                         , 473, 0, 0, 0, '', '', ''],
  'hozier'           : ['Hozier'                              , 431, 0, 0, 0, '', '', ''],
  'humannature'      : ['Human Nature'                        , 419, 0, 0, 0, '', '', ''],
  'jackieonassis'    : ['Jackie Onassis'                      , 317, 0, 0, 0, '', '', ''],
  'jaiwaetford'      : ['Jai Waetford'                        , 413, 0, 0, 0, '', '', ''],
  'jamiefoxx'        : ['Jamie Foxx'                          , 509, 0, 0, 0, '', '', ''],
  'jasonaldean'      : ['Jason Aldean'                        , 425, 0, 0, 0, '', '', ''],
  'jessicamauboy'    : ['Jessica Mauboy'                      , 377, 0, 0, 0, '', '', ''],
  'johnfarnham'      : ['John Farnham and Olivia Newton-John' , 395, 0, 0, 0, '', '', ''],
  'joshpyke'         : ['Josh Pyke'                           , 311, 0, 0, 0, '', '', ''],
  'justicecrew'      : ['Justice Crew'                        , 371, 0, 0, 0, '', '', ''],
  'kellyclarkson'    : ['Kelly Clarkson'                      , 437, 0, 0, 0, '', '', ''],
  'leonbridges'      : ['Leon Bridges'                        , 503, 0, 0, 0, '', '', ''],
  'littleseamusic'   : ['Little Sea'                          , 299, 0, 0, 0, '', '', ''],
  'mariahcarey'      : ['Mariah Carey'                        , 455, 0, 0, 0, '', '', ''],
  'markronson'       : ['Mark Ronson'                         , 497, 0, 0, 0, '', '', ''],
  'marlisa'          : ['Marlisa'                             , 359, 0, 0, 0, '', '', ''],
  'meghantrainor'    : ['Meghan Trainor'                      , 443, 0, 0, 0, '', '', ''],
  'nathanielwillemse': ['Nathaniel'                           , 293, 0, 0, 0, '', '', ''],
  'onedirection'     : ['One Direction'                       , 281, 0, 0, 0, '', '', ''],
  'pekingduk'        : ['Peking Duk'                          , 383, 0, 0, 0, '', '', ''],
  'pitbull'          : ['Pitbull'                             , 467, 0, 0, 0, '', '', ''],
  'reigan'           : ['Reigan'                              , 353, 0, 0, 0, '', '', ''],
  'samanthajade'     : ['Samantha Jade'                       , 329, 0, 0, 0, '', '', ''],
  'stanwalker'       : ['Stan Walker'                         , 287, 0, 0, 0, '', '', ''],
  'taylorhenderson'  : ['Taylor Henderson'                    , 323, 0, 0, 0, '', '', ''],
  'thescript'        : ['The Script'                          , 515, 0, 0, 0, '', '', ''],
  'theveronicas'     : ['The Veronicas'                       , 275, 0, 0, 0, '', '', ''],
  'timomatic'        : ['Tim Omaji'                           , 365, 0, 0, 0, '', '', ''],
  'tonightalive'     : ['Tonight Alive'                       , 407, 0, 0, 0, '', '', ''],
  'tovestyrke'       : ['Tove Styrke'                         , 461, 0, 0, 0, '', '', ''],
  'usher'            : ['Usher'                               , 521, 0, 0, 0, '', '', '']
};

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);

  title_dict = {};                                                                                                            # initialise title dictionary

# Get command line parameters

  no_download = anom.options.setup;                                                                                           # just get credentials

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdat();

  if end_date == '': end_date = (anom.datval - timedelta(days=1)).strftime("%Y-%m-%d");
  if start_date == '': start_date = end_date;

  if no_download:
    dat_list = ['',];
  else:
    dat_list = anom.datlst(start_date, end_date, s='-');
    if dat_list == None:
      return False;

# Sort dates in reverse to allow easy calculation of daily snapshot for channel table

  dat_list.sort(reverse=True);

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
        if (d < 1):
          channel_id                    = channel["id"];
          channel_title                 = channel["snippet"]["title"];
          channel_publishedat           = channel["snippet"]["publishedAt"];
          channel_viewcount             = int(channel["statistics"]["viewCount"]);
          channel_commentcount          = int(channel["statistics"]["commentCount"]);
          channel_subscribercount       = int(channel["statistics"]["subscriberCount"]);
          OFFICIAL_ARTIST_DICT[artist][2] = channel_viewcount;
          OFFICIAL_ARTIST_DICT[artist][3] = channel_commentcount;
          OFFICIAL_ARTIST_DICT[artist][4] = channel_subscribercount;
          OFFICIAL_ARTIST_DICT[artist][5] = channel_id;
          OFFICIAL_ARTIST_DICT[artist][6] = channel_title;

# Convert ISO 8601 datetime stamp

          isodate = anom.dtmdtm(parse(channel_publishedat));
          channel_publishedat = anom.datstr(Revers=True, Sep="-");

          OFFICIAL_ARTIST_DICT[artist][7] = channel_publishedat;

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
          ids="channel==%s" % OFFICIAL_ARTIST_DICT[artist][5],
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
          OFFICIAL_ARTIST_DICT[artist][2] -= row[6];
          OFFICIAL_ARTIST_DICT[artist][3] -= row[7];
          OFFICIAL_ARTIST_DICT[artist][4] += (-row[15] + row[16]);
          rows.append(row);

# Store channel data

          channel_viewcount -= OFFICIAL_ARTIST_DICT[artist][2];
          channel_commentcount -= OFFICIAL_ARTIST_DICT[artist][3];
          channel_subscribercount += OFFICIAL_ARTIST_DICT[artist][4];

# Store video count watched for this date

        channel_videocount = rank - 1;

        channel_row.append(dat_list[d]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][5]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][6]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][7]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][2]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][3]);
        channel_row.append(OFFICIAL_ARTIST_DICT[artist][4]);
        channel_row.append(channel_videocount);
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

  return True;
