import httplib2;
from apiclient.discovery import build;
from oauth2client import GOOGLE_TOKEN_URI;
from oauth2client.client import OAuth2Credentials;
import time;
import datetime;
import json;
import os;
import jinja2;
import inspect;

agencyID_List = {'INI':'20700000000000476','UM':'20700000000001281'};
agencyID_List = anom.json_obj['AgencyIDList']


# Set date variables
anom.curdat();        
SRC_DIR = '%s/' % os.environ['WORKSPACE'];                                                                                                        # initialise date
DAT_DIR = anom.datstr(Revers=True, Sep="");                                                                                   # get date as YYYYMMDD for directory
DST_DIR = '%s/%s/' % (anom.CURDIR, DAT_DIR);                                                                                   # build directory path


def create_credentials(client_id, client_secret, refresh_token):
  """Create Google OAuth2 credentials.

  Args:
    client_id: Client id of a Google Cloud console project.
    client_secret: Client secret of a Google Cloud console project.
    refresh_token: A refresh token authorizing the Google Cloud console project
      to access the DS data of some Google user.

  Returns:
    OAuth2Credentials
  """
  return OAuth2Credentials(access_token=None, client_id=client_id, client_secret=client_secret, refresh_token=refresh_token, token_expiry=None, token_uri=GOOGLE_TOKEN_URI, user_agent=None);

def get_service(credentials):
  """Set up a new DoubleClick Search service.

  Args:
    credentials: An OAuth2Credentials generated with create_credentials, or
    flows in the oatuh2client.client package.
  Returns:
    An authorized Doubleclicksearch serivce.
  """
  # Use the authorize() function of OAuth2Credentials to apply necessary credential
  # headers to all requests.
  http = credentials.authorize(http = httplib2.Http());

  # Construct the service object for the interacting with the DoubleClick Search API.
  service = build('doubleclicksearch', 'v2', http=http);
  return service

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Request sample report and print the report ID that DS returns.
# service = OAuth2 returned service opbject
# template = json object with report criteria
# Returns the report id.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def request_report(service, template):

  try:
    request = service.reports().request(body=template);
    json_data = request.execute();
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return None;

  return json_data['id'];

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Generate report.
# service = OAuth2 returned service opbject
# Returns the report id.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def generate_report(service,json_str):

  try:
    request = service.reports().generate(body=json_str);
    result = request.execute();
  except Exception as e:
    print e
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return None;

  return result;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Poll the reporting API with the reportId until the report is ready.
# Generate report.
# service = OAuth2 returned service opbject
# report_id = the report ID
# Returns the report id.
# For large reports, DS automatically fragments the report into multiple
# files. The 'files' property in the JSON object that DS returns contains
# the list of URLs for file fragment. To download a report, DS needs to
# know the report ID and the index of a file fragment.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def poll_report(service, report_id, filename):

  try:
    request = service.reports().get(reportId=report_id);
    json_data = request.execute()
    while not json_data['isReportReady']:
      anom.wrt_log("Report isn't ready. I'll try again.");
      time.sleep(10);
      json_data = request.execute();
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  #anom.wrt_log("The report is ready.");
  for i in range(len(json_data['files'])):
    anom.wrt_log('Downloading fragment {0} for report {1}'.format(i, report_id))
    download_files(service, report_id, filename, str(i))                                              # See Download the report.

  return True;


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download the report file(s)
# service = OAuth2 returned service opbject
# report_id = the report ID
# filename = filename prefix to save the report to
# report_fragment = fragment index starting at 0
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def download_files(service, report_id, filename, report_fragment):

  try:
    with open(filename % (DST_DIR, report_fragment, anom.CSVEXT), 'wb') as f:
      print DST_DIR
      request = service.reports().getFile(reportId=report_id, reportFragment=report_fragment);
      f.write(request.execute());
  except Exception as e:
    print e
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  return True;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Main
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def main(startDate,endDate):

  client_id = "108764525551-mtb75g6nsdrfqapcjug253pp5g1mpat7.apps.googleusercontent.com";
  client_secret = "itnY8a83s-6BY9R-iuyVX6vb";
  refresh_token = "1/7RQ5YvVb_-lMwkmfsV12L2v7YEZgaV1P6tcf8RqYndM";
  template_dir = os.path.join(os.path.dirname(__file__), 'json_api_templates');
  jinja_env = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir), autoescape = True);
  t = jinja_env.get_template('reports.json');
  agency_template = jinja_env.get_template('id.json');

  reports = \
  {
    'campaign': ['account','accountType','advertiser','agency','avgPos','campaign','clicks','cost','date','engineStatus','impr','searchImpressionShare','status','deviceSegment'],
    'conversion': ['account','accountType','adGroup','advertiser','agency','campaign','conversionAttributionType','conversionDate','conversionRevenue','conversionTimestamp','conversionType','deviceSegment','floodlightActivity','floodlightActivityTag','floodlightGroup','floodlightGroupId','floodlightGroupTag','floodlightGroupConversionType','floodlightOriginalRevenue','status']
  };

  creds = create_credentials(client_id, client_secret, refresh_token);
  service = get_service(creds);

  try:
    os.stat(DST_DIR);
  except:
    try:
      os.mkdir('%s' % (DST_DIR));
    except OSError as e:
      anom.Errmsg = "{0}: {1} {2} {3} function:{4} module:{5}".format(type(e).__name__, e.errno, e.strerror, e.filename,inspect.stack()[0][3], __name__);
      anom.err_exit(1);
  processes = {};
  print "start date is : " + startDate
  num_report = 0;
  anom.wrt_log("Starts triggering report at %s" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
  for id, agency in agencyID_List.iteritems():
    json_str = json.loads(agency_template.render(agencyId=agency));
    agency_ad_list = {}
    if anom.json_obj['AdvertiserIDList'].get(id) != None:
      agency_ad_list['rows'] = [];
      for adid in anom.json_obj['AdvertiserIDList'].get(id):
        agency_ad_list['rows'].append({'agencyId':agency,'advertiserId':adid})
    else:
      agency_ad_list = generate_report(service,json_str);
    print agency_ad_list
    for r in agency_ad_list['rows']:
      for key, value in reports.iteritems():
        template = json.loads(t.render(startDate=startDate, endDate=endDate, agencyId=r['agencyId'], advertiserId=r['advertiserId'], reportType=key, columns=value));
        rep_id = request_report(service, template);
        anom.wrt_log("Triggered report %s" % rep_id)
        #time.sleep(5);
        counter = 1;
        while rep_id == None and counter < 6:
          anom.wrt_log("Report id returns None after trying %s times, retrying in 5 seconds!" % counter);
          time.sleep(5);
          rep_id = request_report(service, template);
          if rep_id != None:
            break;
          counter += 1;
        if rep_id == None:
          anom.wrt_log("Report id returns None after 5 times trying, exiting!");
          return False;
        filename = "%sDoubleClick_%s_%s_%s_%s_%s_%s" % ('%s', key.title(), id, r['advertiserId'], startDate, endDate, '%s%s')
        processes[rep_id] = filename
        num_report += 1
  i=1
  for key, value in processes.iteritems():
    anom.wrt_log("Downloading report %s, %d out of %d" % (key, i, num_report))
    poll_report(service, key, value)
    i += 1
  anom.wrt_log("Finished downloading report at %s" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
  return True;

if __name__ == '__main__': main(anom.options.start_date,anom.options.end_date);

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of API Extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
