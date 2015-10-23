from suds.client import Client
from suds.sax.element import Element
from suds.xsd.doctor import Import, ImportDoctor
from datetime import datetime, timedelta
import time
from lxml import etree
from urllib2 import urlopen
import csv
import pg

########## only for development - start ##############
# #import logging
# import yaml
# class AnomSys:
#     def __init__(self):
#         path = 'C:\\projects\\workspace\\sizmek_report\\src\\etl\\SizmekAPI.json'
#         with open(path) as json_file:
#             self.json_obj = yaml.safe_load(json_file)
#         self.CSVEXT = ".csv";
#         self.Errmsg = None
#     def fildtm(self):
#         Dtmstr = datetime.now().strftime('%Y-%m-%d_%H%M%S');
#         return Dtmstr;
#     def wrt_log(self, m, con=True):
#         print m
# 
# #logging.basicConfig(level=logging.INFO)
# #logging.getLogger('suds.client').setLevel(logging.DEBUG)
# 
# anom = AnomSys()
########## start building extract function ##########
######## set up/helper functions ########
def show_progress(total, current, page_index, page_size):
    print "Total No: %s, Current No: %s, page index: %s, page size: %s" % (total, current, page_index, page_size)

def show_meta_response_status(obj_name, response):
    anom.wrt_log("{0} response status {1}".format(obj_name, response[0]));

def show_meta_response_status_pgn(obj_name, response, page_index, page_size, item_list):
    anom.wrt_log("Total No: {0}, Current No: {1}, page index: {2}, page size: {3}".format(response[1]['TotalCount'], len(item_list), page_index, page_size));

def write_to_csv(item_dic, file_name, col_name, mode='wb'):
    if len(item_dic) is 1:
        with(file_name, mode) as fout:
            cout = csv.DictWriter(fout, fieldnames=col_name)
            cout.writeheader()
            cout.writerow(item_dic)
    else:    
        with open(file_name, mode) as fout:
            cout = csv.DictWriter(fout, fieldnames=col_name)
            cout.writeheader()
            cout.writerows(item_dic)

def get_wsdl_url(api_name):
    try:
        url = anom.json_obj['URL'] + anom.json_obj['Version'] + anom.json_obj['Service'][api_name] + anom.json_obj['URLTail']
    except:
        url = None
        pass
    return url

def setup_soap_client(obj_name, token=None, **kwargs):
    wsdl_url = get_wsdl_url(obj_name)
    # update WSDL (xsd)
    if kwargs.has_key('import_msg_src') and kwargs['import_msg_src']:        
        msg_import = Import('http://api.eyeblaster.com/message')
        if kwargs.has_key('import_arr_src') and kwargs['import_arr_src']:
            arr_import = Import('http://schemas.microsoft.com/2003/10/Serialization/Arrays')
            doctor = ImportDoctor(msg_import, arr_import)
        else:
            doctor = ImportDoctor(msg_import)
        client = Client(wsdl_url, doctor=doctor, faults=False)
    else:
        client = Client(wsdl_url, faults=False)    
    # update token into SOAP header
    if kwargs.has_key('add_token') and kwargs['add_token']:
        header = ('ns1', 'http://api.eyeblaster.com/message')
        element = Element('UserSecurityToken', ns=header).setText(token)
        client.set_options(soapheaders=element)
    return client

######## authentication ########
def get_token():
    username = anom.json_obj['Credentials']['username']
    password = anom.json_obj['Credentials']['password']
    appkey = anom.json_obj['Credentials']['appkey']
    client = setup_soap_client('auth', faults=False)
    response = client.service.ClientLogin(username, password, appkey)
    token = response[1] if response[0] is 200 else None
    return token

######## get meta objects ########
###### get meta responses ######
#### advertiser ####
def get_advertiser_response(obj_name, token, page_index=1, page_size=50, verbose=False, **kwargs):
    token = get_token() if token is None else token
    client = setup_soap_client(obj_name, token, **kwargs)
    # update page info
    paging = client.factory.create('ns1:ListPaging')
    paging['PageIndex'] = page_index
    paging['PageSize'] = page_size
    # update filter array - required for the api method but no filter is set
    filterArrary = client.factory.create('ns0:ArrayOfAdvertiserServiceFilter')
    # get extended info - last arg True to get extended information    
    response = client.service.GetAdvertisers(filterArrary, paging, True) # extended info = True
    if verbose:
        show_meta_response_status(obj_name, response)
    return response

#### campaign ####
def get_campaign_response(obj_name, token, page_index=1, page_size=50, verbose=False, **kwargs):
    token = get_token() if token is None else token
    client = setup_soap_client(obj_name, token, **kwargs)
    # update page info
    paging = client.factory.create('ns1:ListPaging')
    paging['PageIndex'] = page_index
    paging['PageSize'] = page_size
    # update filter - is_recent is boolean
    if kwargs.has_key('is_recent') and kwargs['is_recent']:
        item_filter = client.factory.create('ns0:CampaignRecentFilter')
        item_filter['IsRecent'] = kwargs['is_recent']
    else:
        # campaign_id is integer
        if kwargs.has_key('campaign_id') and kwargs['campaign_id'] is not None:
            item_filter = client.factory.create('ns0:CampaignIDFilter')
            item_filter['CampaignID'] = kwargs['campaign_id']
        else:
            return (0, 'missing campaign id')
    filterArray = client.factory.create('ns0:ArrayOfCampaignServiceFilter')
    filterArray.CampaignServiceFilter = [item_filter]
    # get campaign response
    response = client.service.GetCampaigns(filterArray, paging, True) # extended info = True
    if verbose:
        show_meta_response_status(obj_name, response)
    return response

# get_campaign_response test
# token = get_token()
# get_campaign_response('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
# get_campaign_response('campaign', token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=489599)

#### wrapper to get responses ####
def get_meta_response(obj_name, token, page_index, page_size, verbose=False, **kwargs):
    response = None
    token = get_token() if token is None else token
    if obj_name is 'advertiser':
        # keyword arguments to set - add_token=True, import_msg_src=True, import_arr_src=False
        response = get_advertiser_response(obj_name, token, page_index, page_size, verbose=False, **kwargs)
    elif obj_name is 'convtag':
        'get conversion tag response'
    elif obj_name is 'campaign':
        # keyword arguments to set - add_token=True, import_msg_src=True, import_arr_src=False + is_recent (boolean) or campaign_ids (integer)
        response = get_campaign_response(obj_name, token, page_index, page_size, verbose=False, **kwargs)
    elif object is 'placement':
        'get placement response'
    elif object is 'ad':
        'get ad response'
    return response

# get_meta_response test
# token = get_token()
# get_meta_response('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
# get_meta_response('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=489599)

###### get meta items ######
#### campaign ####
def get_campaign_items(response, verbose=False):
    item_list = [] if response is not None else None
    if response is not None:
        cnt = 0
        for res in response[1]['Campaigns']['CampaignInfo']:
            if type(res['ID']) == type(1):
                dt = res['CampaignExtendedInfo']['StartDate']
                start = datetime(dt['Year'], dt['Month'], dt['Day'], dt['Hour'], dt['Minute'], dt['Second'])
                time_zone_id = dt['TimeZoneID']
                dt = res['CampaignExtendedInfo']['EndDate']
                end = datetime(dt['Year'], dt['Month'], dt['Day'], dt['Hour'], dt['Minute'], dt['Second'])
                dt = res['CampaignExtendedInfo']['ActualStartDate']
                actual_start = datetime(dt['Year'], dt['Month'], dt['Day'], dt['Hour'], dt['Minute'], dt['Second']) if dt is not None else None
                item = {'id': res['ID'], 'name': res['Name'], 'accId': res['AgencyID'], 'accName': res['CampaignExtendedInfo']['AgencyName'],
                        'advId': res['AdvertiserID'], 'start': start, 'end': end, 'actualStart': actual_start, 'bookedImps': res['BookedImpressions'],
                        'stopServ': res['StopServing'], 'timeZoneId': time_zone_id, 'addedDate': datetime.now().date()}
                item_list.append(item)
            else:
                cnt += 1
    if cnt >0:
        anom.wrt_log("{0} records are ignored due to type mismatch.".format(cnt));
    return item_list

#### wrapper to get meta items ####
def get_meta_items(obj_name, response, verbose=False):
    item_list = None
    if obj_name is 'advertiser':
        'parse advertiser items'
    elif obj_name is 'convtag':
        'parse conversion tag items'
    elif obj_name is 'campaign':
        item_list = get_campaign_items(response, verbose)
    elif object is 'placement':
        'parse placement items'
    elif object is 'ad':
        'parse ad items'
    return item_list

# get_meta_items test
# token = get_token()
# response = get_meta_response('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
# response = get_meta_response('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=489599)
# get_meta_items('campaign', response, verbose=True)

###### get meta items with pagination ######
#### campaign with pagination ####
def get_campaign_items_pgn(obj_name, token, page_index, page_size, verbose=False, **kwargs):
    item_list = []
    while True:
        response = get_meta_response(obj_name, token, page_index, page_size, verbose, **kwargs)
        item_list = item_list + get_meta_items(obj_name, response, verbose)
        if verbose:
            show_meta_response_status_pgn(obj_name, response, page_index, page_size, item_list)
        if len(item_list) < response[1]['TotalCount']:
            page_index += 1
        else:
            break
    return item_list

# get_campaign_items_pgn test
# token = get_token()
# item_list = get_campaign_items_pgn('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)

#### wrapper to get meta items with pagination ####
def get_meta_items_pgn(obj_name, token, page_index, page_size, verbose=False, **kwargs):
    item_list = None
    if obj_name is 'advertiser':
        'parse advertiser items'
    elif obj_name is 'convtag':
        'parse converson tag items'
    elif obj_name is 'campaign':
        # keyword arguments to set - add_token=True, import_msg_src=True, import_arr_src=False + is_recent (boolean) or campaign_ids (integer)
        item_list = get_campaign_items_pgn(obj_name, token, page_index, page_size, verbose, **kwargs)
    elif object is 'placement':
        'parse placement items'
    elif object is 'ad':
        'parse ad items'
    return item_list

# get_meta_items_pgn test
# token = get_token()
# item_list = get_meta_items_pgn('campaign', token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)

######## get performance/conversion data ########
def get_perf_report_meta(obj_name, response):
    item = {'jobId': response['JobID'], 'uniqueName': response['UniqueName'], 'camId': response['CampaignID'], 'convAttModel': response['ConversionAttributionModel'],
            'convTagsIds': response['ConversionTagsIDsFilters'], 'ckWinClks': response['CookieWindowClicks'], 'ckWinImps': response['CookieWindowImpressions'],
            'dataLvl': response['DataLevelID'], 'ignoreAssign': response['IsIgnoreAssignmentToCampaign'], 'start': response['ReportStartDate'], 'end': response['ReporEndtDate'],
            'status': None, 'url': None, 'type': obj_name, 'addedDate': datetime.now().date()}
    return item

def initiate_job(obj_name, token, campaign_id, start, end, timezone_id, conv_tag_id=None, verbose=False, **kwargs):
    # set up client
    token = get_token() if token is None else token
    client = setup_soap_client(obj_name, token, **kwargs)
    # update request variables
    base_report = client.factory.create('ns0:ReportBase')
    perf_report = client.factory.create('ns0:PerformanceReport')
    start_datetime = client.factory.create('ns0:APIDateTime')
    endt_datetime = client.factory.create('ns0:APIDateTime')
    perf_report['CampaignID'] = campaign_id
    perf_report['DataLevelID'] = 8 # ad level, 16 if placement level
    if conv_tag_id is not None:
        perf_report['ConversionTagsIDsFilters'] = str(conv_tag_id)    
    start_datetime['Year'] = start.year
    start_datetime['Month'] = start.month
    start_datetime['Day'] = start.day
    start_datetime['Hour'] = 0
    start_datetime['Minute'] = 0
    start_datetime['Second'] = 0
    start_datetime['TimeZoneID'] = timezone_id
    perf_report['ReportStartDate'] = start_datetime    
    endt_datetime['Year'] = end.year
    endt_datetime['Month'] = end.month
    endt_datetime['Day'] = end.day
    endt_datetime['Hour'] = 0
    endt_datetime['Minute'] = 0
    endt_datetime['Second'] = 0
    endt_datetime['TimeZoneID'] = timezone_id
    perf_report['ReporEndtDate'] = endt_datetime    
    base_report = perf_report
    # initiate report job
    response = client.service.InitiateReportJob(base_report)
    report_meta = get_perf_report_meta(obj_name, response[1])
    base_report_res = [response[0], response[1], report_meta]
    if verbose:
        conv_tag = base_report_res[2]['convTagsIds'] if base_report_res[2].has_key('convTagsIds') else 0
        anom.wrt_log("Http Status: {0}, Job Id: {1}, Type: {2}, Campaign Id: {3}, Conv Tag Id: {4}".format(response[0], report_meta['jobId'], obj_name, campaign_id, conv_tag));
    return base_report_res

def get_report_job_status(obj_name, token, base_report_res, verbose=False, **kwargs):
    # set up client
    token = get_token() if token is None else token
    client = setup_soap_client(obj_name, token, **kwargs)
    response = client.service.GetReportJobStatus(base_report_res[1])
    # update base report response
    base_report_res[0] = response[0]
    base_report_res[2]['status'] = response[1]
    if verbose:
        conv_tag = base_report_res[2]['convTagsIds'] if base_report_res[2].has_key('convTagsIds') else 0
        anom.wrt_log("Http Status: {0}, Job Id: {1}, Type: {2}, Campaign Id: {3}, Conv Tag Id: {4}, Status: {5}".format(response[0], base_report_res[2]['jobId'], obj_name, base_report_res[2]['camId'], conv_tag, response[1]));
    return base_report_res

def get_report_as_url(obj_name, token, base_report_res, verbose=False, **kwargs):
    # set up client
    token = get_token() if token is None else token
    client = setup_soap_client(obj_name, token, **kwargs)
    response = client.service.GetReportAsURL(base_report_res[1])
    # update base report response
    base_report_res[0] = response[0]
    base_report_res[2]['url'] = response[1]
    if verbose:
        has_url = True if response[1] is not None else False
        conv_tag = base_report_res[2]['convTagsIds'] if base_report_res[2].has_key('convTagsIds') else 0
        anom.wrt_log("Http Status: {0}, Job Id: {1}, Type: {2}, Campaign Id: {3}, Conv Tag Id: {4}, HasUrl: {5}".format(response[0], base_report_res[2]['jobId'], obj_name, base_report_res[2]['camId'], conv_tag, has_url));
    return base_report_res

def parse_report(obj_name, base_report_res, verbose=False):
    item_list = []
    url = base_report_res[2]['url']
    tree = etree.parse(urlopen(url))
    elements = tree.findall('DataRow') if tree.find('DataRow') is not None else None
    if elements is not None:
        for e in elements:
            if obj_name == 'performance_report':                
                item = {'adId': int(e.find('AdID').text) if e.find('AdID') is not None else None,
                        'plId': int(e.find('PlacementID').text) if e.find('PlacementID') is not None else None,
                        'pkgId': int(e.find('PackageID').text) if e.find('PackageID') is not None else None,
                        'siteId': int(e.find('SiteID').text) if e.find('SiteID') is not None else None,
                        'camId': int(e.find('CampaignID').text) if e.find('CampaignID') is not None else None,
                        'advId': int(e.find('AdvertiserID').text) if e.find('AdvertiserID') is not None else None,
                        'delDate': datetime.strptime(e.find('DeliveryDate').text, '%Y%m%d%H').date() if e.find('DeliveryDate') is not None else None,
                        'imps': int(e.find('Impressions').text) if e.find('Impressions') is not None else None,
                        'clks': int(e.find('Clicks').text) if e.find('Clicks') is not None else None,
                        'ctr': float(e.find('CTR').text) if e.find('CTR') is not None else None,
                        'mmCost': float(e.find('MediaCost').text) if e.find('MediaCost') is not None else None,
                        'effCost': float(e.find('MediaCost').text) if e.find('MediaCost') is not None else None,
                        'visImps': int(e.find('VisibleImpressions').text) if e.find('VisibleImpressions') is not None else None,
                        'invisImps': int(e.find('NonVisibleImpressions').text) if e.find('NonVisibleImpressions') is not None else None,
                        'ttlConvs': int(e.find('TotalConversion').text) if e.find('TotalConversion') is not None else None,
                        'pvConvs': int(e.find('PostImpressionsConversions').text) if e.find('PostImpressionsConversions') is not None else None,
                        'pcConvs': int(e.find('PostClickConversions').text) if e.find('PostClickConversions') is not None else None,
                        'addedDate': datetime.now().date()}
                item_list.append(item)
            else:
                item = {'adId': int(e.find('AdID').text) if e.find('AdID') is not None else None,
                        'plId': int(e.find('PlacementID').text) if e.find('PlacementID') is not None else None,
                        'pkgId': int(e.find('PackageID').text) if e.find('PackageID') is not None else None,
                        'siteId': int(e.find('SiteID').text) if e.find('SiteID') is not None else None,
                        'camId': int(e.find('CampaignID').text) if e.find('CampaignID') is not None else None,
                        'advId': int(e.find('AdvertiserID').text) if e.find('AdvertiserID') is not None else None,
                        'convTagId': int(base_report_res[2]['convTagsIds']) if base_report_res[2].has_key('convTagsIds') else 0,
                        'delDate': datetime.strptime(e.find('DeliveryDate').text, '%Y%m%d%H').date() if e.find('DeliveryDate') is not None else None,
                        'ttlConvs': int(e.find('TotalConversion').text) if e.find('TotalConversion') is not None else None,
                        'pvConvs': int(e.find('PostImpressionsConversions').text) if e.find('PostImpressionsConversions') is not None else None,
                        'pcConvs': int(e.find('PostClickConversions').text) if e.find('PostClickConversions') is not None else None,
                        'addedDate': datetime.now().date()}
                item_list.append(item)                
    return item_list

def get_report_pgn(obj_name, token, campaign_list, conv_tag_dict=None, verbose=False, **kwargs):
    token = get_token() if token is None else token
    init_list = []
    status_list = []
    report_list = []
    data_list = []
    if obj_name == 'performance_report':        
        # initiate        
        for campaign in campaign_list:
            cond = True
            while cond:
                response = initiate_job(obj_name, token, campaign['id'], campaign['start'], campaign['end'], campaign['timeZoneId'], conv_tag_id=None, verbose=True, **kwargs)
                if response[0] is 200 and response[2]['jobId'] is not None:
                    init_list.append(response)
                    cond = False
    else:
        # initiate
        for cam_id, conv_tags_list in conv_tag_dict.items():
            for conv_tag in conv_tags_list:                
                campaign = [item for item in campaign_list if item['id'] == cam_id][0]
                cond = True
                while cond:
                    response = initiate_job(obj_name, token, campaign['id'], campaign['start'], campaign['end'], campaign['timeZoneId'], conv_tag_id=conv_tag, verbose=True, **kwargs)
                    if response[0] is 200 and response[2]['jobId'] is not None:
                        init_list.append(response)
                        cond = False        
    # check job status        
    for response in init_list:
        cond = True
        while cond:
            time.sleep(3)
            response = get_report_job_status(obj_name, token, response, verbose=True, **kwargs)
            if response[0] is 200 and response[2]['status'] == 'Completed':
                status_list.append(response)
                cond = False
    # get report as url        
    for response in status_list:
        cond = True
        while cond:
            response = get_report_as_url(obj_name, token, response, verbose=True, **kwargs)
            if response[0] is 200 and response[2]['url'] is not None:
                report_list.append(response)
                cond = False
    # parse report data        
    for report in report_list:
        data = parse_report(obj_name, report, verbose=True)
        data_list = data_list + data
    return data_list

# # get_report_pgn test
# token = get_token()
# #ids = [489599,489609]
# obj_name = 'performance_report'
# cam_id = 489599
# campaign_list = get_campaign_items_pgn('campaign', token, page_index=0, page_size=50, verbose=False, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=cam_id)
# campaign = campaign_list[0]
# init = initiate_job(obj_name, token, campaign['id'], campaign['start'], campaign['end'], campaign['timeZoneId'], verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
# status = get_report_job_status(obj_name, token, init, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
# url = get_report_as_url(obj_name, token, status, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
# data = parse_report(url, verbose=True)
# data_list = get_report_pgn(obj_name, token, campaign_list, conv_tag_dict=None, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)

######## get ad ops meta objects ########
def get_config():
    config = anom.json_obj['Redshift']['config']
    return config

def query_conv_tag_map(adserver='Sizmek'):
    return "SELECT DISTINCT campaignid, activitytagid from xxanom.adops_conversion_tags_lookup WHERE adserver='%s' AND len(activitytagid) > 0" % adserver

## PyGreSQL
# Windows path evn var - C:\PostgreSQL;C:\PostgreSQL\bin;C:\Python27\;C:\Python27\Scripts;C:\Python27\Lib\site-packages;
def create_conn(config):
    conn = None
    try:
        conn = pg.connect(dbname=config['dbname'], host=config['host'], port=config['port'], user=config['username'], passwd=config['password'])
    except:
        anom.wrt_log('Connection failed')
    return conn

def get_conv_tag_map(conn, adserver='Sizmek'):
    rows = conn.query(query_conv_tag_map(adserver)).dictresult()
    qry_num = len(rows)    
    rows = [(int(row['campaignid']), int(row['activitytagid'])) for row in rows if row['campaignid'].isdigit() and row['activitytagid'].isdigit()]
    qry_num_selected = len(rows)
    anom.wrt_log("Conv tag queried: {0}, Conv tag selected: {1} - non numeric values are excluded.".format(qry_num, qry_num_selected));
    anom.wrt_log('%d campaign-converson tag pairs are pulled and' % len(rows))
    return rows

## psycopg2
# def create_conn(config):
#     conn = None
#     try:
#         conn = psycopg2.connect(dbname=config['dbname'], host=config['host'], port=str(config['port']), user=config['username'], password=config['password'])
#     except:
#         anom.wrt_log('Connection failed')
#     return conn
# 
# def get_conv_tag_map(config, adserver='Sizmek'):
#     conn = create_conn(config)
#     cur = conn.cursor()
#     cur.execute(query_conv_tag_map(adserver))
#     rows = [(int(row[0]), int(row[1])) for row in cur.fetchall() if row[0].isdigit() and row[1].isdigit()]
#     return rows

def create_conv_tag_dict(ids, mapper):
    map_dic = {}
    if len(ids) > 0:
        ids = list(set(ids))
        for cid in ids:
            if len([elem[1] for elem in mapper if elem[0]==cid]) > 0:
                dic = {cid: [elem[1] for elem in mapper if elem[0]==cid]}
                map_dic.update(dic)        
    return map_dic

def filter_obj_list(obj_list, start_key='start', end_key='end', break_date=datetime.now().date()):
    filtered_obj_list = [obj for obj in obj_list if obj[start_key].date() <= break_date and obj[end_key].date() >= break_date + timedelta(days=-1)]
    anom.wrt_log("{0} out of {1} campaigns are filtered out".format((len(obj_list) - len(filtered_obj_list)), len(obj_list)));
    return filtered_obj_list

# # # create_conv_tag_dict test
# token = get_token()
# item_list = get_meta_items_pgn('campaign', token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
# conv_tag_mapper = get_conv_tag_map(get_config(), adserver='Sizmek')
# campaign_ids = [item.get('id') for item in item_list]
# conv_tag_dict = create_conv_tag_dict(campaign_ids, conv_tag_mapper)

######## main extract function ########
# token = get_token()
# #### meta data
# ### campaign
# obj_name = 'campaign'
# file_name = 'SizmekApiCampaign'
# tbl_name = 'mm_api_campaign'
# campaign_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
# campaign_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
# # recent filter
# campaign_list = get_meta_items_pgn(obj_name, token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
# conv_tag_mapper = get_conv_tag_map(get_config(), adserver='Sizmek')
# campaign_ids = [item.get('id') for item in campaign_list]
# extra_campaign_ids = [555223, 517935]
# # id filter
# for cid in set(extra_campaign_ids).difference(campaign_ids):
#     campaign = get_meta_items_pgn('campaign', token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=cid)
#     campaign_list = campaign_list + campaign
# write_to_csv(campaign_list, campaign_file_name, campaign_col_name, mode='wb')
# #### report data
# campaign_list = filter_obj_list(campaign_list)
# ### performance report
# obj_name = 'performance_report'
# file_name = 'SizmekApiPerformanceReport'
# tbl_name = 'mm_api_perf_report'
# perf_rep_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
# perf_rep_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
# ######### correct p_campaign_list into campaign_list ##########
# p_campaign_list = campaign_list[0:5]
# ######### correct p_campaign_list into campaign_list ##########
# perf_rep_list = get_report_pgn(obj_name, token, p_campaign_list, conv_tag_dict=None, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
# write_to_csv(perf_rep_list, perf_rep_file_name, perf_rep_col_name, mode='wb')
# ### conversion report
# obj_name = 'conversion_report'
# file_name = 'SizmekApiConversionReport'
# tbl_name = 'mm_api_conv_report'
# conv_rep_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
# conv_rep_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
# campaign_ids = [item.get('id') for item in campaign_list]
# conv_tag_dict = create_conv_tag_dict(campaign_ids, conv_tag_mapper)
# ######### correct c_conv_tag_dict into conv_tag_dict ##########
# c_conv_tag_dict = {conv_tag_dict.items()[len(conv_tag_dict.items())-1][0]:conv_tag_dict.items()[len(conv_tag_dict.items())-1][1]}
# ######### correct c_conv_tag_dict into conv_tag_dict ##########
# conv_rep_list = get_report_pgn(obj_name, token, campaign_list, conv_tag_dict=c_conv_tag_dict, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
# write_to_csv(conv_rep_list, conv_rep_file_name, conv_rep_col_name, mode='wb')

def extract(f_list, d_list, jobname):
    if anom.json_obj.has_key('Redshift'):
        config = anom.json_obj['Redshift'];
    else:
        anom.Errmsg = 'Redshift Object missing from %s' % anom.options.json_file;
        return False;
    try:
        conn = pg.connect(dbname=config['dbname'], host=config['host'][1], port=config['port'], user=config['user'], passwd=config['pass'])
    except:
        anom.Errmsg = 'Connection failed';
        return False;    
    token = get_token()
    #### meta data
    ### campaign
    obj_name = 'campaign'
    file_name = 'SizmekApiCampaign'
    tbl_name = 'mm_api_campaign'
    campaign_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
    campaign_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
    # recent filter
    campaign_list = get_meta_items_pgn(obj_name, token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, is_recent=True)
    conv_tag_mapper = get_conv_tag_map(conn, adserver='Sizmek')
    campaign_ids = [item.get('id') for item in campaign_list]
    extra_campaign_ids = [504022,504024,504030,506787,509349,513299,517935,543765,554718,555223,555227,555719,555721,564248,564651,564706]
    # id filter
    for cid in set(extra_campaign_ids).difference(campaign_ids):
        campaign = get_meta_items_pgn('campaign', token, page_index=0, page_size=50, verbose=True, add_token=True, import_msg_src=True, import_arr_src=False, campaign_id=cid)
        campaign_list = campaign_list + campaign
    write_to_csv(campaign_list, campaign_file_name, campaign_col_name, mode='wb')
    #### report data
    #campaign_list = filter_obj_list(campaign_list)
    ### performance report
    obj_name = 'performance_report'
    file_name = 'SizmekApiPerformanceReport'
    tbl_name = 'mm_api_perf_report'
    perf_rep_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
    perf_rep_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
    perf_rep_list = get_report_pgn(obj_name, token, campaign_list, conv_tag_dict=None, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
    write_to_csv(perf_rep_list, perf_rep_file_name, perf_rep_col_name, mode='wb')
    ### conversion report
    obj_name = 'conversion_report'
    file_name = 'SizmekApiConversionReport'
    tbl_name = 'mm_api_conv_report'
    conv_rep_col_name = anom.json_obj['TableStruct'][tbl_name]['Columns']
    conv_rep_file_name = '%s_%s%s' % (file_name, anom.fildtm(), anom.CSVEXT);
    campaign_ids = [item.get('id') for item in campaign_list]
    conv_tag_dict = create_conv_tag_dict(campaign_ids, conv_tag_mapper)
    conv_rep_list = get_report_pgn(obj_name, token, campaign_list, conv_tag_dict=conv_tag_dict, verbose=True, add_token=True, import_msg_src=False, import_arr_src=False)
    write_to_csv(conv_rep_list, conv_rep_file_name, conv_rep_col_name, mode='wb')
    return True




