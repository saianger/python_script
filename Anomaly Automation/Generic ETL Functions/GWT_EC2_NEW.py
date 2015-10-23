#!/bin/python
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# GWT_EC2_2S3.py
#
#   Put GWT data files from local EC2 site to S3
#   Copy data into redshift cluster
#
# Assumption:
# 1. File names to load are found in GWT_EC2.log
# 2. If GWT_EC2.log does not exist, there are no files to load
# 3. Rename GWT_EC2.log to GWT_S3_YYYYMMDD_HMS after load completed
#
# This script is to be run by Jenkins, once completed, the data processing
# job(s) are kicked off by Jenkins automatically.
# Copyright (c) 2015 Anomaly
# Author Tony Edward
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Python modules

import urllib;
import os;
import sys;
import datetime;
import glob;
import csv;
import time;
import shutil;
from dateutil.parser import *;
from selenium import webdriver;
from selenium.webdriver.common.keys import Keys;
from selenium.webdriver.common.by import By;
from selenium.webdriver.support.ui import WebDriverWait;
from selenium.common.exceptions import WebDriverException;
from selenium.webdriver.support import expected_conditions as EC;
from selenium.common.exceptions import TimeoutException;
import tldextract;

from AnomSys import AnomSys;

# Global variables

CLIENT_DIR = '';                                                                                                              # initialise client dir

# Construct AnomSys class

anom = AnomSys();

# Global constants

GWT_CRD  = anom.json_obj['Credentials'];                                                                                      # GWT credentials
GWT_URL  = anom.json_obj['GWT_url'];                                                                                          # GWT url
BASE_DIR = anom.json_obj['Basedir'];                                                                                          # set base working directory
LOG_FIL  = anom.json_obj['Logfile'];
FILES    = anom.json_obj['FileStruct'];
SKIP     = anom.json_obj['SkipClients'];

# FUNCTIONS

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Return characters in page between two strings
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def find_between(s, first, last):

  try:
    start = s.index(first) + len(first);
    end = s.index(last, start);
    return s[start:end];
  except ValueError:
    return "Found Nothing";

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Find a token within a page
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def find_token(page, report):
  token = find_between(page, report, "prop")+"prop";
  token = token.decode('unicode-escape');
  token = find_between(token, "security_token", "prop");
  token = token.replace(':', '%3A');
  return token;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Open page and return html code
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def open_page(client, page):

  URL = 'https://www.google.com/webmasters/tools/top-search-queries?hl=en&siteUrl='+client+'&type='+page+'&grid.s=100000';
  mechanize.open(URL);

  return mechanize.response().read();

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Add client and date columns to file
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def add_data(file, rowdata, last_record):

  rows = [];
  for key in FILES:
    if key in file:
      COLUMNS = FILES[key];

  with open(file, 'rb') as f:
    reader = csv.DictReader(f);
    for row in reader:
      for key in row.keys():
        if key in ['Change', '']:
          del row[key];
      for key in rowdata.keys():
        row[key] = rowdata[key];
      if last_record: rows = [];
      rows.append(row);

  with open(file, "wb") as f:
    writer = csv.DictWriter(f, fieldnames=COLUMNS);
    writer.writerow(dict((fn,fn) for fn in COLUMNS));                                                                         # write the header record
    writer.writerows(rows);                                                                                                   # write row to output

  return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get date range based on today
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_date_range(client):

  date_earliest = find_GWT_date(client, [-110, 0, 1]);                                                                         # GWT = Get earliest available date for Client
  if date_earliest == None: return None;

  anom.wrt_log('Earliest Date: %s' % date_earliest);

  date_latest = find_GWT_date(client, [0, -110, -1]);                                                                          # GWT = Get latest available date for Client
  if date_latest == None: return None;

  anom.wrt_log('Latest Date: %s' % date_latest);

  return anom.datlst(date_earliest, date_latest);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get a date from GWT besed on range arguments
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def find_GWT_date(client, range_args):

  page = open_page(client['url'], 'urls');
  report = "/webmasters/tools/top-urls-chart-dl";
  token = find_token(page, report);

# Determine day to download

  for x in range(range_args[0], range_args[1], range_args[2]):
    date = datetime.date.fromordinal(datetime.date.today().toordinal()+x).strftime('%Y%m%d');
    file = "https://www.google.com/"+report+"?hl=en&siteUrl="+client['url']+"&authuser=0&security_token"+token+"prop=WEB&region&db="+date+"&de="+date+"&more=true&format=csv";

# Check for data

    result = open(mechanize.retrieve(file)[0]).readlines();
    if len(result) > 1:
      return date;

  return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Scrape data from GWT using firefox
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def run_selenium(client, date):

# Navigate to

  get_url = 'https://www.google.com/webmasters/tools/top-search-queries?hl=en&siteUrl='+client['url']+'&type=urls&prop=WEB&region&db='+date+'&de='+date+'&more=true&pli=1&grid.s=10000';
  anom.wrt_log(get_url);
  firefox.get(get_url);
  firefox.implicitly_wait(10);                                                                                                # seconds
  urlname = client['url'].replace('.','-').replace('://','-').replace('/','');
  filename = CLIENT_DIR+urlname+"-page_queries-"+date+"-"+date_today+".csv";
  anom.wrt_log(filename);
  element_list = firefox.find_elements_by_class_name('table-range-text');
  if len(element_list) > 0:
    num_records = int(element_list[0].text.replace(',', '').split()[-1]);
  else:
    anom.wrt_log('No records for %s on %s' % (client['name'], date));
    return False;
  step_number = 50;

  anom.wrt_log('Number of records: %d' % num_records);

  with open(filename, "wb") as file:
    writer = csv.writer(file);
    writer.writerow(["Client","Date","Page","Query","Impressions","Clicks"]);                                                 # write the header
    anom.wrt_log(filename);

# Get the rows of data from page

    for page_num in xrange(0,num_records,step_number):
      get_url = 'https://www.google.com/webmasters/tools/top-search-queries?hl=en&siteUrl='+client['url']+'&type=urls&prop=WEB&region&db='+date+'&de='+date+'&more=true&pli=1&grid.s='+str(step_number)+'&grid.r='+str(page_num);
      anom.wrt_log(get_url);
      firefox.get(get_url);
      try:
        WebDriverWait(firefox, 25).until
        (
          EC.visibility_of_element_located((By.CSS_SELECTOR, '.url-expand-closed'))
        );
      except TimeoutException as e:
        anom.wrt_log('{0}: screen {1}, StackTrace {2}'.format(e.msg, e.screen, e.stacktrace));
        break;
      except UnexpectedAlertPresentException as e:
        anom.wrt_log('{0}: screen {1}, StackTrace {2}, Alert {3}'.format(e.msg, e.screen, e.stacktrace, e.alert_text));
        break;
      finally:
        for link in firefox.find_elements_by_class_name('url-expand-closed'):
          linktext = link.text.split('\n')[0];
          data = [];
          link.click();                                                                                                       # Open Link
          try:
            WebDriverWait(firefox, 25).until
            (
              EC.visibility_of_element_located((By.CSS_SELECTOR, '.url-detail-row:not([style*="display: none"])'))
            );
          except TimeoutException as e:
            anom.wrt_log('{0}: screen {1}, StackTrace {2}'.format(e.msg, e.screen, e.stacktrace));
            break;
          except UnexpectedAlertPresentException as e:
            anom.wrt_log('{0}: screen {1}, StackTrace {2}, Alert {3}'.format(e.msg, e.screen, e.stacktrace, e.alert_text));
            break;
          finally:
            queries = [tr for tr in firefox.find_elements_by_css_selector('.url-detail-row:not([style*="display: none"])') if tr.text!=''];
            for tr in queries:
              tds=tr.find_elements_by_tag_name('td');                                                                         # Extract Data
              data.append([client['name'], date, linktext, tds[0].text.encode("utf-8").replace(',',''), tds[1].text.encode("utf-8").replace(',',''), tds[3].text.encode("utf-8").replace(',','')]);
            link.click();                                                                                                     # close Link
            writer.writerows(data);                                                                                           # write the rows
      anom.wrt_log('Page %d to %d of %d scraped' % (page_num, min(step_number + page_num, num_records), num_records));

  return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Find clients in page
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def find_clients():

  Clients = [];
  try:
    WebDriverWait(firefox, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "site-verified")));
  finally:
    links = firefox.find_elements_by_class_name('site-verified');
    for link in links:
      client_name = tldextract.extract(link.text).domain;
      if link.text.startswith("http"):
          client_url = link.text;
      else:
          client_url = 'http://'+link.text;

      if client_url not in SKIP: Clients.append({'name': client_name, 'url': client_url});

  Clients.sort();

  return Clients;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download files from GWT
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def download_files(client, page, Current_Page, report, args, date, filename, last_record=False):

  token = find_token(page, report);
  file = "https://www.google.com"+report+"?hl=en&siteUrl="+client['url']+"&security_token"+token+"&de="+date+"&db="+date+args;
  urlname = client['url'].replace('.','-').replace('://','-').replace('/','');
  downloaded_file = mechanize.retrieve(file, CLIENT_DIR+urlname+'-'+Current_Page+'-'+filename+'-'+date+'-'+date_today+'.csv')[0];

  numlines = len(open(downloaded_file).readlines());

  if numlines < 2:
    os.remove(downloaded_file);
    return False;
  else:
    anom.wrt_log(downloaded_file);
    if not add_data(downloaded_file, {"Client": client['name'], "Date": date}, last_record):
      return False;
    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cleanup open objects prior to exit
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def clean_exit():

  mechanize.close();
  firefox.close();
  anom.virtual_display(start=False);
  os.chdir(anom.Orgdir);
  return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Program executes from here
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# set directory

Curdat = anom.fildtm();

anom.Orgdir = os.getcwd();                                                                                                    # save current directory

try:
  os.chdir(BASE_DIR);                                                                                                         # change to base directory
except:
  sys.exit('Cannot change directory to %s' % BASE_DIR);

# Delete previous log files

OLD_LOG = glob.glob('%s%s%s' % (LOG_FIL, anom.WILDCARD, anom.LOGEXT));                                                         # get list of log files
for file in OLD_LOG:
  os.remove(file);                                                                                                            # delete previous log file

# Create log file

if (not anom.crt_log(LOG_FIL)): anom.err_exit(anom.Errmsg);
if (not anom.wrt_log('Deleted %s ' % OLD_LOG)): anom.err_exit(anom.Errmsg);

# Create environment

anom.Orgdir = os.getcwd();                                                                                                    # store current directory
anom.curdat();                                                                                                                # initialise date
date_today = anom.datstr(Revers=True, Sep="");                                                                                # get date as YYYYMMDD
anom.virtual_display();                                                                                                       # start virtual display for firefox

# Selenium (Firefox specific)

anom.wrt_log('Firefox - Logging in');

firefox = anom.selenium_login(GWT_URL, GWT_CRD);                                                                              # login to GWT with firefox

if firefox == None:
  anom.wrt_log(anom.Errmsg);
  anom.err_exit(anom.Errmsg);

anom.wrt_log('Firefox - Logged in');

# Create browser objects and login

anom.wrt_log('Mechanize - Logging in');

mechanize = anom.mechanize_login(GWT_URL, GWT_CRD);                                                                                     # create headless browser object

if mechanize == None:
  anom.wrt_log(anom.Errmsg);
  anom.err_exit(anom.Errmsg);

anom.wrt_log('Mechanize - Logged in');

# Get list of clients from GWT

clients = find_clients();

for client in clients:

  CLIENT_DIR = '%s%s/' % (BASE_DIR, client['name']);                                                                           # set the client directory string

# Create target directory

  try:
    os.stat(CLIENT_DIR);                                                                                                      # exists?
  except:
    os.mkdir(CLIENT_DIR);                                                                                                     # no, create it

  Prc_mrk = '%sprocessing' % CLIENT_DIR;

  open(Prc_mrk, "wb").close();                                                                                           # set processing marker

#Get Dates (search for YYYYMMDD.mrk file to establish last date processed)

  anom.wrt_log('Determining Dates for %s %s' % (client['name'], client['url']));

# Check for date marker file

  Date_marker = glob.glob('%s%s%s' % (CLIENT_DIR, anom.WILDCARD, anom.MRKEXT));

  if len(Date_marker) < 1:
    date_range = get_date_range(client);
    if date_range == None:
      anom.wrt_log('No dates available for %s %s' % (client['name'], client['url']));
      continue;
  else:
    start_date = os.path.splitext(os.path.basename(Date_marker[0]))[0];
    date_range = anom.datlst(start_date, date_today);

  for date in date_range:

# create first date marker file for new client

    if len(Date_marker) < 1:
      f_time = (parse(date)).strftime('%Y%m%d');
      Date_marker = ['%s%s%s' % (CLIENT_DIR, f_time, anom.MRKEXT)];
      open(Date_marker[0], "wb").close();

# DOWNLOAD FILES

    data_found = run_selenium(client, date);                                                                         # Selenium

    if (data_found):
      Current_Page = 'urls';
      page = open_page(client['url'], Current_Page);
      if not download_files(client, page, Current_Page, "/webmasters/tools/top-search-urls-dl", "&type=urls&prop=WEB&region&more=true", date, 'page'):
        anom.wrt_log("No File available for this date %s" % date);

      Current_Page = 'queries';
      page = open_page(client['url'], Current_Page);
      if not download_files(client, page, Current_Page, "/webmasters/tools/top-queries-chart-dl", "&more=true&prop=WEB&region&format=csv", date, 'chart', last_record=True):
        anom.wrt_log("No File available for this date %s" % date);
      if not download_files(client, page, Current_Page, "/webmasters/tools/top-search-queries-dl", "&more=true&type=queries&prop=WEB&region&format=csv", date, 'search'):
        anom.wrt_log("No File available for this date %s" % date);

# Done

      if len(Date_marker) > 0:
        f_del = Date_marker[0];
        os.remove(f_del);
      f_time = (parse(date)+datetime.timedelta(1)).strftime('%Y%m%d');
      Date_marker = ['%s%s%s' % (CLIENT_DIR, f_time, anom.MRKEXT)];
      open(Date_marker[0], "wb").close();
      anom.wrt_log('Date markers for %s changed from [%s] to [%s]' % (client['name'], os.path.split(f_del)[1], os.path.split(Date_marker[0])[1]));
    else:
      anom.wrt_log('No data found for %s for %s' % (client['name'], date));
      break;

  os.remove(Prc_mrk);                                                                                                    # clear processing marker

clean_exit();
sys.exit(0);

