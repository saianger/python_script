#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys.py
# V 0.1
# System Functions Module
# Author Tony Edward
# Copyright (c) 2015 Anomaly
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Redhat old Crypto warning bypass
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from Crypto.pct_warnings import PowmInsecureWarning;
import warnings;
warnings.simplefilter("ignore", PowmInsecureWarning);

# Python imports

import string;                                                                                                                # string module
import decimal;                                                                                                               # decimal module
import math;                                                                                                                  # maths module
import datetime;                                                                                                              # datetime module
import time;                                                                                                                  # time module
import sys;                                                                                                                   # sys module
import ast;                                                                                                                   # abstract syntax trees module
import hmac;                                                                                                                  # hmac object
import hashlib;                                                                                                               # hashlib object
import base64;                                                                                                                # base64 object
import subprocess;                                                                                                            # subprocess module
import textwrap;                                                                                                              # textwrap module
import re;                                                                                                                    # regular expresions module
import fnmatch;                                                                                                               # filename matching module
import os;                                                                                                                    # os module
import shlex;                                                                                                                 # shlex module
import smtplib;                                                                                                               # smtplib module
from email.MIMEMultipart import MIMEMultipart;                                                                                # MIME submodule Multipart
from email.MIMEText import MIMEText;                                                                                          # MIME submodule Text
import gzip;                                                                                                                  # gzip module
import zipfile;                                                                                                               # zip module
import json;                                                                                                                  # json module
import csv;                                                                                                                   # csv read/write module
import inspect;                                                                                                               # inspect module for errors
from ftplib import FTP;                                                                                                       # ftp module
from argparse import ArgumentParser;                                                                                          # CLI argumnet parser module
import requests;                                                                                                              # HTTP requests module
import httplib2;                                                                                                              # HTTP2 module
import urllib;                                                                                                                # urllib module

# 3rd Party Python modules

import jsonpickle;                                                                                                            # json to python object module
import paramiko;                                                                                                              # paramiku HTTP urllib module
from dateutil.parser import *;                                                                                                # date parsing module
import pysftp;                                                                                                                # sftp module
try:
  from pyvirtualdisplay import Display;                                                                                       # virtual display module
except:
  Display = None;
import mechanize;                                                                                                             # mechhanize web browser module
import cookielib;                                                                                                             # web cookies module
from selenium import webdriver;                                                                                               # S E L E N I U M  modules
from selenium.webdriver.common.keys import Keys;                                                                              # |
from selenium.webdriver.common.by import By;                                                                                  # |
from selenium.webdriver.support.ui import WebDriverWait;                                                                      # |
from selenium.common.exceptions import WebDriverException;                                                                    # |
from selenium.webdriver.support import expected_conditions as EC;                                                             # |
from selenium.common.exceptions import TimeoutException;                                                                      # |
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary                                                           # V
from facebookads import FacebookSession;                                                                                      # F A C E B O O K  modules
from facebookads import FacebookAdsApi;                                                                                       # |
from facebookads.objects import AdAccount;                                                                                    # |
from facebookads.exceptions import FacebookRequestError;                                                                      # V
from apiclient.discovery import build;                                                                                        # G O O G L E  modules
from apiclient.errors import HttpError;                                                                                       # |
from oauth2client.client import flow_from_clientsecrets;                                                                      # |
from oauth2client.file import Storage;                                                                                        # |
from oauth2client import tools;                                                                                               # V
import pg;                                                                                                                    # PyGreSQL module for Redshift and PostgreSQL
import boto;                                                                                                                  # boto module
from boto.s3.key import Key;                                                                                                  # boto Key module

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Definition
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

class AnomSys:

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Constants
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Copyright constants

  CPYRIT = "Copyright";                                                                                                       # copyright word
  CPYCHR = "(C)";                                                                                                             # copyright character
  CPYYER = "2015";                                                                                                            # copyright year
  CPYCMP = "Anomaly";                                                                                                         # copyright company
  CPYNOT = "%s %s %s %s" % (CPYRIT, CPYCHR, CPYYER, CPYCMP);                                                                  # copyright notice

# O/S Constants

  L32 = "linux2";                                                                                                             # linux o/s
  W32 = "win32";                                                                                                              # win32 o/s
  OSX = "darwin";                                                                                                             # mac o/s

# Python Constants

  PY_VER = ".".join(str(i) for i in tuple(sys.version_info));

# Machine constants

  LOCPFX = "127";                                                                                                             # local ip address prefix

  HOSTPT = 5439;                                                                                                              # default redshift host port number
  POP3PT = 110;                                                                                                               # default pop3 port number
  SMTPPT = 25;                                                                                                                # default smtp port number

# Database constants

  DATDRV = "redshift+psycopg2://";                                                                                            # SQLAlchemy driver prefix
  DATHST = "mbau-ausanom-redshift.c3nkjwrx2zc4.ap-southeast-2.redshift.amazonaws.com";                                        # redshift hose name                                                                  # database host
  DATNAM = "dsaf";                                                                                                            # database name
  DATUSR = "anompublic";                                                                                                      # database username
  DATPWD = "Duch+thup*629AY";                                                                                                 # database password

# Debug flags

  Debug = False;

# Application constants

  SYSCOD = "ANOM";                                                                                                            # system code
  SYSNAM = "Anomaly";                                                                                                         # system name
  SYSVER = "1.0";                                                                                                             # system version
  SYSSTR = "%s %s" % (SYSNAM, SYSVER);                                                                                        # system string

# Calendar constants

  Dayary = \
  [
    ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
  ];

  Mthary = \
  [
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
  ];

# Date separators

  DATSLS = "/";                                                                                                               # Slash
  DATDOT = ".";                                                                                                               # Dots or full stops
  DATHYP = "-";                                                                                                               # Hyphens or dashes
  DATSPC = " ";                                                                                                               # Spaces
  DATNUL = "";                                                                                                                # Null

  SEPLST = \
  [
    DATSLS,
    DATDOT,
    DATHYP,
    DATSPC,
    DATNUL,
  ];

# Short/long name array index access

  SHTNAM = 0;
  LNGNAM = 1;

# Set year limits

  MINYER = 1900;                                                                                                              # minimum year number
  MAXYER = 2999;                                                                                                              # maximum year number

# Set time limits

  MINTIM = decimal.Decimal("0.00");                                                                                           # minimum time value
  MAXTIM = decimal.Decimal("24.00");                                                                                          # maximum time value
  MIDNIT = '23:59:59';                                                                                                        # midnight time string

# Date conversion constants

  MTHDAY = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];                                                               # month days array
  UPTO47 = 17532;                                                                                                             # up to 1947 factor
  IN4YRS = 1461;                                                                                                              # days in 4 years

# Date formats with year

  DFMYER = \
  [
    "%d %m %Y",
    "%Y %m %d",
    "%d %B %Y",
    "%B %d %Y",
    "%d %b %Y",
    "%b %d %Y",
    "%d %m %y",
    "%y %m %d",
    "%d %B %y",
    "%B %d %y",
    "%d %b %y",
    "%b %d %y",
  ]

# Date formats without year

  DFMMTH = \
  [
    "%d %m",
    "%d %B",
    "%B %d",
    "%d %b",
    "%b %d",
  ]

# Time of day constants

  DAYBEG = "00:00:00"
  DAYEND = "23:59:59"
  DAYMID = "11:59:59"

# Time formats

  TFMHMS = \
  [
    "%H",
    "%H %M",
    "%H %M %S",
    "%I %p",
    "%I %M %p",
    "%I %M %S %p",
  ]

# ISO 8601

  ISODATFMT = "%Y-%m-%dT%H:%M:%S"

# SQL Constants

  ASCORD = "asc";                                                                                                             # ascending order keyword
  DSCORD = "desc";                                                                                                            # descending order keyword

# Decimal constants

  POSMLT = decimal.Decimal("1.00");                                                                                           # decimal positive multiplier
  NEGMLT = decimal.Decimal("-1.00");                                                                                          # decimal negative multiplier
  DECZER = decimal.Decimal("0.00");                                                                                           # decimal zero
  DECONE = decimal.Decimal("1.00");                                                                                           # decimal one
  DECFIV = decimal.Decimal("5.00");                                                                                           # decimal five
  DECTEN = decimal.Decimal("10.00");                                                                                          # decimal ten
  DEC100 = decimal.Decimal("100.00");                                                                                         # decimal 100
  ONECNT = decimal.Decimal("0.01");                                                                                           # decimal 1 cent
  TWOCNT = decimal.Decimal("0.02");                                                                                           # decimal 2 cents
  THRCNT = decimal.Decimal("0.03");                                                                                           # decimal 3 cents
  FIVCNT = decimal.Decimal("0.05");                                                                                           # decimal 5 cents
  TENCNT = decimal.Decimal("0.10");                                                                                           # decimal 10 cents
  MINCUR = decimal.Decimal("0.05");                                                                                           # decimal minimum currency
  MAXRND = decimal.Decimal("0.02");                                                                                           # decimal maximum rounding

# Directory constants

  IMPDIR = "import";                                                                                                          # import directory
  EXPDIR = "export";                                                                                                          # export directory
  INPDIR = "input";                                                                                                           # input directory
  OUTDIR = "output";                                                                                                          # output directory
  RECDIR = "receive";                                                                                                         # receive directory
  TRNDIR = "transmit";                                                                                                        # transmit directory

  ALWCHR = True;                                                                                                              # allowed characters flag
  DISCHR = False;                                                                                                             # disallowed characters flag
  POSCHR = "+";                                                                                                               # allowed positive characters
  NEGCHR = "-";                                                                                                               # allowed negative characters
  INTCHR = "0123456789";                                                                                                      # allowed int characters
  FLTCHR = ".0123456789";                                                                                                     # allowed float characters
  NUMCHR = "-.0123456789";                                                                                                    # allowed numeric characters
  HEXCHR = "0123456789ABCDEF";                                                                                                # allowed hex characters
  DATCHR = "/-,' 0123456789ABCDEFGHIJLMNOPRSTUVY";                                                                            # allowed date characters
  TIMCHR = "0123456789:.";                                                                                                    # allowed time characters
  EXPCHR = "+-*@/#%()$, " + FLTCHR;                                                                                           # allowed expression characters
  SCHCHR = "_";                                                                                                               # allowed search characters
  STRCHR = "\\";                                                                                                              # disallowed string characters
  CODCHR = "\\";                                                                                                              # disallowed code characters
  KEYCHR = "\\";                                                                                                              # disallowed keyword characters

# Disallowed characters in email addresses

  LFTSQU = chr(0x5b);                                                                                                         # left square bracket ([)
  RHTSQU = chr(0x5d);                                                                                                         # right square  bracket (])
  LFTRND = chr(0x28);                                                                                                         # left bracket (()
  RHTRND = chr(0x29);                                                                                                         # right bracket ())
  LFTANG = chr(0x3c);                                                                                                         # left angle bracket (<)
  RHTANG = chr(0x3e);                                                                                                         # right angle bracket (>)
  DBLQOT = chr(0x22);                                                                                                         # double quote (")
  SNGQOT = chr(0x27);                                                                                                         # single quote (')
  AMPSND = chr(0x26);                                                                                                         # ampersand (&)
  FULCLN = chr(0x3a);                                                                                                         # full colon (:)
  SEMCLN = chr(0x3b);                                                                                                         # semi colon (;)
  COMMAS = chr(0x2c);                                                                                                         # commas (,)
  BKSLSH = chr(0x5c);                                                                                                         # back slash (\)
  ATSIGN = chr(0x40);                                                                                                         # at sign (@)
  SPACES = chr(0x20);                                                                                                         # space ( )

# List of disallowed characters in email addresses

  DISEML = \
  (
    LFTSQU,
    RHTSQU,
    LFTRND,
    RHTRND,
    LFTANG,
    RHTANG,
    DBLQOT,
    SNGQOT,
    AMPSND,
    FULCLN,
    SEMCLN,
    COMMAS,
    BKSLSH,
    ATSIGN,
    SPACES,
  );

# Display constants

  SPCCHR = " ";                                                                                                               # space character
  NULCHR = "";                                                                                                                # null character

# Formatting  constants

  FLTFMT = "%.2f";                                                                                                            # float format
  CURFMT = "$%s" % FLTFMT;                                                                                                    # currency format

# Extension constants

  CSVEXT = ".csv";                                                                                                            # csv file extension
  TXTEXT = ".txt";                                                                                                            # txt file extension
  JPGEXT = ".jpg";                                                                                                            # jpg file extension
  PCLEXT = ".pcl";                                                                                                            # pcl file extension
  PDFEXT = ".pdf";                                                                                                            # pdf file extension
  PNGEXT = ".png";                                                                                                            # png file extension
  PSTEXT = ".pst";                                                                                                            # pst file extension
  XMLEXT = ".xml";                                                                                                            # xml file extension
  LOGEXT = ".log";                                                                                                            # log file extension
  MRKEXT = ".mrk";                                                                                                            # marker file extension (anomaly specific)
  PYTEXT = ".py";                                                                                                             # python file extension
  GZFEXT = ".gz";                                                                                                             # gzip file extension
  ZIPEXT = ".zip";                                                                                                            # zip file extension
  TAREXT = ".tar";                                                                                                            # tar file extension
  BZ2EXT = ".bz2";                                                                                                            # tar.bz2 file extension
  JSNEXT = ".json";                                                                                                           # json file extension
  TMPEXT = ".tmp";                                                                                                            # temporary file extension

# Filename constants

  PYTHON = "/usr/bin/python";                                                                                                 # python pathname (linux)
  WILDCARD = "*";                                                                                                             # wildcard character
  FILESUB = '?';                                                                                                              # filename single substitution character
  FULFNM = "Full";                                                                                                            # part of file name for consolidated files

# Wildcard list

  WILD_LIST = \
  [
    WILDCARD,
    FILESUB,
  ];

# Directory constants

  fs = "/";                                                                                                                   # file separator
  PARDIR = "..";                                                                                                              # parent directory
  CURDIR = ".";                                                                                                               # current directory
  CURPTH = '%s%s' % (CURDIR, fs);                                                                                             # current path

# General constants

  nl = "\n";                                                                                                                  # line feed
  cr = "\r";                                                                                                                  # carriage return
  ff = "\f"                                                                                                                   # form feed
  crlf = '%s%s' % (cr, nl);

  TRUSTR = "Yes";                                                                                                             # true string
  FALSTR = "No";                                                                                                              # false string

  FILE_MAGIC = \
  {
    '\x50\x4B\x03\x04':                 [ZIPEXT, 'PKZIP archive_1'],
    '\x50\x4B\x4C\x49\x54\x45':         [ZIPEXT, 'PKLITE archive'],
    '\x50\x4B\x53\x70\x58':             [ZIPEXT, 'PKSFX self-extracting archive'],
    '\x50\x4B\x05\x06':                 [ZIPEXT, 'PKZIP archive_2'],
    '\x50\x4B\x07\x08':                 [ZIPEXT, 'PKZIP archive_3'],
    '\x57\x69\x6E\x5A\x69\x70':         [ZIPEXT, 'WinZip compressed archive'],
    '\x50\x4B\x03\x04\x14\x00\x01\x00': [ZIPEXT, 'ZLock Pro encrypted ZIP'],
    '\x1F\x8B\x08':                     [GZFEXT, 'GZIP archive file'],
    '\x75\x73\x74\x61\x72':             [TAREXT, 'Tape Archive'],
  	'\x42\x5A\x68':                     [BZ2EXT, 'bzip2 compressed archive']
  };

# Web headers

  WEBHDR = \
  [
    (
      'User-agent',
      'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1'
    ),
  ];

  FOXPRF = \
  [
    ['dom.max_chrome_script_run_time', 0],
    ['dom.max_script_run_time', 0],
  ];

  BADURL = 'https://accounts.google.com/ServiceLoginAuth';                                                                    # google bad login url

# Type casting constants dictionary

  TYPE_CAST = \
  {
    'INTEGER': int,
    'STRING': str,
    'FLOAT': float
  };

  MARKUP_TAGS = \
  {
    'HTML': ['html', '/html'],
    'NEWLINE': ['li', '/li'],
    'RED': ['font color="red"', '/font'],
    'GREEN': ['font color="green"', '/font'],
    'BLUE': ['font color="blue"', '/font'],
    'BOLD': ['b', '/b'],
    'ITALIC': ['b', '/b']
  };

  HTTPERRORS = \
  {
    100: ["CONTINUE", "HTTP/1.1", "RFC 2616", "Section 10.1.1"],
    101: ["SWITCHING_PROTOCOLS", "HTTP/1.1", "RFC 2616", "Section 10.1.2"],
    102: ["PROCESSING", "WEBDAV", "RFC 2518", "Section 10.1"],
    200: ["OK", "HTTP/1.1", "RFC 2616", "Section 10.2.1"],
    201: ["CREATED", "HTTP/1.1", "RFC 2616", "Section 10.2.2"],
    202: ["ACCEPTED", "HTTP/1.1", "RFC 2616", "Section 10.2.3"],
    203: ["NON_AUTHORITATIVE_INFORMATION", "HTTP/1.1", "RFC 2616", "Section 10.2.4"],
    204: ["NO_CONTENT", "HTTP/1.1", "RFC 2616", "Section 10.2.5"],
    205: ["RESET_CONTENT", "HTTP/1.1", "RFC 2616", "Section 10.2.6"],
    206: ["PARTIAL_CONTENT", "HTTP/1.1", "RFC 2616", "Section 10.2.7"],
    207: ["MULTI_STATUS", "WEBDAV", "RFC 2518", "Section 10.2"],
    226: ["IM_USED", "Delta encoding in HTTP", "RFC 3229", "Section 10.4.1"],
    300: ["MULTIPLE_CHOICES", "HTTP/1.1", "RFC 2616", "Section 10.3.1"],
    301: ["MOVED_PERMANENTLY", "HTTP/1.1", "RFC 2616", "Section 10.3.2"],
    302: ["FOUND", "HTTP/1.1", "RFC 2616", "Section 10.3.3"],
    303: ["SEE_OTHER", "HTTP/1.1", "RFC 2616", "Section 10.3.4"],
    304: ["NOT_MODIFIED", "HTTP/1.1", "RFC 2616", "Section 10.3.5"],
    305: ["USE_PROXY", "HTTP/1.1", "RFC 2616", "Section 10.3.6"],
    307: ["TEMPORARY_REDIRECT", "HTTP/1.1", "RFC 2616", "Section 10.3.8"],
    400: ["BAD_REQUEST", "HTTP/1.1", "RFC 2616", "Section 10.4.1"],
    401: ["UNAUTHORIZED", "HTTP/1.1", "RFC 2616", "Section 10.4.2"],
    402: ["PAYMENT_REQUIRED", "HTTP/1.1", "RFC 2616", "Section 10.4.3"],
    403: ["FORBIDDEN", "HTTP/1.1", "RFC 2616", "Section 10.4.4"],
    404: ["NOT_FOUND", "HTTP/1.1", "RFC 2616", "Section 10.4.5"],
    405: ["METHOD_NOT_ALLOWED", "HTTP/1.1", "RFC 2616", "Section 10.4.6"],
    406: ["NOT_ACCEPTABLE", "HTTP/1.1", "RFC 2616", "Section 10.4.7"],
    407: ["PROXY_AUTHENTICATION_REQUIRED", "HTTP/1.1", "RFC 2616", "Section 10.4.8"],
    408: ["REQUEST_TIMEOUT", "HTTP/1.1", "RFC 2616", "Section 10.4.9"],
    409: ["CONFLICT", "HTTP/1.1", "RFC 2616", "Section 10.4.10"],
    410: ["GONE", "HTTP/1.1", "RFC 2616", "Section 10.4.11"],
    411: ["LENGTH_REQUIRED", "HTTP/1.1", "RFC 2616", "Section 10.4.12"],
    412: ["PRECONDITION_FAILED", "HTTP/1.1", "RFC 2616", "Section 10.4.13"],
    413: ["REQUEST_ENTITY_TOO_LARGE", "HTTP/1.1", "RFC 2616", "Section 10.4.14"],
    414: ["REQUEST_URI_TOO_LONG", "HTTP/1.1", "RFC 2616", "Section 10.4.15"],
    415: ["UNSUPPORTED_MEDIA_TYPE", "HTTP/1.1", "RFC 2616", "Section 10.4.16"],
    416: ["REQUESTED_RANGE_NOT_SATISFIABLE", "HTTP/1.1", "RFC 2616", "Section 10.4.17"],
    417: ["EXPECTATION_FAILED", "HTTP/1.1", "RFC 2616", "Section 10.4.18"],
    422: ["UNPROCESSABLE_ENTITY", "WEBDAV", "RFC 2518", "Section 10.3"],
    423: ["LOCKED", "WEBDAV", "RFC 2518", "Section 10.4"],
    424: ["FAILED_DEPENDENCY", "WEBDAV", "RFC 2518", "Section 10.5"],
    426: ["UPGRADE_REQUIRED", "HTTP Upgrade to TLS", "RFC 2817", "Section 6"],
    500: ["INTERNAL_SERVER_ERROR", "HTTP/1.1", "RFC 2616", "Section 10.5.1"],
    501: ["NOT_IMPLEMENTED", "HTTP/1.1", "RFC 2616", "Section 10.5.2"],
    502: ["BAD_GATEWAY", "HTTP/1.1", "RFC 2616", "Section 10.5.3"],
    503: ["SERVICE_UNAVAILABLE", "HTTP/1.1", "RFC 2616", "Section 10.5.4"],
    504: ["GATEWAY_TIMEOUT", "HTTP/1.1", "RFC 2616", "Section 10.5.5"],
    505: ["HTTP_VERSION_NOT_SUPPORTED", "HTTP/1.1", "RFC 2616", "Section 10.5.6"],
    507: ["INSUFFICIENT_STORAGE", "WEBDAV", "RFC 2518", "Section 10.6"],
    510: ["NOT_EXTENDED", "An HTTP Extension Framework", "RFC 2774", "Section 7"]
  };

# BOTO connection api names

  S3 = 'S3Connection'
  GS = 'GSConnection';
  EC2 = 'EC2Connection';
  REDSHIFT = 'RedshiftConnection';

  BOTO_API_LIST = \
  [
    S3,
    GS,
    EC2,
    REDSHIFT,
  ];

  BOTO_FILE_LIST = \
  [
    S3,
    GS,
  ];

# boto file actions

  BOTO_GET    = "get";
  BOTO_SET    = "set";
  BOTO_EXISTS = "exists";
  BOTO_DELETE = "delete";

# boto file actions list

  BOTO_FILE_ACTION = \
  [
    BOTO_GET,
    BOTO_SET ,
    BOTO_EXISTS,
    BOTO_DELETE,
  ];

# boto api class calls

  BOTO_API = \
  {
    S3: boto.connect_s3,
    GS: boto.connect_gs,
    EC2: boto.connect_ec2,
    REDSHIFT: boto.connect_redshift,
  };

# BOTO objects

  BOTO_BUCKET = 'Bucket';
  BOTO_KEY = 'Key';

  BOTO_OBJECTS = \
  [
    BOTO_BUCKET,
    BOTO_KEY,
  ];

# boto file size object

  Boto_size = [0]*2;

# BOTO credential object {key: value} pair of {ACCESS_KEY: SECRET_KEY}

  BOTO_CREDENTIALS = {};

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Variables
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# O/S default variables

  Oprsys = L32;                                                                                                               # default o/s
  Osname = "Not defined";                                                                                                     # system os name
  Cmdlin = None;                                                                                                              # command line parameters
  Restrt = False;                                                                                                             # restart flag

# Python interpreter variables

  Pytver = '0.0.0';                                                                                                           # system python version
  PYTL32 = 'python';                                                                                                          # system python name
  PYTW32 = 'pythonw';                                                                                                         # system python name
  PYTOSX = 'python';                                                                                                          # system python name
  Pyname = PYTL32;                                                                                                            # default python interpreter

# Connamd line options

  parser = None;                                                                                                              # initialise parser object
  options = None;                                                                                                             # command line options
  inputs = [];                                                                                                                # command line inputs

# Machine variables

  Lochst = "";                                                                                                                # local host name
  Locipa = "";                                                                                                                # local ip address

# Date/time variables

  Yernum = 0;                                                                                                                 # year number
  Mthnum = 0;                                                                                                                 # month number
  Mthnam = [];                                                                                                                # month name
  Daynum = 0;                                                                                                                 # day number
  Daynam = [];                                                                                                                # day name
  Hornum = 0;                                                                                                                 # hours number (24-hour)
  Hor12h = 0;                                                                                                                 # hours number (12-hour)
  Minnum = 0;                                                                                                                 # minutes number
  Secnum = 0;                                                                                                                 # seconds number
  Micsec = 0;                                                                                                                 # microseconds number
  Ampmid = "";                                                                                                                # ampm indicator

# Environment variables

  ds = "/";                                                                                                                   # date separator
  ts = ":";                                                                                                                   # time separator

  Orgdir = "~/";                                                                                                              # original user directory
  Curdir = "";                                                                                                                # current user directory
  Curpth = "";                                                                                                                # current user path

  Jenkins = False;                                                                                                            # script launched by jenkins?

  JENKINS_ENV = \
  {
    'JOB_NAME':        '',                                                                                                    # jenkins job name
    'BUILD_NUMBER':    '',                                                                                                    # The current build number, such as "153"
    'BUILD_ID':        '',                                                                                                    # The current build id, such as "2005-08-22_23-59-59" (YYYY-MM-DD_hh-mm-ss, defunct since version 1.597)
    'BUILD_URL':       '',                                                                                                    # The URL where the results of this build can be found (e.g. http://buildserver/jenkins/job/MyJobName/666/)
    'NODE_NAME':       '',                                                                                                    # The name of the node the current build is running on. Equals 'master' for master node.
    'BUILD_TAG':       '',                                                                                                    # String of jenkins-${JOB_NAME}-${BUILD_NUMBER}. Convenient to put into a resource file, a jar file, etc for easier identification.
    'JENKINS_URL':     '',                                                                                                    # Set to the URL of the Jenkins master that's running the build. This value is used by Jenkins CLI for example
    'EXECUTOR_NUMBER': '',                                                                                                    # The unique number that identifies the current executor (among executors of the same machine) that's carrying out this build. This is the number you see in the "build executor status", except that the number starts from 0, not 1.
    'WORKSPACE':       '',
  };

# General variables

  Errflg = False;                                                                                                             # system error flag
  Errmsg = "";                                                                                                                # system error message

# Message variables

  Msgstr = "";                                                                                                                # message string

# Calculation variables

  Expstr = "";                                                                                                                # expression string
  Expval = decimal.Decimal("0.00");                                                                                           # expression value

# Temporary variables

  intval = 0;                                                                                                                 # integer value (temporary)
  dblval = 0.0;                                                                                                               # double value (temporary)
  decval = None;                                                                                                              # decimal value (temporary)
  bolval = False;                                                                                                             # boolean value (temporary)
  datval = None;                                                                                                              # date value (temporary)
  dtmval = None;                                                                                                              # datetime value (temporary)
  timval = None;                                                                                                              # time value (temporary)
  tmsval = "";                                                                                                                # timestamp value (temporary)
  datdys = None;                                                                                                              # date days (temporary)
  lstval = None;                                                                                                              # list value (temporary)
  fnmval = "";                                                                                                                # filename value (temporary)
  emladr = "";                                                                                                                # email address (temporary)

  yernum = 0;                                                                                                                 # year number (temporary)
  mthnum = 0;                                                                                                                 # month number (temporary)
  daynum = 0;                                                                                                                 # day number (temporary)
  hornum = 0;                                                                                                                 # hour number (temporary)
  minnum = 0;                                                                                                                 # minute number (temporary)
  secnum = 0;                                                                                                                 # second number (temporary)
  micsec = 0;                                                                                                                 # microsecond number (temporary)

# File objects

  Logfil = None;                                                                                                              # initialise log file object

# File name variables

  Ziplist = [];                                                                                                               # list of files in zip archive
  Extract_list = [];                                                                                                          # list of file from facebook api call
  Gzfile = '';                                                                                                                # name of un-gzipped file
  Missing = [];                                                                                                               # missing files list
  Ftp_fillst = [];                                                                                                            # list of files returned by getftp
  Monitor_list = [];                                                                                                          # data activity monitoring list

# Display objects

  display = None;                                                                                                             # virtual display object

# Facebook API variables

  sleep_time = 0;

# CLI Parameter List

  input_lists = [[]];                                                                                                         # empty list of lists

# JSON string from CLI file

  json_obj = {};                                                                                                              # empty json object

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Constructor
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def __init__(self):

# Store command line options and inputs

    self.parser = ArgumentParser();
    self.parser.add_argument("--metrics", dest="metrics", help="Report metrics", default='');
    self.parser.add_argument("--dimensions", dest="dimensions", help="Report dimensions", default='');
    self.parser.add_argument("--start-date", dest="start_date", help="Start date, in YYYY-MM-DD format", default='', type=str);
    self.parser.add_argument("--end-date", dest="end_date", help="End date, in YYYY-MM-DD format", default='', type=str);
    self.parser.add_argument('--rolling-days', dest="rolling_days", help="Number of days Rolling Logic", default=1, type=int);
    self.parser.add_argument('--ignore-header', dest="ignore_header", help="Number of header rows to ingnore in Redshift Copy", default=1, type=int);
    self.parser.add_argument("--start-index", dest="start_index", help="Start index", default=1, type=int);
    self.parser.add_argument("--max-results", dest="max_results", help="Max results", default=20, type=int);
    self.parser.add_argument("--filters", dest="filters", help="Filters", default='country==AU');
    self.parser.add_argument("--sort", dest="sort", help="Sort order", default="-views");
    self.parser.add_argument('--setup', dest="setup", help="Setup", default=False, action='store_true');
    self.parser.add_argument('--no-redshift', dest="load_redshift", help="Load data to Redshift", default=True, action='store_false');
    self.parser.add_argument('--no-s3', dest="load_s3", help="Load data to S3", default=True, action='store_false');
    self.parser.add_argument('--multi-zip', dest="multi_zip", help="Allow Multiple files in zip", default=False, action='store_true');
    self.parser.add_argument('--database', dest="dbname", help="Database Name", default='', type=str);
    self.parser.add_argument('--schema', dest="schema", help="Schema Name", default='');
    self.parser.add_argument('--debug', dest="debug", help="Debug Flag", default=False, action='store_true');
    self.parser.add_argument('--timer', dest="timer", help="Countdown Timer in Seconds", default=0);
    self.parser.add_argument('--input-lists', dest="input_lists", help="Lists of Parameters", default='[[]]');
    self.parser.add_argument('--json-file', dest="json_file", help="JSON input file for complex Parameters", default=self.NULCHR, type=str);
    self.parser.add_argument('--stop-process', dest="stop_process", help="Stop Process at specified position", default=0, type=int);
    self.parser.add_argument('--http-error', dest='http_error', help='HTTP error count before abort', default=5, type=int);
    self.parser.add_argument('--auth_host_name', dest="auth_host_name", help="Host name for OAuth", default='localhost', type=str);
    self.parser.add_argument('--auth_host_port', dest="auth_host_port", help="Localhost port for OAuth", default=[8080, 8090], type=int);
    self.parser.add_argument('--[no]auth_local_webserver', dest="noauth_local_webserver", help="run a local webserver for OAuth", default=True, type=bool);
    self.parser.add_argument('--logging-level', dest="logging_level", default='ERROR');
    self.parser.add_argument('--client_id', dest='c_id', action='store', help='Specifies the DS API client_id. Looks like: 1234567890.apps.googleusercontent.com');
    self.parser.add_argument('--client_secret', dest='secret', action='store', help='Specifies the DS API client_secret. Looks like: 1ab2CDEfghigKlM3OPzyx2Q');
    self.parser.add_argument('--refresh_token', dest='token', action='store', help='Specifies the DS API refresh_token. Looks like: 4/abC1ddW3WJURhF7DUj-6FHq8kkE');
    self.parser.add_argument('inputs', nargs='*');

# Parse command line into options and inputs

    self.options = self.parser.parse_args();
    self.inputs = self.options.inputs;
    self.Debug = self.options.debug;                                                                                          # set global debug mode
    self.sleep_time = int(self.options.timer);                                                                                # set global timer value

    self.input_lists = ast.literal_eval(self.options.input_lists);                                                            # set CLI input list values
    if (self.options.json_file != self.NULCHR):
      self.json_obj = self.json_dict(self.options.json_file);                                                                 # only load if CLI argument is set
    if (self.json_obj == None):
      sys.exit(self.Errmsg);                                                                                                  # exit on bad json file

    pass;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Get object type
# 1. o = object
# Returns connection object type as string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def obj_type(self, o):

    return type(o).__name__;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert string to integer
# 1. Negflg = allow negative number
# 2. Result is put into AnomSys.intval
# 3. Result is set to zero if empty or error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strint(self, Strval, Negflg):

    if (Strval == ""):                                                                                                        # empty string value?
      self.intval = 0;                                                                                                        # set integer value
      return True;                                                                                                            # return ok

    try:
      i = int(Strval);                                                                                                        # convert string if possible

    except:
      self.intval = 0;                                                                                                        # set integer value
      return False;                                                                                                           # return error

    if ((i < 0) and (not Negflg)):                                                                                            # disallow negative?
      self.intval = 0;                                                                                                        # set system double value
      return False;                                                                                                           # return error

    self.intval = i;                                                                                                          # set result value
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert string to positive hex integer
# 1. Assumes a string with up to 4 hex characters, upper or lower case
# 2. Returns hex integer, from 0000h to FFFFh
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strhex(self, s):

    if (s == None):                                                                                                           # null string value?
      self.intval = 0;                                                                                                        # set system int value
      return True;                                                                                                            # return ok

    if (s == ""):                                                                                                             # empty string value?
      self.intval = 0;                                                                                                        # set system int value
      return True;                                                                                                            # return ok

    try:
      i = int(s, 16);                                                                                                         # convert string if possible

    except:
      self.intval = 0;                                                                                                        # set system int value
      return False;                                                                                                           # return error

    self.intval = i;                                                                                                          # set result value
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Print hex string
# s = hex string to print (hex byte string i.e, "1B054D0070")
# ps = print stream
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def prthex(self, s, ps):

    j = len(s);                                                                                                               # get string length
    if (j < 1): return;                                                                                                       # exit if empty string

    for i in range (0, j, 2):                                                                                                 # scan each 2-digit code
      Hexstr = s[i:i+2];                                                                                                      # get 2-digit hex code
      if (self.strhex(Hexstr)): ps.write(chr(self.intval));                                                                   # converted 2-digit hex code?

    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert string to double
# 1. Negflg = allow negative number
# 2. Decflg = allow decimal number
# 3. Result is put into AnomSys.dblval
# 4. Result is set to zero if empty or error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strdbl(self, Strval, Negflg, Decflg):

    if (Strval == ""):                                                                                                        # empty string value?
      self.dblval = 0.0;                                                                                                      # set double value
      return True;                                                                                                            # return ok

    try:
      d = float(Strval);                                                                                                      # convert string if possible

    except:
      self.dblval = 0.0;                                                                                                      # set double value
      return False;                                                                                                           # return error

    if (d < 0 and not Negflg):                                                                                                # disallow negative?
      self.dblval = 0.0;                                                                                                      # set system double value
      return False;                                                                                                           # return error

    if (d != long(d) and not Decflg):                                                                                         # disallow decimal?
      self.dblval = 0.0;                                                                                                      # set system double value
      return False;                                                                                                           # return error

    self.dblval = d;                                                                                                          # set result value
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert string to decimal
# 1. Negflg = allow negative number
# 2. Decflg = allow decimal number
# 3. Result is put into AnomSys.decval
# 4. Result is set to zero if empty or error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strdec(self, Strval, Negflg, Decflg):

    if (Strval == ""):                                                                                                        # empty string value?
      self.decval = decimal.Decimal("0.0");                                                                                   # set decimal value
      return True;                                                                                                            # return ok

    try:
      d = decimal.Decimal(Strval);                                                                                            # convert string if possible

    except:
      self.decval = decimal.Decimal("0.0");                                                                                   # set decimal value
      return False;                                                                                                           # return error

    if (d < 0 and not Negflg):                                                                                                # disallow negative?
      self.decval = decimal.Decimal("0.0");                                                                                   # set decimal value
      return False;                                                                                                           # return error

    if (d != long(d) and not Decflg):                                                                                         # disallow decimal?
      self.decval = decimal.Decimal("0.0");                                                                                   # set decimal value
      return False;                                                                                                           # return error

    self.decval = d;                                                                                                          # set result value
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Round double number to n places
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dblrnd(self, d, n):

    return math.floor((d * pow(10, n)) + 0.5) / pow(10, n);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Round double number s to nearest value n
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dblrxn(self, d, n):

    return self.dblrnd((d / n), 0) * n;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Round decimal number to n places (default 2)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def decrnd(self, d, n=2):

    y = str(10**-n);
    return d.quantize(decimal.Decimal(y), rounding=decimal.ROUND_HALF_UP);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Round decimal number d to nearest value n
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def decrxn(self, d, n):

    return self.decrnd(d / n, 0) * n;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return log (base y) of x
# 1. log (base y) x = ln(x) / ln(y)
# 2. See: Thomas, p323
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def log(self, y, x):

    return (math.log(x) / math.log(y));

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Check number value range (integer, double, decimal)
# 1. Returns true if value in specified range
# 2. Uses Minflg, Maxflg to specify inclusive endpoint
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def inrnge(self, Numval, Minval, Maxval, Minflg, Maxflg):

    ok = False;                                                                                                               # working flag

# Test minimum condition

    if (Minflg):                                                                                                              # inclusive minimum?
      ok = (Numval >= Minval);
    else:
      ok = (Numval > Minval);

    if (not ok): return False;                                                                                                # exit if error

# Test maximum condition

    if (Maxflg):                                                                                                              # inclusive maximum?
      ok = (Numval <= Maxval);
    else:
      ok = (Numval < Maxval);

    if (not ok): return False;                                                                                                # exit if error

# Both conditions ok, return success

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Pad signed integer with leading zeros, in pad length field width
# 1. Only pads if pad length > string length of unsigned integer (*)
# 2. Always truncate to pad length characters (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def padzer(self, Intval, Padlen):

    Strval = str(Intval);                                                                                                     # get string from integer
    return Strval.zfill(Padlen);                                                                                              # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Pad signed integer with leading spaces and sign, in pad length field width
# 1. Only pads if pad length > string length of unsigned integer (*)
# 2. Always truncate to pad length characters (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def padspc(self, Numval, Padlen):

    Strval = str(Numval);                                                                                                     # get string from number
    return Strval.rjust(Padlen);                                                                                              # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Pad string with trailing blanks, in pad length field width
# 1. Only pads if pad length > string length of input string (*)
# 2. Always truncate to pad length characters (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def padblk(self, s, Padlen):

    Strval = s;                                                                                                               # get string value
    if (Strval == None): Strval = "";                                                                                         # clear if null string
    return Strval.ljust(Padlen);                                                                                              # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return string limited to maximum length
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def maxlen(self, s, m):

    Strval = s;                                                                                                               # get string value
    if (Strval == None): Strval = "";                                                                                         # clear if null string
    if (len(Strval) > m): Strval = Strval[0:m];                                                                               # limit to maximum length
    return Strval;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return string wrapped into items of maximum length
# s = String to wrap
# m = Maximum item length
# Returns list of 0+ items of length <= m
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def wrpstr(self, s, m):

    Strval = s;                                                                                                               # get string value
    if (Strval == None): Strval = "";                                                                                         # clear if null string
    return textwrap.wrap(Strval, m);                                                                                          # return wrapped string list

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert string to token string array
# 1. Strval = string to convert
# 2. Delimt = delimiter string to use
# 3. Nulflg = false=do not strip out blank or empty tokens, true=strip out blank or empty tokens
# 4. Result is put into lstval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strtok(self, Strval, Delimt, Nulflg):

# Check input string value

    if (Strval == ""):                                                                                                        # empty input string?
      self.lstval = [];                                                                                                       # clear list value
      return True;                                                                                                            # return ok

# Split string into list

    self.lstval = Strval.split(Delimt);                                                                                       # split string into list

# Return array, and do not strip out empty token strings

    if (not Nulflg): return True;                                                                                             # exit if no null strip

# Return array, and strip out empty token strings

    j = len(self.lstval);                                                                                                     # get list length
    if (j < 1): return True;                                                                                                  # exit if no elements
    for i in range (0, j): self.lstval[i] = self.lstval[i].strip();                                                           # strip space from all elements
    while ("" in self.lstval): self.lstval.remove("");                                                                        # remove all empty elements
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Strip disallowed characters from string
# s = source string
# d = disallowed characters string or list of strings
# Returns stripped string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strdis(self, s, d):

    t = [];

    if (type(d) == str):
      t.append(d);
    else:
      if (type(d) == list):
        t = d;
      else:
        return s;

    j = len(t);

    for i in range(0, j):
      y = s.replace(t[i], "");
      s = y;

    return s;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Replace newline characters with space
# s = source string
# Returns replaced string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixnew(self, s):

    return s.replace(self.nl, self.SPCCHR);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix quote characters in string
# s = string to fix
# Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixqot(self, s):

    return s.replace("'", "''");

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix slash characters in string
# s = String to fix
# Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixslh(self, s):

    return s.replace("/", "\\");

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix backslash characters in string
# s = String to fix
# Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixbsl(self, s):

    return s.replace("\\", "/");

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix space characters in string
# s = string to fix
# d = character to replace space
# Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixspc(self, s, d):

    return s.replace(self.SPCCHR, d);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix percent characters in string
# s = string to fix
# Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixpct(self, s):

    return s.replace("%", "\\\\%");

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Strip high bits from string
# s = string to strip
# Returns stripped string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strbit(self, s):

    Strlst = list(s);                                                                                                         # create string list
    j = len(Strlst);                                                                                                          # get string list length

    for i in range (0, j):                                                                                                    # scan string list
      c = ord(Strlst[i]) & 0x007f;                                                                                            # strip high bit
      Strlst[i] = chr(c);

    return "".join(Strlst);                                                                                                   # return result string

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Fix xhtml text
# 1. Filters illegal characters in xhtml string
# 2. h = string to transform
# 3. Returns fixed string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fixhtm(self, h):

    return h.replace(chr(0x3c),   "&lt;") \
            .replace(chr(0x3e), "  &gt;") \
            .replace(chr(0x22), "&quot;") \
            .replace(chr(0x27),  "&#39;") \
            .replace(chr(0x26),  "&amp;");

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Integer to boolean function
# 1. i = boolean integer (Basic) (False=0, True =-1)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def intbol(self, i):

    return (i == -1);                                                                                                         # return boolean result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set date value and components from today
# 1. Sets: Yernum, Mthnum, Mthnam, Daynum, Daynam
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def curdat(self):

    try:
      self.datval = datetime.date.today();                                                                                    # create date from today

    except:
      self.datval = None;                                                                                                     # clear datetime object
      self.Yernum = 0;                                                                                                        # clear year number
      self.Mthnum = 0;                                                                                                        # clear month number
      self.Mthnam = [];                                                                                                       # clear month names
      self.Daynum = 0;                                                                                                        # clear day number
      self.Daynam = [];                                                                                                       # clear day names
      return False;                                                                                                           # return error

    self.Yernum = self.datval.year;                                                                                           # get year number
    self.Mthnum = self.datval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.datval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.datval.weekday()), self.daynam(self.LNGNAM, self.datval.weekday())];         # get day name list
    self.tmsval = "%04d-%02d-%02d 00:00:00" % (self.Yernum, self.Mthnum, self.Daynum);                                        # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set datetime value and components from now
# 1. Sets: Yernum, Mthnum, Mthnam. Daynum, Daynam, Hornum, Minnum, Secnum, Micsec, tmsval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def curdtm(self):

    try:
      self.dtmval = datetime.datetime.now();                                                                                  # create datetime from now

    except:
      self.dtmval = None;                                                                                                     # clear datetime object
      self.Yernum = 0;                                                                                                        # clear year number
      self.Mthnum = 0;                                                                                                        # clear month number
      self.Mthnam = [];                                                                                                       # clear month names
      self.Daynum = 0;                                                                                                        # clear day number
      self.Daynam = [];                                                                                                       # clear day names
      self.Hornum = 0;                                                                                                        # clear hour number (24-hour)
      self.Minnum = 0;                                                                                                        # clear minute number
      self.Secnum = 0;                                                                                                        # clear second number
      self.Micsec = 0;                                                                                                        # clear microsecond number
      self.tmsval = "";                                                                                                       # clear timestamp string
      return False;                                                                                                           # return error

    self.Yernum = self.dtmval.year;                                                                                           # get year number
    self.Mthnum = self.dtmval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.dtmval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.dtmval.weekday()), self.daynam(self.LNGNAM, self.dtmval.weekday())];         # get day name list
    self.Hornum = self.dtmval.hour;                                                                                           # get hour number (24-hour)
    self.Minnum = self.dtmval.minute;                                                                                         # get minute number
    self.Secnum = self.dtmval.second;                                                                                         # get second number
    self.Micsec = 0;                                                                                                          # get microsecond number (*)
    self.tmsval = "%04d-%02d-%02d %02d:%02d:%02d" % (self.Yernum, self.Mthnum, self.Daynum, self.Hornum, self.Minnum, self.Secnum); # format timestamp string

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set date value and components from date object
# 1. Sets: Yernum, Mthnum, Mthnam, Daynum, Daynam
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def datdat(self, dt):

    if (dt == None): return False;
    self.datval = dt;                                                                                                         # clear date object

    try:
      self.Yernum = self.datval.year;                                                                                         # get year number
      self.Mthnum = self.datval.month;                                                                                        # get month number (zero offset)
      self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                           # get month name list
      self.Daynum = self.datval.day;                                                                                          # get day of month number
      self.Daynam = [self.daynam(self.SHTNAM, self.datval.weekday()), self.daynam(self.LNGNAM, self.datval.weekday())];       # get day name list
      self.tmsval = "%04d-%02d-%02d 00:00:00" % (self.Yernum, self.Mthnum, self.Daynum);                                      # format timestamp string

    except:
      self.datval = None;                                                                                                     # clear datetime object
      self.Yernum = 0;                                                                                                        # clear year number
      self.Mthnum = 0;                                                                                                        # clear month number
      self.Mthnam = [];                                                                                                       # clear month names
      self.Daynum = 0;                                                                                                        # clear day number
      self.Daynam = [];                                                                                                       # clear day names
      self.tmsval = "";                                                                                                       # clear timestamp string
      return False;                                                                                                           # return error

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - get number of days between dates (postgresql timestamps)
# 1. Sets: datdys
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def btwdat(self, ts1, ts2):

    if (ts1 == "" or ts2 == ""): return False;

    if (not self.tmsdat(ts1)): return False;

    datone = self.datval;

    if (not self.tmsdat(ts2)): return False;

    dattwo = self.datval;

    datdys = dattwo - datone;

    self.datdys = datdys.days;

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - get list of dates between days inclusive
# 1. d1 = earliest formatted string date or base date
# 2. d2 = latest formatted string date or number of dates required + or -
# 3. s = separator default ''
# Returns list of dates string format as YYYYMMDD or None if dates invalid
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def datlst(self, d1, d2, s='', template=''):

    dates = [];                                                                                                               # load dates list
    sort = False;                                                                                                             # date sort
    sign = lambda x: (1, -1)[x<0];                                                                                            # temporary sign function

    if len(template) < 1: template = '%s%s%s%s%s' % ('%Y', s, '%m', s, '%d');

    try:
      earliest = parse(d1);
      if type(d2) == int:
        sort = (d2 < 0);
        latest = earliest + datetime.timedelta(days=d2-sign(d2));
      else:
        latest= parse(d2);
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e.args), inspect.stack()[0][3], __name__);
      return None;

    if sort: latest, earliest = earliest, latest;                                                                             # correct for -ve days

    delta = latest - earliest;
    if delta.days < 0:
      self.Errmsg = 'Latest date %s is prior to Earliest date %s' % (d2, d1);                                                 # dates reversed?
      return None;
    for i in range(delta.days + 1):
      dates.append((earliest + datetime.timedelta(days=i)).strftime(template))            ;                                   # add to dates list

    return dates;                                                                                                             # return list of dates

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set datetime value and components from datetime object
# 1. Sets: Yernum, Mthnum, Mthnam. Daynum, Daynam, Hornum, Minnum, Secnum, Micsec, tmsval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dtmdtm(self, dt):

    if (dt == None): return False;
    self.dtmval = dt;                                                                                                         # clear datetime object

    try:
      self.Yernum = self.dtmval.year;                                                                                         # get year number
      self.Mthnum = self.dtmval.month;                                                                                        # get month number (zero offset)
      self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                           # get month name list
      self.Daynum = self.dtmval.day;                                                                                          # get day of month number
      self.Daynam = [self.daynam(self.SHTNAM, self.dtmval.weekday()), self.daynam(self.LNGNAM, self.dtmval.weekday())];       # get day name list
      self.Hornum = self.dtmval.hour;                                                                                         # get hour number (24-hour)
      self.Minnum = self.dtmval.minute;                                                                                       # get minute number
      self.Secnum = self.dtmval.second;                                                                                       # get second number
      self.Micsec = 0;                                                                                                        # get microsecond number (*)
      self.tmsval = "%04d-%02d-%02d %02d:%02d:%02d" % (self.Yernum, self.Mthnum, self.Daynum, self.Hornum, self.Minnum, self.Secnum); # format timestamp string

    except:
      self.dtmval = None;                                                                                                     # clear datetime object
      self.Yernum = 0;                                                                                                        # clear year number
      self.Mthnum = 0;                                                                                                        # clear month number
      self.Mthnam = [];                                                                                                       # clear month names
      self.Daynum = 0;                                                                                                        # clear day number
      self.Daynam = [];                                                                                                       # clear day names
      self.Hornum = 0;                                                                                                        # clear hour number (24-hour)
      self.Minnum = 0;                                                                                                        # clear minute number
      self.Secnum = 0;                                                                                                        # clear second number
      self.Micsec = 0;                                                                                                        # clear microsecond number
      self.tmsval = "";                                                                                                       # clear timestamp string
      return False;                                                                                                           # return error

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set date value and components from postgresql timestamp string
# 1. Sets: Yernum, Mthnum, Mthnam. Daynum, Daynam, Hornum, Minnum, Secnum, Micsec, tmsval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def tmsdat(self, ts):

    self.datval = None;                                                                                                       # clear date object
    self.Yernum = 0;                                                                                                          # clear year number
    self.Mthnum = 0;                                                                                                          # clear month number
    self.Mthnam = [];                                                                                                         # clear month names
    self.Daynum = 0;                                                                                                          # clear day number
    self.Daynam = [];                                                                                                         # clear day names
    self.tmsval = "";                                                                                                         # clear timestamp string

    if (ts == None): return False;
    if (ts == ""): return False;

    try:
      yernum = int(ts[0:4]);
      mthnum = int(ts[5:7]);
      daynum = int(ts[8:10]);
      hornum = 0;
      minnum = 0;
      secnum = 0;

    except:
      return False;                                                                                                           # return error

    try:
      self.datval = datetime.date(yernum, mthnum, daynum);                                                                    # set dateobject

    except:
      self.datval = None;
      return False;                                                                                                           # return error

    self.Yernum = self.datval.year;                                                                                           # get year number
    self.Mthnum = self.datval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.datval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.datval.weekday()), self.daynam(self.LNGNAM, self.datval.weekday())];         # get day name list
    self.tmsval = "%04d-%02d-%02d 00:00:00" % (self.Yernum, self.Mthnum, self.Daynum);                                        # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set datetime value and components from postgresql timestamp string
# 1. Sets: Yernum, Mthnum, Mthnam. Daynum, Daynam, Hornum, Minnum, Secnum, Micsec, tmsval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def tmsdtm(self, ts):

    self.dtmval = None;                                                                                                       # clear datetime object
    self.Yernum = 0;                                                                                                          # clear year number
    self.Mthnum = 0;                                                                                                          # clear month number
    self.Mthnam = [];                                                                                                         # clear month names
    self.Daynum = 0;                                                                                                          # clear day number
    self.Daynam = [];                                                                                                         # clear day names
    self.Hornum = 0;                                                                                                          # clear hour number (24-hour)
    self.Minnum = 0;                                                                                                          # clear minute number
    self.Secnum = 0;                                                                                                          # clear second number
    self.Micsec = 0;                                                                                                          # clear microsecond number
    self.tmsval = "";                                                                                                         # clear timestamp string

    if (ts == None): return False;
    if (ts == ""): return False;

    try:
      yernum = int(ts[0:4]);
      mthnum = int(ts[5:7]);
      daynum = int(ts[8:10]);
      hornum = int(ts[11:13]);
      minnum = int(ts[14:16]);
      secnum = int(ts[17:19]);

    except:
      return False;                                                                                                           # return error

    try:
      self.dtmval = datetime.datetime(yernum, mthnum, daynum, hornum, minnum, secnum);                                        # set datetime object

    except:
      self.dtmval = None;
      return False;                                                                                                           # return error

    self.Yernum = self.dtmval.year;                                                                                           # get year number
    self.Mthnum = self.dtmval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.dtmval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.dtmval.weekday()), self.daynam(self.LNGNAM, self.dtmval.weekday())];         # get day name list
    self.Hornum = self.dtmval.hour;                                                                                           # get hour number (24-hour)
    self.Minnum = self.dtmval.minute;                                                                                         # get minute number
    self.Secnum = self.dtmval.second;                                                                                         # get second number
    self.Micsec = 0;                                                                                                          # get microsecond number (*)
    self.tmsval = "%04d-%02d-%02d %02d:%02d:%02d" % (self.Yernum, self.Mthnum, self.Daynum, self.Hornum, self.Minnum, self.Secnum); # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set time value and components from postgresql timestamp string
# 1. Sets: Hornum, Minnum, Secnum, Micsec, tmsval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def tmstim(self, ts):

    self.dtmval = None;                                                                                                       # clear datetime object
    self.Yernum = 0;                                                                                                          # clear year number
    self.Mthnum = 0;                                                                                                          # clear month number
    self.Mthnam = [];                                                                                                         # clear month names
    self.Daynum = 0;                                                                                                          # clear day number
    self.Daynam = [];                                                                                                         # clear day names
    self.Hornum = 0;                                                                                                          # clear hour number (24-hour)
    self.Minnum = 0;                                                                                                          # clear minute number
    self.Secnum = 0;                                                                                                          # clear second number
    self.Micsec = 0;                                                                                                          # clear microsecond number
    self.tmsval = "";                                                                                                         # clear timestamp string

    if (ts == None): return False;
    if (ts == ""): return False;

    try:
      yernum = int(ts[0:4]);
      mthnum = int(ts[5:7]);
      daynum = int(ts[8:10]);
      hornum = int(ts[11:13]);
      minnum = int(ts[14:16]);
      secnum = int(ts[17:19]);

    except:
      return False;                                                                                                           # return error

    try:
      self.timval = datetime.time(hornum, minnum, secnum);                                                                    # set datetime object

    except:
      self.timval = None;
      return False;                                                                                                           # return error

    self.Hornum = self.dtmval.hour;                                                                                           # get hour number (24-hour)
    self.Minnum = self.dtmval.minute;                                                                                         # get minute number
    self.Secnum = 0;                                                                                                          # get second number
    self.Micsec = 0;                                                                                                          # get microsecond number (*)
    self.tmsval = ts;                                                                                                         # get timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set date value and components from date string (d/m/yyyy...)
# 1. Sets: Yernum, Mthnum, Mthnam, Daynum, Daynam
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strdat(self, ds):

    self.datval = None;                                                                                                       # clear date object
    self.Yernum = 0;                                                                                                          # clear year number
    self.Mthnum = 0;                                                                                                          # clear month number
    self.Mthnam = [];                                                                                                         # clear month names
    self.Daynum = 0;                                                                                                          # clear day number
    self.Daynam = [];                                                                                                         # clear day names
    self.tmsval = "";                                                                                                         # clear timestamp string

    if (ds == None): return False;                                                                                            # exit if null date string
    Ds = ds.strip();
    if (len(Ds) < 1): return False;                                                                                           # exit if empty date string

# Clean date string

    Ds = Ds.replace("/"," ").replace("-"," ").replace(","," ").replace("'","19");

# Check for single day date string if no valid date yet

    try:
      Fd = time.strptime(Ds, "%d");
      Yr = datetime.date.today().year;
      Mn = datetime.date.today().month;
      self.datval = datetime.date(Yr, Mn, Fd.tm_mday);

    except:
      pass;

# Check for short date string if no valid date yet

    if (self.datval == None):
      for Df in self.DFMMTH:
        try:
          Fd = time.strptime(Ds, Df);
          Yr = datetime.date.today().year;
          self.datval = datetime.date(Yr, Fd.tm_mon, Fd.tm_mday);
          if (self.datval != None): break;

        except:
          pass;

# Check for full date string

    if (self.datval == None):
      for Df in self.DFMYER:
        try:
          Fd = time.strptime(Ds, Df);
          self.datval = datetime.date(Fd.tm_year, Fd.tm_mon, Fd.tm_mday);
          if (self.datval != None): break;

        except:
          pass;

    if (self.datval == None): return False;

    self.Yernum = self.datval.year;                                                                                           # get year number
    self.Mthnum = self.datval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.datval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.datval.weekday()), self.daynam(self.LNGNAM, self.datval.weekday())];         # get day name list
    self.tmsval = "%04d-%02d-%02d 00:00:00" % (self.Yernum, self.Mthnum, self.Daynum);                                           # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set date value and components from datetime string (d/m/yyyy 00:00:00)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strdtm(self, ds):

    try:
      self.strtok(ds, " ", False);
    except:
      return False;

    try:
      self.strdat(self.lstval[0]);
    except:
      return False;

    try:
      self.strtim(self.lstval[1]);
      self.lstval = self.DAYBEG;
    except:
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set time value and components from time string (hh:mm:ss)
# 1. Sets: Hornum, Minnum, Secnum
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def strtim(self, ts, Secflg=False):

    self.timval = None;                                                                                                       # clear time object
    self.Hornum = 0;                                                                                                          # clear hour number (24-hour)
    self.Minnum = 0;                                                                                                          # clear minute number
    self.Secnum = 0;                                                                                                          # clear second number
    self.Micsec = 0;                                                                                                          # clear microsecond number
    self.tmsval = "";                                                                                                         # clear timestamp string

    if (ts == None): return False;                                                                                            # exit if null time string
    Ts = str(ts).strip();
    if (len(Ts) < 1): return False;                                                                                           # exit if empty time string

    Ts = Ts.replace(":"," ").replace(","," ").replace("-"," ").replace("."," ");

    for Tf in self.TFMHMS:
      try:
        Td = time.strptime(Ts, Tf);
        self.timval = datetime.time(Td.tm_hour, Td.tm_min, Td.tm_sec);
        if (self.timval != None): break;

      except:
        pass;

    if (self.timval == None): return False;                                                                                   # return error

    self.Hornum = self.timval.hour;                                                                                           # get hour number (zero offset)
    self.Minnum = self.timval.minute;                                                                                         # get minute number (zero offset)
    self.Secnum = self.timval.second;                                                                                         # get minute number (zero offset)
    if (not Secflg):
      self.tmsval = "%02d:%02d" % (self.Hornum, self.Minnum);                                                                 # format timestamp string
    else:
      self.tmsval = "%02d:%02d:%02d" % (self.Hornum, self.Minnum, self.Secnum);                                               # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Returns time value as decimal time (07:30 = 7.5000)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dectim(self, ts):

    self.strtim(ts, True);

# Set midnight time adjustment

    if (ts == self.MIDNIT):
      Adjsec = self.dec("1.0");
    else:
      Adjsec = self.DECZER;

    Td = self.decrnd(self.dec(self.Hornum) + (((self.dec(self.Minnum) * 60) + (self.dec(self.Secnum) + Adjsec)) / 3600), 4);

    return Td;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Make date value from current datetime components
# 1. Uses: Yernum, Mthnum, Daynum
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def makdat(self):

    try:
      self.datval = datetime.date(self.Yernum, self.Mthnum, self.Daynum);                                                     # set date object

    except:
      return False;                                                                                                           # return error

    self.Yernum = self.datval.year;                                                                                           # get year number
    self.Mthnum = self.datval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.datval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.datval.weekday()), self.daynam(self.LNGNAM, self.datval.weekday())];         # get day name list
    self.tmsval = "%04d-%02d-%02d 00:00:00" % (self.Yernum, self.Mthnum, self.Daynum);                                           # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Make datetime value from current datetime components
# 1. Uses: Yernum, Mthnum, Daynum, Hornum, Minnum
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def makdtm(self):

    try:
      self.dtmval = datetime.datetime(self.Yernum, self.Mthnum, self.Daynum, self.Hornum, self.Minnum, self.Secnum);          # set datetime object

    except:
      self.dtmval = None;
      return False;                                                                                                           # return error

    self.Yernum = self.dtmval.year;                                                                                           # get year number
    self.Mthnum = self.dtmval.month;                                                                                          # get month number (zero offset)
    self.Mthnam = [self.mthnam(self.SHTNAM, self.Mthnum), self.mthnam(self.LNGNAM, self.Mthnum)];                             # get month name list
    self.Daynum = self.dtmval.day;                                                                                            # get day of month number
    self.Daynam = [self.daynam(self.SHTNAM, self.dtmval.weekday()), self.daynam(self.LNGNAM, self.dtmval.weekday())];         # get day name list
    self.Hornum = self.dtmval.hour;                                                                                           # get hour number (24-hour)
    self.Minnum = self.dtmval.minute;                                                                                         # get minute number
    self.Secnum = self.dtmval.second;                                                                                         # get second number
    self.Micsec = 0;                                                                                                          # get microsecond number (*)
    self.tmsval = "%04d-%02d-%02d %02d:%02d:%02d" % (self.Yernum, self.Mthnum, self.Daynum, self.Hornum, self.Minnum, self.Secnum); # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Make time value from current datetime components
# 1. Uses: Hornum, Minnum
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def maktim(self):

    try:
      self.timval = datetime.time(self.Hornum, self.Minnum);                                                                  # set time object

    except:
      self.timval = None;
      return False;                                                                                                           # return error

    self.Hornum = self.timval.hour;                                                                                           # get hour number (24-hour)
    self.Minnum = self.timval.minute;                                                                                         # get minute number
    self.Secnum = 0;                                                                                                          # get second number
    self.Micsec = 0;                                                                                                          # get microsecond number (*)
    self.tmsval = "%02d:%02d" % (self.Hornum, self.Minnum);                                                                   # format timestamp string
    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return month name from datetime month index
# i = Name type (0=short, 1=long)
# j = Month index (1..12)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def mthnam(self, i, j):

    if (i < 0) or (i > 1): return "";                                                                                         # exit if out of range
    if (j < 1) or (j > len(self.Mthary[i])): return "";                                                                       # exit if out of range
    return self.Mthary[i][j-1];                                                                                               # return month name

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return day name from datetime day of week index
# i = Name type (0=short, 1=long)
# j = Day index (0..6)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def daynam(self, i, j):

    if (i < 0) or (i > 1): return "";                                                                                         # exit if out of range
    if (j < 0) or (j > len(self.Dayary[i])-1): return "";                                                                     # exit if out of range
    return self.Dayary[i][j];                                                                                                 # return day name

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return generic formatted date/time string from components
# 1. Assumes that datetime components have already been created (*)
# 2. Currently returns date and time string in default "dd/mm/yyyy hh:mm[:ss]" 24-hour format
# 3. Secflg = Seconds include flag
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dtmstr(self, Secflg=False):

    if (not Secflg):
      Dtmstr = "%02d/%02d/%04d %02d:%02d" % (self.Daynum, self.Mthnum, self.Yernum, self.Hornum, self.Minnum);
    else:
      Dtmstr = "%02d/%02d/%04d %02d:%02d:%02d" % (self.Daynum, self.Mthnum, self.Yernum, self.Hornum, self.Minnum, self.Secnum);

    return Dtmstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return generic formatted date string from components
# 1. Assumes that date components have already been created (*)
# 2. Returns date string in default "dd/mm/yyyy" format if Revers = False with parameterized separator and US date flag
# 3. Returns date string in default "yyyy/mm/dd" format if Revers = True with parameterized separator
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def datstr(self, Revers=False, Sep="/", US=False):

    if Sep not in self.SEPLST: Sep = self.SEPLST[0];                                                                          # force slash if invalid separator

    if (Revers):
      Datstr = "%04d%s%02d%s%02d" % (self.Yernum, Sep, self.Mthnum, Sep, self.Daynum);
    else:
      if (US):
        Datstr = "%02d%s%02d%s%04d" % (self.Mthnum, Sep, self.Daynum, Sep, self.Yernum);
      else:
        Datstr = "%02d%s%02d%s%04d" % (self.Daynum, Sep, self.Mthnum, Sep, self.Yernum);
    return Datstr;                                                                                                            # return result

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return re-formatted date string from passed date string
# 1. date_s = date string
# 2. in_fmt = input format
# 3. out_fmt = output format (defaults to YYYY-MM-DD)
# 4. if NoneType returned then check Errmsg for details
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def datcon(self, date_s, in_fmt, out_fmt='%Y-%m-%d'):

    self.Errmsg = '';

    try:
      return datetime.datetime.strptime(date_s, in_fmt).strftime(out_fmt);
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return formatted date/time string for use in filenames
# 1. Currently returns date and time string in default "YYYY-MM-DD_HHMMSS" 24-hour format
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fildtm(self):

    Dtmstr = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S');
    return Dtmstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return dmy date string from components
# 1. Assumes that date components have already been created (*)
# 2. Currently returns date string in default "d/m/yyyy" format (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def datdmy(self):

    Datstr = "%d/%d/%d" % (self.Daynum, self.Mthnum, self.Yernum);
    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return postgres timestamp for current day at 00:00:00 time
# 1. Assumes that date components have already been created (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def daybeg(self):

    self.curdat();

    Datstr = self.datstr(True);
    Datstr += self.SPCCHR + self.DAYBEG;

    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return postgres timestamp for current day at 11:59:59 time
# 1. Assumes that date components have already been created (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def daymid(self):

    self.curdat();

    Datstr = self.datstr(True);
    Datstr += self.SPCCHR + self.DAYMID;

    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return postgres timestamp for current day at 23:59:59 time
# 1. Assumes that date components have already been created (*)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dayend(self):

    self.curdat();

    Datstr = self.datstr(True);
    Datstr += self.SPCCHR + self.DAYEND;

    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return postgres timestamp for start of current week at 00:00:00 time
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def wekbeg(self):

    self.curdat();
    d = self.datval;
    daydif = datetime.date(d.year, d.month, d.day).weekday()
    datval = datetime.date(d.year, d.month, d.day) - datetime.timedelta(days=daydif);
    self.datdat(datval);
    Datstr = self.datstr(True);
    Datstr += self.SPCCHR + self.DAYBEG;

    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return postgres timestamp for end of current week at 23:59:59 time
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def wekend(self):

    self.curdat();
    d = self.datval;
    daydif = datetime.date(d.year, d.month, d.day).weekday()
    datval = datetime.date(d.year, d.month, d.day) - datetime.timedelta(days=daydif-7);
    self.datdat(datval);
    Datstr = self.datstr(True);
    Datstr += self.SPCCHR + self.DAYEND;

    return Datstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return generic formatted time string from components
# 1. Assumes that datetime components have already been created
# 2. Currently returns time string in default "hh:mm[:ss]" 24-hour format
# 3. Secflg = Seconds include flag
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def timstr(self, Secflg=False):

    if (not Secflg):
      Timstr = "%02d:%02d" % (self.Hornum, self.Minnum);
    else:
      Timstr = "%02d:%02d:%02d" % (self.Hornum, self.Minnum, self.Secnum);

    return Timstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return time string in 12-hour format
# 1. Assumes that datetime components have already been created
# 2. Currently returns time string in default "hh:mm[:ss]" 12-hour format
# 3. Secflg = Seconds include flag
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def tim12h(self, Secflg=False):

    self.Ampmid = "am";                                                                                                       # set default ampm indicator
    Hornum = self.Hornum;                                                                                                     # get hours number

    if (Hornum > 11): self.Ampmid = "pm";                                                                                     # reset ampm indicator if needed
    if (Hornum > 12): Hornum -= 12;                                                                                           # reset 12pm+ hours number if needed
    if (Hornum == 0): Hornum = 12;                                                                                            # reset zero hours number if needed
    self.Hor12h = Hornum;                                                                                                     # set hour number (12h format)

    if (not Secflg):
      Timstr = "%02d:%02d%s" % (Hornum, self.Minnum, self.Ampmid);
    else:
      Timstr = "%02d:%02d:%02d%s" % (Hornum, self.Minnum, self.Secnum, self.Ampmid);

    return Timstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return ISO 8601 date string from datetime object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def isodat(self):

    Dtmstr = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "%.02f" % (time.timezone/3600);
    return Dtmstr;                                                                                                            # return result

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return string from boolean
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def bolstr(self, f):

    if (f):
      return self.TRUSTR;
    else:
      return self.FALSTR;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert postgresql boolean string to boolean value
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def bol(self, s):

    if (s.upper() in ["T", "TRUE", "Y", "YES", "1"]):
      return True;
    else:
      return False;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert postgresql decimal string to decimal object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dec(self, s):

    if (s == None or s == ""): s = "0.00";
    return decimal.Decimal(s);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert postgresql timestamp string to datetime object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dtm(self, s):

    self.tmsdtm(s);
    return self.dtmval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert postgresql timestamp string to date object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dat(self, s):

    self.tmsdat(s);
    return self.datval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert postgresql timestamp string to time object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def tim(self, s):

    self.tmstim(s);
    return self.timval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return datetime stamp from current datetime
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def now(self):

    self.curdtm();
    return self.tmsval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return date stamp from current datetime
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def today(self):

    self.curdat();
    return self.tmsval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return datetime stamp from current datetime
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def time(self):

    self.curdtm();
    return self.dtmval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return date stamp from current datetime
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def date(self):

    self.curdat();
    return self.datval;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert date object to postgresql timestamp string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dattms(self, dt):

    if (self.datdat(dt)):
      return self.tmsval;
    else:
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert datetime object to postgresql timestamp string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dtmtms(self, dt):

    if (self.dtmdtm(dt)):
      return self.tmsval;
    else:
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return date string from date object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def das(self, d):

    Datstr = "";                                                                                                              # clear date string
    if (self.datdat(d)): Datstr = self.datstr();                                                                              # set date string
    return Datstr;                                                                                                            # return date string

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return datetime string from date object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def dts(self, d):

    Datstr = "";                                                                                                              # clear date string
    if (self.dtmdtm(d)): Datstr = self.dtmstr();                                                                              # set date string
    return Datstr;                                                                                                            # return date string

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Decode from base64 string to byte string
# s = base64 string
# Returns byte string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def d64(self, s):

    try:
      return base64.decodestring(s);

    except:
      return "";

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Encode to base64 string from byte string
# s = binary string
# Returns base64 string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def e64(self, s):

    try:
      return base64.encodestring(s);

    except:
      return "";

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Check email address syntax
# s = Email address string to check
# This function tests email address and returns false if any of the following are true:
#   1. First character = "."
#   2. Last character = "."
#   3. Any occurence of ".."
#   4. No "@" character
#   5. Last character of name part of email address = "."
#   6. Invalid characters in name part of email address
#   7. First character of domain part of email address = "."
#   8. Invalid characters in domain part of email address
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def chkeml(self, s):

    S = s.strip();                                                                                                            # copy and format input string

    if (S == ""): return False;                                                                                               # exit if empty string

# Check for "." as first character in domain

    if (S[0] == ".") : return False;                                                                                          # exit if first character is "."

# Check for "." as last character in domain

    if (S[-1] == ".") : return False;                                                                                         # exit if last character is "."

# check for ".." in whole email address (dont test the last character as already eliminated in previous check)

    if (S.find("..") > -1): return False;                                                                                     # exit if ".."

# locate position of first "@" character

    j = len(S);
    n = S.find("@");
    if (n < 0): return False;                                                                                                 # exit if "@" sign missing
    if ((n < 1) or (n > j - 2)): return False;                                                                                # exit if "@" is first or last character

# Check the name part of the email address

    Emlnam = S[:n];                                                                                                           # get name end of email address

# Check for "." as last character

    if (Emlnam[-1] == "."): return False;                                                                                     # name ends starts with "."

    j = len(Emlnam);                                                                                                          # get string length
    for i in range (0, j):                                                                                                    # scan string
      c = Emlnam[i];                                                                                                          # get next character
      if (c in self.DISEML): return False;                                                                                    # exit if disallowed character

# Check the domain part of the email address

    Emldom = S[(n + 1):];                                                                                                     # get domain end of email address

# Check for "." as first character

    if (Emldom[0] == "."): return False;                                                                                      # domain starts with "."

# Check for no dot in domain

    n = Emldom.find(".");
    if (n < 0): return False;

# Check for invalid characters

    j = len(Emldom);                                                                                                          # get string length
    for i in range (0, j):                                                                                                    # scan string
      c = Emldom[i];                                                                                                          # get next character
      if (c in self.DISEML): return False;                                                                                    # exit if disallowed character

    return True;                                                                                                              # return ok

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Get email address from string
# s = String to extract email address from
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def streml(self, s):

    Emlstr = s.strip();

    if (Emlstr == ""): return False;

    self.strtok(Emlstr, " ", True);                                                                                           # get elements

    j = len(self.lstval);

    for i in range (0, j):

      Chkstr = self.lstval[i];

      if (self.chkeml(Chkstr)):                                                                                               # valid email?
        self.emladr = Chkstr;
        return True;

    self.emladr = "";

    return False;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return python list from postgresql array
# 1. Pgastr = An array string from postgresql
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def arrlst(self, Pgastr):

# Replace braces with square brackets

    Lststr = Pgastr.replace("{","[").replace("}","]");

    try:
      Pylist = eval(Lststr);                                                                                                  # create list from string

    except:
      Pylist = [];                                                                                                            # create empty list

    return Pylist;                                                                                                            # return python list

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Thousands comma separator function
# 1. Returns formatted string
# 2. a = Amount will accept float, decimal, integer or string
# 3. t = Format template string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fmtflt(self, a, t = FLTFMT):

    amtstr = t % a;                                                                                                           # set string to format
    p = re.compile(r"(\d)(\d\d\d[.,])");                                                                                      # get pattern object
    while True:                                                                                                               # loop
      amtstr, n = re.subn(p,r"\1,\2",amtstr);                                                                                 # make string from object pattern
      if (n < 1): break;                                                                                                      # if no more substitutions
    return amtstr;                                                                                                            # return formatted string

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - List to tuple
# 1. Returns list formatted as tuple
# 2. l = List
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def lsttup(self, l):

    return str(l).replace('[','(').replace(']',')');

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Virtual display
# 1. v = visible: 0 = False, 1 = True
# 2. w = width in pixels
# 3. h = height in pixels
# 4. s = start or stop flag
# Sets AnomSys.display to None on display.stop()
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def virtual_display(self, v=0, w=1024, h=768, start=True):

    if Display == None: return;                                                                                               # running in windows?

    if self.display != None:
      if (start):
        pass;                                                                                                                 # if running and told to start, do nothing
      else:
        self.display.stop();                                                                                                  # stop display
        self.display = None;                                                                                                  # destroy display object
    else:
      self.display = Display(visible=v, size=(w, h));                                                                         # create display object
      self.display.start();                                                                                                   # start display

    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Mechanize login
# 1. url     = web url to attempt login
# 2. login   = dictionary of login credentials
# 3. headers = list of tuples of header strings set by default
# 4. fnr     = form number defaults to 0
# 5. badurl  = url to expect if bad credentials
# 6. action  = button name to click
# Returns None if un-successful else returns browser object (Check AnomSys.Errmsg for code/reason)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def mechanize_login(self, url, login, headers=WEBHDR, fnr=0, badurl=BADURL, action='signIn'):

    self.Errmsg = '';

# Create objects and initialise

    br = mechanize.Browser();                                                                                                 # creatre a browser object
    cj = cookielib.LWPCookieJar();                                                                                            # create a cookie Jar
    br.set_cookiejar(cj);                                                                                                     # assign to browser
    br.set_handle_robots(False);                                                                                              # disable robots.txt
    br.addheaders = headers;                                                                                                  # set header strings from list

# Attempt to log into the page

    cur_url = url;
    try:
      br.open(cur_url);                                                                                                       # open url
      for cred in login:
        br.select_form(nr=fnr);                                                                                               # select form 0
        key = cred.keys()[0];
        br.form[key] = cred[key][0];                                                                                          # set credentials from login list
        time.sleep(2);
        br.submit(name=action);                                                                                               # submit form
        time.sleep(2);
        cur_url = br.geturl();
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e.args), inspect.stack()[0][3], __name__);
      return None;

    if cur_url == badurl:
      self.Errmsg = 'Error: Invalid Credentials returned: {0} function:{1} module:{2}'.format(cur_url, inspect.stack()[0][3], __name__);
      return None;
    return br;                                                                                                                # return browser object

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Selenium login
# 1. url     = web url to attempt login
# 2. login   = dictionary of login credentials
# 3. prefs   = list of preferences
# 4. fnr     = form number defaults to 0
# 5. badurl  = url to expect if bad credentials
# Returns None if un-successful else returns browser object (Check AnomSys.Errmsg for code/reason)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def selenium_login(self, url, login, prefs=FOXPRF):

    self.Errmsg = '';

    self.virtual_display();                                                                                                   # start virtual display for firefox

# Create objects and initialise

    profile = webdriver.FirefoxProfile();                                                                                     # create profile object
    for pref in prefs:
      profile.set_preference(pref[0], pref[1]);                                                                               # set preferences

    driver = webdriver.Firefox();                                                                                             # start firefox

    try:
      driver.implicitly_wait(10);                                                                                             # seconds
      driver.get(url);                                                                                                        # request page
      driver.implicitly_wait(10);                                                                                             # seconds
      for cred in login:
        key = cred.keys()[0];
        form_data = driver.find_element_by_id(key);
        form_data.send_keys(cred[key][0]);
        driver.find_element_by_id(cred[key][1]).click();                                                                      # simulate click on action button
        driver.implicitly_wait(10);                                                                                           # seconds
    except Exception as e:
      driver.implicitly_wait(10);                                                                                             # seconds
      driver.get_screenshot_as_file('Login_Page.png');
      self.Errmsg = "{0}: {1} function: {2} module: {3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

    return driver;                                                                                                            # return firefox browser object;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Send Email message
# Fraddr = From address
# Toaddr = To address as address or list of addresses
# Smtpsv = Mail server
# Smtppt = Server port
# Smtpus = Username
# Smtppw = Password
# Subject = Email subject line
# Body = content of message
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def snd_eml(self, Fraddr, Toaddr, Smtpsv, Smtppt, Smtpus, Smtppw, Subject, Body):

    Smtses = None;                                                                                                            # clear session object
    self.Errmsg = '';

    if len(Toaddr) < 1:
      if self.Debug: self.wrt_log('No Email Address');
      return True;                                                                                                            # nothing to delete

    Toaddr_list = [];                                                                                                         # initialise list

    if (type(Toaddr) == str):
      Toaddr_list.append(Toaddr);                                                                                             # put single filename into list
    else:
      if (type(Toaddr) == list):
        Toaddr_list = Toaddr;                                                                                                 # assign list

    try:
      Smtses = smtplib.SMTP(Smtpsv, Smtppt);                                                                                  # create smtp session object
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

    Smtses.ehlo()
    Smtses.starttls()
    Smtses.ehlo()
    Smtses.login(Smtpus, Smtppw)

    msg = MIMEMultipart();
    msg['From'] = Fraddr;
    msg['To'] = Toaddr[0];
    if len(Toaddr) > 1:
      msg['cc'] = ', '.join(Toaddr[1:]);
    msg['Subject'] = Subject;
    msg.attach(MIMEText(Body, 'plain'));

    try:
      Smtses.sendmail(Fraddr, Toaddr, msg.as_string());                                                                       # send email to smtp host
      Smtses.quit();                                                                                                          # quit smtp session
      return True;                                                                                                            # return ok
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      Smtses.quit();                                                                                                          # quit smtp session
      return False;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Strip non ascii characters from input string
# Returns stripped string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def nonasc(self, s):

    stripped = (c for c in s if 0 < ord(c) < 127);

    return ''.join(stripped);


#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Convert json object to= python dictionary
# f = filename, json object
# keyname = mandatory keyname for object or sting but not required for filename
# Returns dictionary possibly containing appropriate unicode strings
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def json_dict(self, f):

    if len(f) < 1:
      return None;
    try:
      if type(f) == str:                                                                                                      # filename ?
        with open(f) as j_file:
          j_obj = json.load(j_file);
      elif type(f) == file:
        j_obj = json.load(j_file);
      elif type(f) == dict:                                                                                                   # json object ?
        j_obj = f;
      else:
        self.Errmsg = "TypeError: Type not matched to function requirements in function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
        return None;

      return jsonpickle.decode(jsonpickle.encode(j_obj));                                                                     # read json object as python dictionary
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Return list of files matching a list of wildcard templates
# f_list = list of files to match
# m_list = list of masks to match to f_list
# Returns list of matched files
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def wild_match(self, f_list, m_list):

    fileList = [];
    for mask in m_list:
        template = re.compile(fnmatch.translate(mask)).match;
        for f_name in filter(template, f_list):
          fileList.append(f_name);
    return fileList;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create ftp connection
# 1. h = ftp host
# 2. u = ftp user
# 3. h = ftp password
# Returns ftp connection object or None
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def makftp(self, h, u, p, logging=True):

    self.Errmsg = '';

    try:
      ftp_client = FTP(h, user=u, passwd=p);
      if logging: self.wrt_log('Connected to %s' % h);
      return ftp_client;
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Close ftp connection
# 1. c = ftp client
# Closes ftp client
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def clsftp(self, c, logging=True):

    c.close();
    if logging: self.wrt_log('Disconnected from %s' % c.host);

    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Get file from ftp server
# 1. c = client
# 2. f = file or list of files (wild cards allowed)
# 3. rd = remote directory
# 4. ld = local directory
# Returns list of file retrieved, sets self.Missing for files not found on remote server returns False on error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def getftp(self, h, u, p, f, rd='', ld='', logging=True):

    c = self.makftp(h, u, p,);
    if not c: return False;

    self.Errmsg = '';
    self.Ftp_fillst = [];                                                                                                     # constructed list of files
    ftp_fil_lst = [];

    f_list = [];

    if len(ld) > 0 : ld = '%s/' % ld.rstrip(self.fs);                                                                         # sanitise local directory path name
    if len(f) < 1: return True;

    if type(f) in [str, unicode]:
      f_list.append(str(f));
    elif type(f) == list:
      f_list = f;
    else:
      self.Errmsg = "TypeError: Type not matched to function requirements in function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
      return False;

    self.Missing.extend(f_list);

    try:
      if len(rd) > 0:
        c.cwd(rd);                                                                                                            # set remote directory path
        self.wrt_log('Remote directory set to %s' % rd);
      try:
        nlst = c.nlst();                                                                                                      # get possible list of files
      except:
        self.Errmsg = 'No files to retrieve from remote server';
        return False;

      for file in f_list:
        if file in nlst:
          ftp_fil_lst.append(file);
      for file in ftp_fil_lst:
        if logging: self.wrt_log('Retrieving %s' % file);
        c.retrbinary('RETR %s' % file, open('%s%s' % (ld, file),'wb').write);                                                 # get the file to local storage
        if logging: self.wrt_log('Retrieved %s' % file);
        self.Ftp_fillst.append(file);                                                                                         # add file to file list
        self.Missing.remove(file);                                                                                            # remove from missing file list
        time.sleep(1);
    except Exception as e:
      self.clsftp(c);
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

    self.clsftp(c);
    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - delete file from ftp server
# 1. c = client
# 2. f = file or list of files (wild cards allowed)
# 3. rd = remote directory
# Returns list of files deleted and returns false on error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def delftp(self, c, f, rd='', logging=True):

    c = self.makftp(h, u, p,);
    if not c: return False;

    self.Errmsg = '';
    self.Ftp_fillst = [];                                                                                                     # constructed list of files
    f_list = [];

    if len(f) < 1: return True;

    if type(f) in [str, unicode]:
      f_list.append(str(f));
    elif type(f) == list:
      f_list = f;
    else:
      self.Errmsg = "TypeError: Type not matched to function requirements in function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
      return False;

    try:
      if len(rd) > 0: c.cwd(rd);                                                                                              # set remote directory path
      for file in f_list:
        nlst = c.nlst(file);                                                                                                  # get possible list of files
        if len(nlst) > 0: self.Ftp_fillst.extend(nlst);                                                                       # add to get_list
      if len(self.Ftp_fillst) > 0:
        for file in self.Ftp_fillst:
          c.delete(file);                                                                                                     # delete the remote file
        if logging:
          self.wrt_log('Deleted remote file %s' % file);
    except Exception as e:
      self.clsftp(c);
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

    self.clsftp(c);
    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create sftp connection
# 1. h = ftp host
# 2. u = ftp user
# 3. h = ftp password
# 4. hp = host port defaulted to 22
# 5. ssh = path/to/keyfile
# Returns sftp connection object or None
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def mksftp(self, h, u, p, hp=22, ssh='', logging=True):

    self.Errmsg = '';

    try:
      if len(ssh) > 0:
        sftp_client = pysftp.Connection(h, username=u, private_key=ssh);
      else:
        sftp_client = pysftp.Connection(h, username=u, password=p);
      if logging: self.wrt_log('Connected to %s' % h);
      return sftp_client;
    except Exception as e:
      self.Errmsg = "{0}: {1}: Function: {2} Module: {3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Delete file or list of files
# 1. f = file or list of files to delete
# 6. logging will occur if not set by caller
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def del_fil(self, f, logging=True):

    self.Errmsg = '';

    if len(f) < 1:
      if self.Debug:
        self.wrt_log('No File to Delete');
        return True;                                                                                                          # nothing to delete

    f_list = [];                                                                                                              # initialise list

    if (type(f) == str):
      f_list.append(f);                                                                                                       # put single filename into list
    else:
      if (type(f) == list):
        f_list = f;                                                                                                           # assign list

    for file in f_list:
      try:
        os.remove(file);                                                                                                      # delete source file
        if logging: self.wrt_log('Deleted %s' % file);
      except Exception as e:
        self.Errmsg = "{0}: {1} function:{2} line number: {3} module:{4}".format(type(e).__name__, str(e), inspect.stack()[0][3], inspect.stack()[0][2], __name__);
        return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Delete file or list of files from remote server (ssh)
# 1. f = file or list of files to delete
# 2. h = ssh host
# 3. u = ssh user
# 4. p = ssh password
# 5. d = ssh directory
# 6. logging will occur if not set by caller
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def del_src(self, f, h, u, p, d, logging=True):

    self.Errmsg = '';

    if len(f) < 1: return True;                                                                                               # nothing to delete

    f_list = [];                                                                                                              # initialise list

    if (type(f) == str):
      f_list.append(f);                                                                                                       # put single filename into list
    else:
      if (type(f) == list):
        f_list = f;                                                                                                           # assign list

    ssh = paramiko.SSHClient();                                                                                               # Create client object
    ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")));                                         # handle authentication keys

    try:
      ssh.connect(h, username=u, password=p);                                                                                 # connect to remote host
    except paramiko.ssh_exception.AuthenticationException as e:
      self.Errmsg = '{0}: Connection to host: {1} - {2}'.format(type(e).__name__, h, e);
      if logging: self.wrt_log(self.Errmsg);
      ssh.close();
      return False;

    sftp = ssh.open_sftp();                                                                                                   # create sftp connection

    try:
      sftp.chdir(d);                                                                                                          # move to source directory
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, str(e.strerror), d);
      if logging: self.wrt_log(self.Errmsg);
      sftp.close();
      ssh.close();
      return False;

    for file in f_list:
      try:
        sftp.remove(file);                                                                                                    # delete the file
        if logging: self.wrt_log('Removed via sftp %s' % file);
      except IOError as e:
        self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, str(e.strerror), file);
        if logging: self.wrt_log(self.Errmsg);

# Close connections

    sftp.close();
    ssh.close();

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create gzipped file
# 1. f = filename in current path
# Creates gzipped file in format 'f.gz'
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def file_gz(self, f):

    self.Errmsg = '';

    try:
      f_in = open(f, 'rb');                                                                                                   # open input file
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;

    f_name = '%s%s' % (f, self.GZFEXT);

    try:
      f_out = gzip.open(f_name, 'wb');                                                                                        # create output file overwrite if exists
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      f_in.close();                                                                                                           # close input
      return False;

    try:
      f_out.writelines(f_in);                                                                                                 # write input file records to output file
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      f_in.close();                                                                                                           # close input
      f_out.close();                                                                                                          # close output
      return False;

    f_out.close();                                                                                                            # close output
    f_in.close();                                                                                                             # close input

    if not self.del_fil(f, logging=self.Debug):
      return false;                                                                                                           # delete source file

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Extract from gzipped file
# 1. f = filename in current path
# Unpacks gzipped file
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def file_ungz(self, f):

    self.Errmsg = '';

    try:
      base_file = os.path.splitext(f)[0];
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} line number: {3} module:{4}".format(type(e).__name__, str(e), inspect.stack()[0][3], inspect.stack()[0][2], __name__);
      return False;

    try:
      f_in = gzip.open(f, 'rb');                                                                                              # open input file
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} line number: {3} module:{4}".format(type(e).__name__, str(e), inspect.stack()[0][3], inspect.stack()[0][2], __name__);
      return False;

    try:
      f_out = open(base_file, 'wb');                                                                                          # create output file overwrite if exists
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} line number: {3} module:{4}".format(type(e).__name__, str(e), inspect.stack()[0][3], inspect.stack()[0][2], __name__);
      f_in.close();
      return False;

    try:
      f_out.write(f_in.read());                                                                                               # write input file records to output file
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} line number: {3} module:{4}".format(type(e).__name__, str(e), inspect.stack()[0][3], inspect.stack()[0][2], __name__);
      f_out.close();                                                                                                          # close output
      f_in.close();                                                                                                           # close input
      return False;

    f_out.close();                                                                                                            # close output
    f_in.close();                                                                                                             # close input

    self.Gzfile = base_file;                                                                                                  # set Gzfile from base file name
    if not self.del_fil(f, logging=self.Debug):                                                                               # delete source gz file
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Zip file
# 1. f = file or list of files
# 2. z = zip archive name
# 3. mode = create ('w') or append ('a') defaults to 'w'
# Zips all files into a zip archive
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def zip_file(self, f, z, mode='w'):

    self.Errmsg = '';

    if len(f) < 1: return True;                                                                                               # nothing to zip

    f_list = [];                                                                                                              # initialise list

    if (type(f) == str):
      f_list.append(f);                                                                                                       # put single filename into list
    else:
      if (type(f) == list):
        f_list = f;                                                                                                           # assign list

    l = len(f_list);
    comp = zipfile.ZIP_DEFLATED;
    Zipfil = zipfile.ZipFile(z, mode);

    try:
      for file in f_list:
        Zipfil.write(file, compress_type = comp);
      Zipfil.close();
    except Exception as e:
      self.Errmsg = "{0}: {1} {2}".format(type(e).__name__, str(e), z);
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Unzip file
# 1. f = filename in current path
# 2. multi = expect multiple files in archive (defaults to False)
# Unzips all files within a zip archive
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def unzip_file(self, f, multi=False):

    self.Errmsg = '';
    f_list = [];
    self.Ziplist = [];

    if len(f) < 1:
      anom.wrt_log('No file passed to function:{0}'.format(inspect.stack()[0][3]));
      return True;

    if type(f) in [str, unicode]:
      f_list.append(str(f));
    elif type(f) == list:
      f_list = f;
    else:
      self.Errmsg = "TypeError: Type not matched to function requirements in function:{0} module:{1}".format(inspect.stack()[0][3], __name__);
      return False;

    for zip_file in f_list:

      try:
        Zipfil = zipfile.ZipFile(zip_file);                                                                                   # get zip file

        Ziplst = Zipfil.namelist();                                                                                           # get list of files in zip archive
        if multi: self.wrt_log('Files in %s %s' % (zip_file, str(Ziplst)));

# Unpack file contents

        if (not multi):
          if (len(Ziplst) != 1):
            self.Errmsg = "%d files in zip archive. There should only be 1" % l;                                              # report excess files
            return False;

        for file in Ziplst:                                                                                                   # scan zip entries
          f_out = open(file, 'wb');                                                                                           # open output file
          f_out.write(Zipfil.read(file));                                                                                     # write output file
          f_out.close();                                                                                                      # close output file
          self.Ziplist.append(file);

      except Exception as e:
        self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
        return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Determine compressed file type
# 1. f = filename to check
# Returns file extetension if compressed or CSVEXT if not
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def cmp_typ(self, f):

    l = max(len(x) for x in self.FILE_MAGIC);                                                                                 # get length of longest signature

    with open(f) as inpfil:
      c = inpfil.read(l);                                                                                                     # read l bytes from file
    for m, t in self.FILE_MAGIC.items():
      if c.startswith(m):
        return t;                                                                                                             # return the file type dictionary list element
    return [self.CSVEXT, 'Delimited File'];                                                                                   # assume CSV content

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Execute O/S command
# 1. c = command line to execute
# 2. logging = allow capture of external output to log file. If you need the output as a result, do not set logging True.
# Returns list containing [status and [stdout, stderr]]
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def exec_cmd(self, c, logging=False):

    p = subprocess.Popen(c, stdout=subprocess.PIPE,  stderr=subprocess.PIPE, shell=True);                                     # execute command line

    r = None;                                                                                                                 # initialise return code
    prev = '';                                                                                                                # initialise comparison string
    while r is None:
      if logging:
        output = p.stdout.readline();                                                                                         # get any output till now
        if output != prev:
          self.wrt_log(output.rstrip());                                                                                      # if different from previous output, write to log file
          prev = output;                                                                                                      # store previous output
      r = p.poll();

    r_list = p.communicate();                                                                                                 # extract result list

    return r, r_list;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create log file
# f = filename to create
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def crt_log(self, f):

    self.Errmsg = '';

    if (self.Logfil != None):
      if (not self.Logfil.closed):
        self.Errmsg = 'Log File %s is open' % self.Logfil.name;
        return False;

    dtm = self.fildtm();                                                                                                      # date/time stamp for log file name

    try:
      self.Logfil = open('%s_%s%s' % (f, dtm, self.LOGEXT), 'wb');
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Obfuscate string object
# 1. s = string to obfuscate
# 2. o = key or list of list of keys to obfuscate
# Returns obfuscated string
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def obfuscate(self, s, o):

    if len(o) > 0:
      o_list = [];                                                                                                            # initialise list
      if (type(o) == str):
        o_list.append(t);                                                                                                     # put single tag into list
      else:
        if (type(o) == list):
          o_list = o;                                                                                                         # assign list
    else:
      return s;

    for key in o_list:
      s = s.replace(key, '*' * len(key));                                                                                     # obfuscate string

    return s;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Write to log file
# 1. m   = message to log
# 2. con = output message to console if True
# Assumes log file already open and will output to console and return without error if no log file
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def wrt_log(self, m, con=True):

    dtm = self.fildtm();                                                                                                      # get actual time now

    logflg = False;

    if (self.Logfil != None):
      logflg = (not self.Logfil.closed);

# Output to console for monitoring

    if con: self.prtstr('Logged = %s %s: %s%s' % (str(logflg), dtm, m, self.nl));

# Return without error if no log file

    if not logflg: return True;

# Output to log file if open

    try:
      self.Logfil.writelines('%s: %s%s' % (dtm, m, self.crlf));                                                               # datestamped output message
      self.Logfil.flush();
    except IOError as e:
      self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Close log file
# Will check prior to close if log file existsw and is open
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def cls_log(self):

    if (self.Logfil != None):
      if (not self.Logfil.closed):
        self.Logfil.close();                                                                                                  # close the file
    self.Logfil = None;                                                                                                       # set file object back to None

    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Set html markup tags
# 1. t = tag or list of tags to set
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def set_html(self, t):

    tag_on = self.NULCHR;
    tag_off = self.NULCHR;

    if len(t) > 0:

      t_list = [];                                                                                                            # initialise list

      if (type(t) == str):
        t_list.append(t);                                                                                                     # put single tag into list
      else:
        if (type(t) == list):
          t_list = t;                                                                                                         # assign list

      on = ['<%s><%s>' % (self.MARKUP_TAGS['HTML'][0], self.MARKUP_TAGS['NEWLINE'][0])];
      off = ['<%s><%s>' % (self.MARKUP_TAGS['NEWLINE'][1], self.MARKUP_TAGS['HTML'][1])];

      c = 0;                                                                                                                  # initialise counter

      for tag in t_list:
        if self.MARKUP_TAGS.has_key(tag):
          on.append('<%s>' % self.MARKUP_TAGS[tag][0]);
          off.insert(0, '<%s>' % self.MARKUP_TAGS[tag][1]);
          c += 1;                                                                                                             # at least 1 valid tag found

      if c > 0:
        tag_on = self.NULCHR.join(on);
        tag_off = self.NULCHR.join(off);

    return tag_on, tag_off;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Print string to console
# 1. s = string to print
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def prtstr(self, s):

    if (s == None): return;
    sys.stdout.write(s);
    sys.stdout.flush();
    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Error Exit
# 1. m = error message
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def err_exit(self, m):

    self.cls_log();                                                                                                           # close log file
    os.chdir(self.Orgdir);                                                                                                    # change to original directory
    sys.exit(m);                                                                                                              # exit with error message
    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Produce a count down console output
# 1. s = seconds to countdown
# 2. l = length of banner
# 3. ci = continuous integration flag (don't print timer banner if continuous integration is on)
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def countdown(self, s, l=50, ci=True):

    if s == 0: return;
    for i in range(0, s+1):
      time.sleep(1);
      if not ci:
        percent = float(i) / s;
        hashes = '#' * int(round(percent * l));
        spaces = ' ' * (l - len(hashes));
        self.prtstr("\rPercent: [{0}] {1}%".format(hashes + spaces, int(round(percent * 100))));
    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Reporting Do Nothing function
# 1. a = Action not done as a string
# Report that nothing was done and return True
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def do_nothing(self, a='Do'):

    self.wrt_log('Nothing to %s' % a);
    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Get Facebook access token
# 1. app_id = Facebook registered app id
# 2. app_secret = Facebook registered app secret
# Returns current refreshed app_token
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def get_fb_token(self, app_id, app_secret):

    payload = {'grant_type': 'client_credentials', 'client_id': app_id, 'client_secret': app_secret};

    file = requests.post('https://graph.facebook.com/oauth/access_token?', params = payload);
    if file.status_code == 200:
      return file.text.split("=")[1];
    else:
      self.Errmsg = 'Request Error [{0}]: {1} {2} {3}'.format(file.status_code, str(self.json_dict(json.loads(file.text))), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Facebook API pull function
# 1. p = Parameter list for facebook credentials and client ID
# 2. f = file name list
# 3. m = metrics to pull for client
# 4. c = counter flag (defaults to False)
# Pull data from Facebook using the Facebook API. Returns True and sets Facebook File List
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def fb_api_pull(self, p, f, m, c=False):

# Check that files list and metrics list have same number of elements

    if (len(f) != len(m)):
      self.Errmsg = 'Facebook files dont match facebook metrics f = %d : m = %d' % (len(f), len(m));
      return False;

# Clear list of returned files

    self.Extract_list = [];

# Check parameter length

    param_count = 4;

    if (len(p) < param_count):
      self.Errmsg = 'Not enough parameters - %d supplied, %d required' % (len(p), param_count);
      return False;

# Set credential constants passed from caller

    my_app_id       = p[0];
    my_app_secret   = p[1];
    my_access_token = p[2];
    account_id      = p[3];

# get list of account ids

    if len(account_id) < 1:
      self.Errmsg = 'No Account Id specified. Please correct prior to re-running this process';
      return False;

    a_list = [];                                                                                                              # initialise list

    if (type(account_id) == str):
      a_list.append(account_id);                                                                                              # put single tag into list
    else:
      if (type(account_id) == list):
        a_list = account_id;                                                                                                  # assign list

#    my_access_token = self.get_fb_token(my_app_id, my_app_secret);
#    if my_access_token == None: return False;                                                                                 # error during authentication

# Initialize Facebook sessions

    session = FacebookSession(my_app_id, my_app_secret, my_access_token);
    self.wrt_log('Session Object created %s' % str(session));                                                                 # log the event
    api = FacebookAdsApi(session);
    self.wrt_log('AdsApi Object created %s' % str(api));                                                                      # log the event

# Get running time and put it into file name

    runtime = self.fildtm();

    FacebookAdsApi.set_default_api(api);

# Get account object(s)

    account = [];
    for i in range(0, len(a_list)):
      account.append(AdAccount(a_list[i]));
      self.wrt_log('AdAccount Object created %s' % str(account[i]));                                                          # log the event

# Loop through the file names to extract data into

    for i in range(0, len(f)):

# Allow for rate limit timeout to expire at facebook

      if (i > 0): self.countdown(self.sleep_time);

# Extract data from facebook

      f_name = '%s%s%s' % (runtime, f[i].rstrip('.'), self.CSVEXT);                                                           # set filename
      try:
        json_list = [];
        with open(f_name, 'wb') as outfil:
          for a in range(0, len(a_list)):                                                                                     # loop through account objects for this file type
            stats = account[a].get_report_stats(params=m[i]);                                                                 # get the data from facebook api
            self.wrt_log('%d JSON Objects retrieved' % len(stats));                                                           # log the event
            for stat in stats:
              json_list.append(json.dumps(dict(stat.items())));
          outfil.write("\n".join(json_list));                                                                                 # write json object to file
        self.wrt_log('Created %s' % f_name);                                                                                  # log the event
      except Exception as e:
        self.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
        self.Missing.append(str(e));
        return False;

# Save file name

      self.Extract_list.append(f_name);

    self.wrt_log('%d files retrieved' % len(self.Extract_list));                                                              # log the event

# Return to True caller

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Monitoring function to construct SQL statements for later execution to database
# 1. t = start timestamp
# 2. c = python calling script
# 3. s = table schema
# 4. k = key for monitoring function
# 5. v = value passed. can be of any type (k = 'Data' {key: [value, source, destination, recs]},  k = 'Requests' = value)
# Creates a list of SQL statments and stores in self.Monitor_list for later execution to Redshift
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def monitor(self, t, c, s, k, v):

    self.curdtm();
    end_time = self.tmsval;

    try:
      MONITOR = self.json_obj['Monitoring'];                                                                                  # extract monitoring object
      TABLE = MONITOR[k];                                                                                                     # get table name
      VALUES_LIST = [];

      if (k == 'Requests'):
        VALUES = [t, end_time, self.JENKINS_ENV['JOB_NAME'], s];                                                              # set common values
        VALUES.append(v);                                                                                                     # set monitor type values
        VALUES_LIST.append(VALUES);                                                                                           # add to list of values

      if (k == 'Data'):
        for key in v:
          VALUES = [t, end_time, self.JENKINS_ENV['JOB_NAME'], s];                                                            # set common values
          for i in range(0, len(v[key])):
            VALUES.append(v[key][i]);                                                                                         # set monitor type value
          VALUES_LIST.append(VALUES);                                                                                         # add to values list

# Construct SQL statments for monitor table

      for i in range(0, len(VALUES_LIST)):
        sql_cmd = \
          "INSERT INTO " + \
          "%s%s " % (TABLE['Schema'], TABLE['Table']) + \
          "(%s) " % ', '.join(TABLE['Columns']) + \
          "VALUES " + \
          "%s;" % str(tuple(VALUES_LIST[i]));

        self.Monitor_list.append(str(sql_cmd));                                                                               # store statement to list
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

    return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Google OAuth2Credentials preparation
# 1. t_file = token file name
# 2. flow = flow from secrets object
# Returns refreshed credentials for authorization
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def prepare_credentials(self, t_file, flow):

# Retrieve existing credendials

    storage = Storage(t_file);
    credentials = storage.get();

# If no credentials exist, we create new ones

    if credentials is None or credentials.invalid:
      credentials = tools.run_flow(flow, storage, self.options);
      anom.wrt_log("Credentials refreshed");
    return credentials;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Google API service object
# 1. t_file = token file name
# 2. api = api service to create
# 2. api_ver = api version to use
# 2. client_secrets = file containing authorisation secrets (json object)
# 2. scope = scope url
# Returns service object for api calls
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def initialize_service(self, t_file, api, api_ver, client_secrets, scope):

# Create a flow object from client secrets file

    flow = flow_from_clientsecrets(client_secrets, scope=scope, message='%s is missing' % client_secrets)

# Creates an http object and authorize it using the function prepare_credentials()

    http = httplib2.Http()
    credentials = self.prepare_credentials(t_file, flow)
    http = credentials.authorize(http)

# Build the Analytics Service Object with the authorized http object

    return build(api, api_ver, http=http)

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create connection object for PostgeSQL or Redshift RDBMS
# 1. dbname = database name
# 2. host = host url
# 3. user = user name
# 4. passwd = password
# 5. port = port number as integer
# 6. conn = connection object to close if not None
# Returns connection object or closes passed connection object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def pgcon(self, dbname='', host='', user='', passwd='', port=5439, conn=None):

    if conn is not None:
      conn.close();                                                                                                           # close the connection object
      return None;

    try:
      return pg.connect(dbname=dbname, host=host, user=user, passwd=passwd, port=int(port));                                  # return the connection object
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - BOTO callback function
# 1. c = byte count
# 2. t = total file size
# Boto_size list object is set
# No values are returned by this call back function
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def boto_callback(self, count, total):

    if self.Boto_size[0] == 0:
      self.Boto_size[0] = total;                                                                                              # log the file size
      self.wrt_log("File size = %d Bytes" % total);                                                                           # store the file size
    else:
      self.wrt_log("Bytes transferred = %d Bytes" % count);                                                                   # log the current bytes transferred
      self.Boto_size[-1] = count;                                                                                             # store the current bytes transferred

    return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Perform BOTO bucket list operation
# 1. conn = boto connection object
# 2. bucket = boto bucket object
# 3. path = boto key conn
# 4. file = boto file key (wildcard as "*")
# Returns False on error
# File list returned in anom.lstval
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def boto_list(self, conn, bucket, path, file):

    if file == anom.WILDCARD: file = '';                                                                                      # set blank file if wildcard

    self.lstval = [];                                                                                                         # initialise list
    search_key = "%s/%s" % (path.strip('/'), file);                                                                           # build search key
    try:
      BUCKET = conn.get_bucket(bucket);                                                                                       # get bucket object
      for key in BUCKET.list(prefix=search_key):
        self.lstval.append(os.path.basename(key.name.encode('utf-8')));                                                       # create file list
      return True;
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Create boto connection object - defaults to s3
# 1. credentials = {key: value} pair
# 2. api = type of connection (s3, gs, ec2, etc.....)
# 3. conn = connection to close
# Returns connection object or closes passed connection object
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def boto_connection(self, credentials=None, api=S3, conn=None):

# Get keys from credentials kv pair

    if conn is not None:
      conn.close();
      self.wrt_log("boto connection closed")
      return None;

    access_key = credentials.keys()[0];
    secret_key = credentials[access_key];

    try:
      return self.BOTO_API[api](access_key, secret_key);
      self.wrt_log("boto connection created")
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return None;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Perform BOTO file operation
# 1. conn = boto connection object
# 2. bucket = boto bucket object
# 3. path = boto key conn
# 4. file = local file name and boto file key
# 5. action = boto file method to call (BOTO_GET, BOTO_SET, BOTO_EXISTS or BOTO_DELETE)
# 6. callback = number of callback results during boto file method.
# Returns False on error
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def boto_file(self, conn, bucket, path, file, action=BOTO_SET, callback=1):

# Clear file size object

    self.Boto_size = [0]*2;

# check for valid boto file action

    if action not in self.BOTO_FILE_ACTION:
      self.Errmsg = 'Invalid boto file action "{0}": {1} function:{2} module:{3}'.format(action, inspect.stack()[0][3], __name__);
      return False;

# check to see if action allowed with object

    if self.obj_type(conn) not in self.BOTO_FILE_LIST:
      self.Errmsg = 'File operations not allowed for {0}: {1} function:{2} module:{3}'.format(self.obj_type(conn), inspect.stack()[0][3], __name__);
      return False;

    try:
      boto_bucket = conn.get_bucket(bucket);                                                                                  # get a bucket object
      boto_key = Key(boto_bucket);                                                                                            # create key handle object
      boto_key.name = "%s/%s" % (path.strip('/'), file);                                                                                 # set the key value

      BOTO_FILE = \
      {
        self.BOTO_GET: [boto_key.get_contents_to_filename, True],
        self.BOTO_SET: [boto_key.set_contents_from_filename, True],
        self.BOTO_EXISTS: [boto_key.exists, False],
        self.BOTO_DELETE: [boto_key.delete, False],
      };

      self.wrt_log("%s file %s/%s via %s" % (action, boto_bucket.name, boto_key.name, self.obj_type(conn)));                                   # log action
      if BOTO_FILE[action][1]:
        boto_result = BOTO_FILE[action][0](file, cb=self.boto_callback, num_cb=callback);                                     # call with parameters
      else:
        boto_result = BOTO_FILE[action][0]();                                                                                 # call without parameters
      if action == self.BOTO_EXISTS:
        return boto_result;                                                                                                   # return result of call if boolean
      elif action == self.BOTO_DELETE:
        boto_result = BOTO_FILE[self.BOTO_EXISTS][0]();
        return not boto_result;
      else:
        return True;                                                                                                   # return file size values
    except Exception as e:
      self.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AnomSys class - Build a Gupta URL for API call
# 1. gupta_obj = gupta parameters
# 2. start_date = report start date
# 3. end_date = report end date
# 4. requester_index = requesterURL index from gupta_obj
# 5. campaign_id = campaign id from summary request
# Returns url for http request
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  def get_gupta(self, gupta_obj, start_date, end_date, requester_index, campaign_id=None):

    GUPTA = gupta_obj['Gupta'];                                                                                               # obtain gupta parameters
    kw_list = [];                                                                                                             # initialise key word list
    if campaign_id is not None: kw_list.append({'campaignId': campaign_id});
    kw_list.append({'start': start_date});
    kw_list.append({'end': end_date});
    kw_list.append({'clientId': GUPTA['ID']});
    kw_list.sort();                                                                                                           # Sort the list for signing
    query_str = '';                                                                                                           # initialise query string
    for i in kw_list:
      if len(query_str) > 0: query_str += '&';
      query_str += urllib.urlencode(i);                                                                                       # build query values
    sign_str = 'GET\n{0}\n{1}\n{2}'.format(GUPTA['Host'], GUPTA['requestURL'][requester_index], query_str);                   # build string to sign
    signature = base64.b64encode(hmac.new(GUPTA['Token'], sign_str, digestmod=hashlib.sha256).digest());                      # create hash signature
    signature = urllib.urlencode({'signature': signature});                                                                   # return URLencoded signature
    gupta_url = '{0}://{1}{2}?{3}&{4}'.format(GUPTA['Protocol'], GUPTA['Host'], GUPTA['requestURL'][requester_index], query_str, signature) # build full url string
    return gupta_url;                                                                                                         # return to caller

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of AnomSys class
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Publish AnomSys class (allows for syntax 'from AnomSys import *')

anom = AnomSys();
