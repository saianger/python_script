import json;
import csv;
import re;
import shutil;

TRANSFORM_LIST = 0

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform function
# 1 . files = a file or list of files to process
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
raw = '\xC3\xBE'
fix = '\xFE'
raw1 = '\x00'
fix1 = ''
delim = '\xFE'

def repl(m):
  return m.group(1) + "_fixed." + m.group(3)


def clean_file(filename,raw,fix):
  fullfilenamefixed = re.sub(r'(.*)(\.)(.*)',repl,filename)
  try:
    with open(filename, 'r') as inpfil:
      with open(fullfilenamefixed, 'w') as outfil:
        for line in inpfil:
          new_line = re.sub(raw,fix,line)
          outfil.write(new_line)
  except Exception as e:
    print e;
    return False
  else:
    shutil.move(fullfilenamefixed,filename)


def repl1(m):
   return m.group(3) + "-" + m.group(1) + "-" + m.group(2) + " " + m.group(5)


def clean_transaction_time(inputFileName, cleanColIdx):
  try:
    outputFileName = re.sub(r'(.*)(\.)(.*)',repl,inputFileName)
    with open(outputFileName, 'w') as h:
      writer = csv.writer(h, delimiter=delim, lineterminator='\n')
      with open(inputFileName, 'r') as r:
        inputReader = csv.reader(r, delimiter=delim)
        for fileLine in inputReader:
          cleanOutputLine = []
          for colIdx, colVal in enumerate(fileLine):
            if colIdx in cleanColIdx:
              colVal = re.sub(r'(..)-(..)-(....)(-)(..:..:..)',repl1,colVal)
            cleanOutputLine.append(colVal)
          writer.writerow(cleanOutputLine)
  except Exception as inst:
    print inst
    exit(1)
  else:
    shutil.move(outputFileName,inputFileName)



def transform(f, i):

  if (i < TRANSFORM_LIST):
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

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Write your transform code here
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  for file in f_list:
# replace bad delimiters to throne delimiter
    if 'NetworkMatchtablesIC' in file:
      clean_file(file, raw, fix)
    else:
      clean_file(file, raw1, fix1)
      if 'NetworkActivity' in file:
        clean_transaction_time(file,[0,25])
      else:
        clean_transaction_time(file,[0])

  return True;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Transform actions 
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of transform code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
