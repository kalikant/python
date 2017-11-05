#!/usr/bin/env python

import re
import sys

usage="python doubleQuoteRemoval.py <srcFile> <targetFile> <srcDelimiter> <targetDelimiter>"    
if(len(sys.argv)) < 5:
	print(usage)
	sys.exit(2)
reload(sys)
sys.setdefaultencoding('utf-8')
src_file=sys.argv[1]
target_file=sys.argv[2]
srcDelimiter=sys.argv[3]
tarDelimiter=sys.argv[4]

srcRegEx=r'"\s*'+srcDelimiter+'\s*"'
print(srcRegEx)
print(tarDelimiter)


def dequote(s):
    new_str = re.sub(srcRegEx, tarDelimiter, s)
    new_str = re.sub(r'"', "", new_str)
    return new_str
	
originalfile = open(src_file, "r")
newfile = open(target_file, "w")

for line in originalfile:
  newfile.write(dequote(line));

originalfile.close()
newfile.close()

