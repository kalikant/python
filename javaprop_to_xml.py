import os
import glob
import sys
import cStringIO
import xml.sax
from pyjavaproperties import Properties
from xml.sax.saxutils import escape

BASEXMLCONFIGPREFIX="table"
I_SOURCETYPE="sourcetype"
I_KEYCOLS="keycols"
I_SCHEMA="col_schema"

def formXMLConfig(tablename, prop) :
	base="%s.%s"%(BASEXMLCONFIGPREFIX,tablename)
	vars=(I_SOURCETYPE,I_KEYCOLS,I_SCHEMA)
	l=""
	for var in vars:
		l="%s\n<property>\n<name>%s.%s</name>\n<value>%s</value>\n</property>"\
			%(l,base,var,escape(prop[var]))
	return l

def validateXMLString(xml_string) :
	try:
		parser=xml.sax.make_parser()
		stream=cStringIO.StringIO(xml_string)
		parser.parse(stream)
	except (xml.sax.SAXParseException), e:
		print " XML Error: %s" % e
		return 2
	return 1
	
if __name__=="__main__":
	usage="python javaprop_to_xml.py <config_dir> <source> <country>"
	if (len(sys.argv))<4:
		print(usage)
		sys.exit(2)
	
	reload(sys)
	sys.setdefaultencoding('utf-8')
	
	configdir=sys.argv[1]
	source=sys.argv[2]
	country=sys.argv[3]
	s="%s/%s_%s_*.config"%(configdir,source,country)
	configfiles=sorted(glob.glob(s))
	if (len(configfiles)<=0) :
		msg="%s has no files of form *.config"%(configdir)
		print(msg)
		sys.exit(2)
	xml_string="<configuration>"
	for f in configfiles:
		filename=os.path.basename(f)
		tablename=filename.split(".")[0]
		
		prop=Properties()
		prop.load(open(f))
		xhline=formXMLConfig(tablename,prop)
		if validateXMLString("<configuration>"+xhline+"</configuration>")==2 :
			print "XML Error in file: %s"%(f)
			sys.exit(2)
		xml_string+=xhline
	xml_string+="\n</configuration>\n"
	if validateXMLString(xml_string)==2 :
			sys.exit(2)
	print xml_string
	
	
