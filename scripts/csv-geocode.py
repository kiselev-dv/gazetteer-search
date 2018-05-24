import csv, sys, re
import urllib
import urllib2
import json

reload(sys)  
sys.setdefaultencoding('utf8')

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

for line in unicode_csv_reader(sys.stdin, delimiter='\t'):
    addr_text = line[1]
    text = re.sub('^[0-9 \\"\,]*', '', addr_text)
    data = urllib.urlencode({'q': text, 'prefix': False})
    full_url = 'http://localhost:8080/location/_search.json?' + data
    response = urllib2.urlopen(full_url)
    resp = json.loads(response.read())
    
    newline = list(line)
    if len(resp[u'rows']) > 0 :
        first_hit = resp[u'rows'][0]
        
        newline.append(first_hit[u'full_text'])
        newline.append(first_hit[u'osm_id'])
        newline.append(str(int(resp[u'total_hits']))) 

        print '\t'.join(newline)
    else:
        newline.append(u'osm_id')
        newline.append(u'full_text')
        newline.append(u'total_hits')
        print '\t'.join(newline)
