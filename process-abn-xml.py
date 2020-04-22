import xml.sax
import argparse
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import pydocumentdb.errors as errors
import datetime
import pprint
import uuid
import time
import csv
import sys

class ABRContentHandler(xml.sax.ContentHandler):
  def __init__(self, client, collection, args, writer): 
    xml.sax.ContentHandler.__init__(self)
    self.__client = client
    self.__collection = collection
    self.__debug = args.debug 
    self.__throughput = args.throughput 
    self.__frequency = args.frequency 
    self.__writer = writer 

    self.__entry = {}
    self.__stack = [] 

    self.__counter = 0
    self.__data_size = 0

    self.__currentName = "";
    self.__time = time.time()

  def set_collection(self, collection):
    self.__collection = collection

  def startElement(self, name, attrs):
    self.__currentName = name
  
    if (name == "ABR"):

       self.__entry = {}
       self.__stack = [self.__entry]

       for attrName in attrs.keys():
           self.__stack[-1][str(attrName)] = str(attrs.get(attrName))

    elif (name == "DGR" or name == "NonIndividualName" or name == "OtherEntity"):
       if str(name) not in self.__stack[-1]:
          group = {} 
          value = self.__stack[-1]
          value[str(name)] = [group] 
          self.__stack.append(group)
       else:
          group = {}
          self.__stack[-1][str(name)].append(group)
          self.__stack.append(group)

          for attrName in attrs.keys():
              self.__stack[-1][str(attrName)] = str(attrs.get(attrName))

    elif (name == "GivenName"):
        if str(name) not in self.__entry:
           self.__entry[str(self.__currentName)] = []

  def endElement(self, name):
    if (name == "DGR" or name == "NonIndividualName" or name == "OtherEntity"):
       self.__stack.pop(-1)

    elif (name == "ABR"):
        if 'ABN' in self.__entry.keys():
           self.__entry['id'] =  str(self.__entry['ABN']) 
           self.__entry['part'] = self.__entry['recordLastUpdatedDate'] 
           self.__data_size += sys.getsizeof(self.__entry)

           if ((self.__counter % self.__throughput) == 0 and self.__counter != 0):
              current_time = time.time()
              print("%s : %d - %d - [%ld] - %s"% (datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), 
                    current_time - self.__time, self.__counter, self.__data_size, self.__entry['ABN'])) 
              writer.writerow({
                    'date-time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                    'throughput': self.__throughput,
                    'frequency': '1000',
                    'record-counter': self.__counter, 
                    'time-taken': current_time - self.__time,
                    'data-size': self.__data_size})
              self.__time = current_time
              self.__data_size = 0 

           self.__counter += 1

           if self.__debug == True:
              pp = pprint.PrettyPrinter(indent=2)
              pp.pprint(self.__entry)

           try: 
             self.__client.CreateDocument(self.__collection['_self'], self.__entry)
           except errors.DocumentDBError as e:
             print('\nError: {0}'.format(e.message))
        else:
           print("ERROR: ABN not present")
           pp = pprint.PrettyPrinter(indent=2)
           pp.pprint(self.__entry)


        self.__stack = []

  def characters(self, content):
    
    if (self.__currentName == "GivenName"):
       self.__entry[str(self.__currentName)].append(str(content))

    elif (self.__currentName == "ABN"):
        self.__stack[-1][str(self.__currentName)] = str(content)

    elif len(self.__stack) > 0:
        self.__stack[-1][str(self.__currentName)] = str(content)

argParser = argparse.ArgumentParser(description='Process ABN XML File.')

argParser.add_argument('input', metavar='i', nargs='+',
                    help='the XML Files')
argParser.add_argument('--create', help='Create the CosmosDB artifacts',
    action='store_true')
argParser.add_argument('--debug', help='Debug flag - set to false',
    action='store_false', default=False)
argParser.add_argument('--trace', help='Trace File')
argParser.add_argument('--key', help='CosmosDB Master Key')
argParser.add_argument('--host', help='CosmosDB Host Name')
argParser.add_argument('--location', help='CosmosDB Location')
argParser.add_argument('--dbname', help='The CosmosDB Database Name',
    default='ato')
argParser.add_argument('--colname', help='The CosmosDB Collection Name',
    default='abr')
argParser.add_argument('--throughput', help='The CosmosDB throughput - default 400',
    type=int, 
    default=400)
argParser.add_argument('--frequency', help='The the Observation Frequency- default 1000',
    type=int, 
    default=1000)


args = argParser.parse_args()

print("Input File: '%s'"% args.input[0]) 
print("Host Name: '%s'"% args.host)
print("Preferred Location: '%s'"% args.location)
print("Database Name: '%s'"% args.dbname)
print("Collection Name: '%s'"% args.colname)
print("Throughput: %d"% args.throughput)
print("Trace File: %s"% args.trace)
print("Debug: %s"% args.debug)

# Configuring the connection policy (allowing for endpoint discovery)
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery
connectionPolicy.PreferredLocations = [args.location]

# Set keys to connect to Azure Cosmos DB

client = document_client.DocumentClient(args.host, {'masterKey': args.key}, connectionPolicy)

db = None

if (args.create):
   db = client.CreateDatabase({ 'id': args.dbname})
else:
   db = client.ReadDatabase('dbs/' + args.dbname)

collection = None 

if (args.create):
# Create collection options
  options = {
      'offerEnableRUPerMinuteThroughput': True,
      'offerVersion': "V2",
      'offerThroughput': args.throughput
  }
  collection = client.CreateCollection(db['_self'], { 'id':args.colname,
			"partitionKey": {
                            "paths": [
                               "/part"
                             ],
                             "kind": "Hash"
                }}, options)

else:
  collection = client.ReadCollection('dbs/' + args.dbname + '/colls/' + args.colname)
  
# Create a collection

saxParser = xml.sax.make_parser()

with open(args.trace, 'w', newline='') as csv_file:
    field_names = ['date-time', 'throughput', 'frequency', 'record-counter', 'time-taken', 'data-size']
    writer = csv.DictWriter(csv_file, fieldnames=field_names)  
    writer.writeheader()
    saxParser.setContentHandler(ABRContentHandler(client, collection, args, writer))
    saxParser.parse(open(args.input[0],"r"))

