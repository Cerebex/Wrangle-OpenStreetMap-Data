"""
Listed below is the directions as stated by Udacity:


After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import re
import xml.etree.cElementTree as ET
import cerberus

import schema

OSM_PATH = "sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "ave": "Avenue",
            "Rd.": "Road",
            "rd": "Road",
            "Rd\\": "Road",
            "Rd": "Road",
            "RD": "Road",
            "NE": "Northeast",
            "N.W.": "Northwast",
            "n.w.": "Northwast",
            "NW": "Northwast",
            "Cir": "Circle",
            "Dr": "Drive",
            "Dr.": "Drive",
            "Ct": "Court",
            "E": "East",
            "W": "West",
            "N": "West",
            "Ln.": "Lane",
            "Blvd": "Boulevard",
            "Pl": "Place",
            "Ter": "Terrace"
            }

def is_street_name(elem):
    '''Finds if attribute is a street name'''
    return (elem.attrib['k'] == "addr:street")

def update_name(name, mapping):
    '''corrects street type in street name'''
    for map in mapping:

        if mapping[map] in name:
            return name

        elif map in name:
            name = name.replace(map, mapping[map])

    return name

def fix_pharmacy(value):
  # remove non-letters and pharmacy from names of pharmacies
  value = value.replace('/', '')
  value = value.replace('-', '')
  value = value.replace('Pharmacy', '')
  value = value.replace('pharmacy', '')
  value = value.replace(' ', '')
  
  return value

def fix_county(value):
  # if only one county listed, remove state abbreviation
  if value.find(':') == -1 and value.find(';') == -1 :
    value = value.split(',', 1)[0]
  
  return value

def fix_phone(value):
  # modify all phone numbers so they are simply 10 digits
  
  # +1 866-RIDMTA should be 1-866-RIDE-MTA
  # https://mta.maryland.gov/ride-mta-to-african-american-festival
  if value == '+1 866-RIDMTA':
    value = '8667433682'
  
  # +13192881 should be 2023192881
  # https://www.yelp.com/biz/jolie-jewelry-washington
  if value == '+13192881':
    value =  '2023192881'
  
  # 649 3555 should be 3016493555
  # https://standrewapostle.org/School-WP/contact/
  if value == '649 3555':
    value =  '3016493555'
  
  value = value.replace('+1 ', '')
  value = value.replace('-', '')
  value = value.replace(' ', '')
  value = value.replace('.', '')
  value = value.replace('(', '')
  value = value.replace(')', '')
  value = value.replace('+1', '')
  value = value.replace('New Customer: ', '')
  value = value.replace('Susanna Farm Nursery: ', '')
  value = value.replace('tel:', '')
  
  if len(value)>10:
    value = value[0:10]
  
  return value

def fix_postcode(value):
  # modify all postcode numbers so they are simply 5 digits
  
  # 2011 should be 20011
  # https://www.yelp.com/biz/epiphany-open-pit-beef-and-subs-washington
  if value == '2011':
    value = '20011'
  
  # 2005 should be 20005
  # https://www.google.com/maps/place/7-Eleven/@38.9084384,-77.0322289,21z/data=!4m18!1m12!4m11!1m3!2m2!1d-77.0321551!2d38.9085401!1m6!1m2!1s0x89b7b7ea61f0d259:0x8fc5b8b0c95d49af!2s7-Eleven+Washington,+DC!2m2!1d-77.0321722!2d38.9085452!3m4!1s0x89b7b7ea61f0d259:0x8fc5b8b0c95d49af!8m2!3d38.9085452!4d-77.0321722
  if value == '2005':
    value = '20005'
  
  if len(value)!=5:
    value = value[0:5]
  
  return value

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
  """Clean and shape node or way XML element to Python dict"""

  node_attribs = {}
  way_attribs = {}
  way_nodes = []
  tags = []  # Handle secondary tags the same way for both node and way elements
  
  if element.tag == 'node':
    # create a dictionary of all attributes of a specific node element
    for node_field in node_attr_fields:
      node_attribs[node_field] = element.attrib[node_field]
    
    # iterate through all "tag" attributes set underneath the specific node selected
    for tag in element.iter("tag"):
      tag_dict = {}
      n = {}
      
      # creates dictionary of all "tag" attributes keys and values set underneath the specific node selected
      for tag in element.iter("tag"):
        n[tag.attrib['k']] = tag.attrib['v']
      
      # sends all pharmacy names values through the fix_pharmacy function to clean their values then saves the cleaned
      # value in the tag_dict dictionary
      if tag.attrib['k'] == "name" and 'amenity' in n.keys() and n['amenity'] == 'pharmacy' and 'name' in n.keys(): 
        tag_dict['value'] = fix_pharmacy(tag.attrib['v'])
      
      # sends all pharmacy names values through the fix_pharmacy function to clean their values then saves the cleaned
      # value in the tag_dict dictionary
      elif tag.attrib['k'] == 'phone':
        tag_dict['value'] = fix_phone(tag.attrib['v'])
      
      # sends all phone number values through the fix_phone function to clean their values then saves the cleaned
      # value in the tag_dict dictionary
      elif tag.attrib['k'] == 'contact:phone':
        tag_dict['value'] = fix_phone(tag.attrib['v'])
      
      # sends all pharmacy phone number values through the fix_phone function to clean their values then saves the 
      # cleaned value in the tag_dict dictionary
      elif tag.attrib['k'] == 'phone:pharmacy':
        tag_dict['value'] = fix_phone(tag.attrib['v'])
      
      # sends all postal code values through the fix_postcode function to clean their values then saves the 
      # cleaned value in the tag_dict dictionary
      elif tag.attrib['k'] == 'addr:postcode':
        tag_dict['value'] = fix_postcode(tag.attrib['v'])
      
      # Saves key value into the tag_dict dictionary if the value does not need to be cleaned
      else:
        tag_dict['value'] = tag.attrib['v']
      
      tag_dict['id'] = node_attribs['id']
      

      catch = problem_chars.search(tag.attrib['k'])
      # if we find problem characters in the key then skip this element
      if catch:
        continue
      
      # if there are no problem characters or ':' in the key value then process element
      elif ':' not in tag.attrib['k']:
        tag_dict['key'] = tag.attrib['k']
        tag_dict['type'] = 'regular'
      
      # if the tag "k" value contains a ":" the characters before the ":" is set as the tag type
      # and characters after the ":" isset as the tag key
      # if there are additional ":" in the "k" value they are ignored and kept as part of
      # the tag key
      elif ':' in tag.attrib['k']:
        tag_dict['key'] = tag.attrib['k'].split(':', 1)[1]
        tag_dict['type'] = tag.attrib['k'].split(':', 1)[0]
      
      tags.append(tag_dict)
  
  elif element.tag == 'way':
    index = 0
    # create a dictionary of all attributes of a specific way element
    for way_field in way_attr_fields:
      way_attribs[way_field] = element.attrib[way_field]
    

    for tag in element.iter("tag"):
      tag_dict = {}

      # sends all county values through the fix_county function to clean their values then save the 
      # cleaned value in the tag_dict dictionary
      if tag.attrib['k'] == 'tiger:county':
        tag_dict['value'] = fix_county(tag.attrib['v'])
      
      # sends all street name values through the update_name function to clean their values then save the 
      # cleaned value in the tag_dict dictionary
      elif is_street_name(tag):
        tag_dict['value'] = update_name(tag.attrib['v'], mapping)
      
      # Saves key value into the tag_dict dictionary if the value does not need to be cleaned
      else:
        tag_dict['value'] = tag.attrib['v']
      
      tag_dict['id'] = way_attribs['id']
      
      catch = problem_chars.search(tag.attrib['k'])
      # if we find problem characters in the key then skip this element
      if catch:
        continue
      
      # if there are no problem characters or ':' in the key value then process element
      elif ':' not in tag.attrib['k']:
        tag_dict['key']= tag.attrib['k']
        tag_dict['type']= 'regular'
      
      # if the tag "k" value contains a ":" the characters before the ":" is set as the tag type
      # and characters after the ":" isset as the tag key
      # if there are additional ":" in the "k" value they are ignored and kept as part of
      # the tag key
      elif ':' in tag.attrib['k']:
        tag_dict['key'] = tag.attrib['k'].split(':', 1)[1]
        tag_dict['type'] = tag.attrib['k'].split(':', 1)[0]
      
      tags.append(tag_dict)
    
    # Creates "way_nodes" list which holds a list of dictionaries, one for each nd child tag.  
    # Each dictionary has the fields:
    # id: the top level element (way) id
    # node_id: the ref attribute value of the nd tag
    # position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within the way element
    for tag in element.iter("nd"):
      tag_dict = {}
      tag_dict['id'] = way_attribs['id']
      tag_dict['node_id'] = tag.attrib['ref']
      tag_dict['position'] = index
      index += 1
      way_nodes.append(tag_dict)

  if element.tag == 'node':
    return {'node': node_attribs, 'node_tags': tags}
  
  elif element.tag == 'way':
    return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
  """Yield element if it is the right type of tag"""

  context = ET.iterparse(osm_file, events=('start', 'end'))
  _, root = next(context)
  
  for event, elem in context:
  
    if event == 'end' and elem.tag in tags:
      yield elem
      root.clear()


def validate_element(element, validator, schema=SCHEMA):
  """Raise ValidationError if element does not match schema"""
  if validator.validate(element, schema) is not True:
    field, errors = next(validator.errors.iteritems())
    message_string = "\nElement of type '{0}' has the following errors:\n{1}"
    error_strings = (
      "{0}: {1}".format(k, v if isinstance(v, str) else ", ".join(v))
      for k, v in errors.iteritems()
    )
    
    raise cerberus.ValidationError(
      message_string.format(field, "\n".join(error_strings))
    )


class UnicodeDictWriter(csv.DictWriter, object):
  """Extend csv.DictWriter to handle Unicode input"""

  def writerow(self, row):
    super(UnicodeDictWriter, self).writerow({
      k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
      })

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
  """Iteratively process each XML element and write to csv(s)"""

  with codecs.open(NODES_PATH, 'w') as nodes_file, \
       codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
       codecs.open(WAYS_PATH, 'w') as ways_file, \
       codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
       codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

      nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
      node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
      ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
      way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
      way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

      nodes_writer.writeheader()
      node_tags_writer.writeheader()
      ways_writer.writeheader()
      way_nodes_writer.writeheader()
      way_tags_writer.writeheader()

      validator = cerberus.Validator()

      for element in get_element(file_in, tags=('node', 'way')):
        el = shape_element(element)
        
        if el:
          
          if validate is True:
            validate_element(el, validator)

          if element.tag == 'node':
            nodes_writer.writerow(el['node'])
            node_tags_writer.writerows(el['node_tags'])
          
          elif element.tag == 'way':
            ways_writer.writerow(el['way'])
            way_nodes_writer.writerows(el['way_nodes'])
            way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
  # Note: Validation is ~ 10X slower. For the project consider using a small
  # sample of the map when validating.
  process_map(OSM_PATH, validate=True)
