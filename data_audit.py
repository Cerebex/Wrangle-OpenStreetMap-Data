import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint


OSM_PATH = "sample.osm"

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)


expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]

def audit_streets(filename):
    '''outputs specific street values to be reviewed for potential future correction'''
    bad_streets = defaultdict(set)

    for event, elem in ET.iterparse(filename):
        if elem.tag == "node" or elem.tag == "way":

            for attr in elem.iter("tag"):
                k = attr.attrib["k"]

                '''Finds if attribute is a street name'''
                if k == "addr:street":
                    '''finds street names which have unexpected street types and then creates a dictionary with the unknown street type 
                        and then the entire street name. '''
                    street_name = attr.attrib["v"]
                    match = street_type_re.search(street_name)

                    if match:
                        street_type = match.group()

                        if street_type not in expected:
                            bad_streets[street_type].add(street_name)
    pprint.pprint(bad_streets)

def audit_pharmacy(osmfile):
    '''outputs specific values to be reviewed for potential future correction'''

    pharmacy_values = []

    for _, elem in ET.iterparse(osmfile):

        if elem.tag == "node": 
            n = {}

            for tag in elem.iter("tag"):
                n[tag.attrib['k']] = tag.attrib['v']

            if 'amenity' in n.keys() and n['amenity'] == 'pharmacy' and 'name' in n.keys(): 
                pharmacy_values.append(n['name'])

    pprint.pprint(pharmacy_values)

def audit_county(osmfile):
    '''outputs specific county values to be reviewed for potential future correction'''

    county_values = []

    for _, elem in ET.iterparse(osmfile):

        if elem.tag == "way": 
            w = {}

            for tag in elem.iter("tag"):
                if tag.attrib['k'] == 'tiger:county':
                    county_values.append(tag.attrib['v'])

    pprint.pprint(county_values)

def audit_phone(osmfile):
    '''outputs specific phone values to be reviewed for potential future correction'''

    phone_values = []

    for _, elem in ET.iterparse(osmfile):

        if elem.tag == "node": 
            n = {}

            for tag in elem.iter("tag"):
                n[tag.attrib['k']] = tag.attrib['v']

            if 'phone' in n.keys():
                phone_values.append(n['phone'])

            if 'contact:phone' in n.keys():
                phone_values.append(n['contact:phone'])

            if 'phone:pharmacy' in n.keys():
                phone_values.append(n['phone:pharmacy'])

    pprint.pprint(phone_values)

def audit_postcode(osmfile):
    '''outputs specific postal code values to be reviewed for potential future correction'''

    postcode_values = []

    for _, elem in ET.iterparse(osmfile):

        if elem.tag == "node": 
            n = {}
            for tag in elem.iter("tag"):
                if tag.attrib['k'] == 'addr:postcode':
                    postcode_values.append(tag.attrib['v'])

    pprint.pprint(postcode_values)
