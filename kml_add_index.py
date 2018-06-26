"""Add "begin" timespan attribute to KML/KMZ polygons based on placemark name
Usage
    python KmlTimeSpan.py --help
    python KmlTimeSpan.py <kml_or_kmz_file>
"""
import argparse
import os
import xml.dom.minidom
import zipfile

def add_timespan(infile, suffix='_timespan'):
    """Read a KMZ file and add a timespan based on the placemark name"""
    # read input kml/kmz data
    # parse raw kml as document object model
    rawkml = read_kmz(infile)
    dom = xml.dom.minidom.parseString(rawkml)

    # add begin timespan to each placemark element
    for i, elem in enumerate(dom.getElementsByTagName('Placemark')):
        # begin = elem.getElementsByTagName("name")[0].childNodes[0].data
        elem.appendChild(create_index(dom, i))

    # write output kmz file
    write_kmz(dom, infile[:-4] + suffix + '.kml')

def create_index(dom, index):
    name = dom.createElement('name')
    name.appendChild(dom.createTextNode(str(index)))
    return name
#
# def create_span(dom, begin=None, end=None):
#     """ return a time span kml code block
#     not specifying the begin/end will omit that attribute (and the file will work fine)
#
#     <TimeSpan>
#         <begin>
#             1984
#         </begin>
#         <end>
#             1996
#         </end>
#     </TimeSpan>
#     """
#     span = dom.createElement('TimeSpan')
#     if begin:
#         b = dom.createElement("begin")
#         b.appendChild(dom.createTextNode(begin))
#         span.appendChild(b)
#
#     if end:
#         e = dom.createElement("end")
#         e.appendChild(dom.createTextNode(end))
#         span.appendChild(e)
#
#     return span


def read_kmz(infile):
    """read raw kml/kmz file"""
    if infile.upper().endswith('.KML'):
        with open(infile, "r") as f:
            return f.read()

    if infile.upper().endswith('.KMZ'):
        with zipfile.ZipFile(infile, "r") as f:
            return f.read(f.namelist()[0])


def write_kmz(dom, outkmz, arcname='doc.kml'):
    """write pretty xml to doc.kml then zip to kmz"""
    kml_temp = os.path.join(os.path.dirname(outkmz), arcname)
    with open(outkmz, "w") as f:
        f.write(dom.toprettyxml())

    # with zipfile.ZipFile(outkmz, "w") as f:
    #     f.write(kml_temp, arcname, zipfile.ZIP_STORED)
    #
    # os.remove(kml_temp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Add 'begin' timespan attribute to KML polygons based on numbers in the placemark name")
    parser.add_argument('infile', help="Input kml or kmz file with placemark names as year (YYYY).")
    parser.add_argument('suffix', help="suffix added to output file name", default='_timespan')
    add_timespan(**vars(parser.parse_args()))
