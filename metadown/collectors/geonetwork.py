import os
import csv
import tempfile
import codecs
from urlparse import urlsplit
from shutil import abspath

import requests

from metadown.utils.etree import etree

namespaces = {
"gmx":"http://www.isotc211.org/2005/gmx",
"gsr":"http://www.isotc211.org/2005/gsr",
"gss":"http://www.isotc211.org/2005/gss",
"gts":"http://www.isotc211.org/2005/gts",
"xs":"http://www.w3.org/2001/XMLSchema",
"gml":"http://www.opengis.net/gml/3.2",
"xlink":"http://www.w3.org/1999/xlink",
"xsi":"http://www.w3.org/2001/XMLSchema-instance",
"gco":"http://www.isotc211.org/2005/gco",
"gmd":"http://www.isotc211.org/2005/gmd",
"gmi":"http://www.isotc211.org/2005/gmi",
"srv":"http://www.isotc211.org/2005/srv",
}


class GeoNetworkCollector(object):
    def __init__(self, base_url):
        self.data = base_url + '/srv/en/csv.search?'
        self.download = base_url + '/srv/en/xml_iso19139?id='

    def utf_8_encoder(self, unicode_csv_data):
        for line in unicode_csv_data:
            yield line.encode('utf-8')

    def run(self):

        isos = []

        o, t =  tempfile.mkstemp()
        with codecs.open(t, "w+", "utf-8") as h:
            h.write(requests.get(self.data).text)

        with codecs.open(t, "rb", "utf-8") as f:
            reader = csv.DictReader(self.utf_8_encoder(f))
            for row in reader:
                if row.get('schema') != 'iso19139':
                    continue

                download_url = self.download + row.get('id')
                isos.append(download_url)

        os.unlink(f.name)

        return isos

    @staticmethod
    def namer(url, **kwargs):
        uid = urlsplit(url).query
        uid = uid[uid.index("=")+1:]
        return "GeoNetwork-" + uid + ".xml"

    @staticmethod
    def uuid_namer(url, **kwargs):
        root = etree.parse(url).getroot()
        x_res = root.xpath(
            '/gmd:MD_Metadata/gmd:fileIdentifier/gco:CharacterString',
            namespaces=namespaces)
        uuid = "GeoNetwork-" + x_res[0].text + ".xml"
        return uuid



    # TODO: change to not use static method
    @staticmethod
    def modifier(url, **kwargs):
        # BWA: this seems like a pretty janky way to translate XML.
        # shouldn't we use an XSLT stylesheet proper?
        # translate ISO19139 to ISO19115
        # base url from GeoNetwork ISO data
        base_url = '/'.join(url.split('/')[:-1])
        gmi_ns = "http://www.isotc211.org/2005/gmi"
        etree.register_namespace("gmi",gmi_ns)
        new_root = etree.Element("{%s}MI_Metadata" % gmi_ns)
        old_root = etree.parse(url).getroot()
        # carry over an attributes we need
        for k, v in old_root.attrib.iteritems():
            new_root.set(k,v)
        for e in old_root:
            new_root.append(e)
        # TODO: use var interpolation to keep XML schema locations DRY
        # GN UUID from ISO metadata
        uuid = new_root.xpath(
            'gmd:fileIdentifier/gco:CharacterString',
             namespaces=namespaces)[0].text
        # TODO: Don't load this every time, consider saving as a instance
        # variable instead of parsing the tree every time
        gn_metadata_url = base_url + '/xml.search'
        gn_metadata = etree.parse(gn_metadata_url)
        # category information, mostly used for human readable names
        category_info_url = base_url + '/xml.info?type=categories'
        category_info = etree.parse(category_info_url)
        gn_categories = gn_metadata.xpath("//uuid[text()='{}']/../category[not(@internal)]".format(uuid))
        if gn_categories:
            data_id_elem = new_root.xpath('gmd:identificationInfo/gmd:MD_DataIdentification',
                                          namespaces=namespaces)
            # most files will have MD_Metadata, but if they don't, create one
            if data_id_elem:
                data_id = data_id_elem[0]
            # this will be missing some mandatory elements such as citation,
            # but should be fine for finding elements
            else:
                ident_info = new_root.xpath('gmd:identificationInfo',
                                            namespaces=namespaces)[0]
                data_id = etree.SubElement(ident_info,
                                            "{http://www.isotc211.org/2005/gmd}MD_DataIdentification")


            # append any GeoNetwork category IDs if present
            supp_info_elem = data_id.xpath('gmd:supplementalInformation',
                                           namespaces=namespaces)
            # if supplemental info is not present, generate the element
            if not supp_info_elem:
                supp_info = etree.SubElement(data_id, "{http://www.isotc211.org/2005/gmd}supplementalInformation")
            else:
                supp_info = supp_info_elem[0]

            # make new line and text string for supplemental info
            gn_cat_str = 'GeoNetwork Categories: "'
            # find category in GeoNetwork catalog
            xpath_expr = "categories/category[name/text()='{}']/label/eng"
            cats = [category_info.xpath(xpath_expr.format(g_cat.text))[0].text
                                        for g_cat in gn_categories]
            cat_texts = ', '.join(cats) + '"'
            gn_cat_str += cat_texts
            supp_info_str_elem = supp_info.xpath('gco:CharacterString',
                                            namespaces=namespaces)
            # if we created a new element we need to add a CharacterString
            # element
            if not supp_info_str_elem:
                supp_info_str = etree.SubElement(supp_info,
                            "{http://www.isotc211.org/2005/gco}CharacterString")
            else:
                supp_info_str = supp_info_str_elem[0]

            if supp_info_str.text is None:
                supp_info_str.text = gn_cat_str
            else:
                supp_info_str.text += "\n{}".format(gn_cat_str)

        # TODO: change lineage since we are changing the ISO file, so we should
        # note modifications to history
        return etree.tostring(new_root, encoding="UTF-8",
                              pretty_print=True, xml_declaration=True)


