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
        # GN UUID from ISO metadata
        uuid = old_root.xpath(
             '/gmd:MD_Metadata/gmd:fileIdentifier/gco:CharacterString',
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
            new_root.xpath('//gmd:MD_DataIdentification', namespaces=namespaces)

            # append any GeoNetwork category IDs if present
            keywords_res = new_root.xpath('//gmd:MD_Keywords', namespaces=namespaces)
            # if keywords is not present, generate the element
            if not keywords_res:
                data_id = new_root.xpath('//gmd:MD_DataIdentification', namespaces=namespaces)[0]
                desc_kw = etree.SubElement(data_id, "{http://www.isotc211.org/2005/gmd}descriptiveKeywords")
                keywords = etree.SubElement(data_id, "{http://www.isotc211.org/2005/gmd}MD_Keywords")
            else:
                keywords = keywords_res[0]

            # append any GeoNetwork categories the keywords element
            for g_cat in gn_categories:
                # find category in GeoNetwork catalog
                cat = category_info.xpath("categories/category[name/text()='{}']/label/eng".format(g_cat.text))[0].text
                kw_elem = etree.SubElement(keywords, "{http://www.isotc211.org/2005/gmd}keyword")
                kw_str = etree.SubElement(kw_elem, "{http://www.isotc211.org/2005/gco}CharacterString")
                # may not necessarily correspond to theme keywords
                # <gmd:MD_KeywordTypeCode codeListValue="theme" codeList="http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/ML_gmxCodelists.xml#MD_KeywordTypeCode"/>"
                kw_str.text = cat

        # TODO: change lineage since we are changing the ISO file, so we should
        # note modifications to history
        return etree.tostring(new_root, encoding="UTF-8",
                              pretty_print=True, xml_declaration=True)


