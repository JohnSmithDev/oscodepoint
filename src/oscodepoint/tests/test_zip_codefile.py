#!/usr/bin/env python

import os
import shutil
import tempfile
import unittest


from oscodepoint import open_codepoint

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_data")

class _BaseTestOpenCodePoint(unittest.TestCase):

    BORCHESTER_PLLS = [('BC1 0AA', -1.1852933474060652, 59.983374579108265),
                       ('BC1 0AB', -1.1852933474060652, 59.983374579108265),
                       ('BC1 0AC', -1.1852933474060652, 59.983374579108265)]
    MIDSOMER_PLLS = [('MS1 0AA', -1.664835190983481, 53.707062099865446),
                     ('MS1 0AB', -1.6648351196141495, 53.70707108780403),
                     ('MS1 0AC', -1.6648350482447793, 53.70708007574259)]
    TRUMPTON_PLLS = [('TU1 0AA', -1.094966499045151, 53.59315059547333),
                     ('TU1 0AB', -1.09495120066994, 53.59315946838718),
                     ('TU1 0AC', -1.0949359022883196, 53.59316834129901)]
    WYVERN_PLLS = [('WY1 0AA', -2.00144484120052, 52.897441072585195),
                   ('WY1 0AB', -2.0014448415540445, 52.8974500619057),
                   ('WY1 0AC', -2.0014448419075688, 52.89745905122621)]


    def test_county_list(self):
        self.assertEqual({u'T10000001': u'Borsetshire County',
                          u'T10000002': u'Midsomer County',
                          u'T10000003': u'Trumptonshire County',
                          u'T10000004': u'Wyvern County'},
                         self.codepoint.codelist["County"])

    def test_nhs_list(self):
        self.assertEqual({u'T18000001': u'West Midlands',
                          u'T18000003': u'South East',
                          u'T18000004': u'South West'},
                         self.codepoint.nhs_codelist['BBC SHA'])

    def test_total_counts(self):
        self.assertEqual(12, self.codepoint.metadata["total_count"])

    def test_area_counts(self):
        self.assertEqual({'TU': 3, 'WY': 3, 'MS': 3, 'BC': 3},
                         self.codepoint.metadata["area_counts"])

    def _postcode_lat_long(self, entry):
        return (entry["Postcode"], entry["Longitude"], entry["Latitude"])

    def test_total_entries(self):
        all_entries = sorted([z for z in self.codepoint.entries()])
        self.assertEqual(self.BORCHESTER_PLLS + self.MIDSOMER_PLLS +
                         self.TRUMPTON_PLLS + self.WYVERN_PLLS,
                         [self._postcode_lat_long(z) for z in all_entries])

    def test_area_entries(self):
        subset_entries = sorted([z for z in self.codepoint.entries(
            areas=['MS', 'WY'])])
        self.assertEqual(self.MIDSOMER_PLLS + self.WYVERN_PLLS,
                         [self._postcode_lat_long(z) for z in subset_entries])


class TestOpenCodePointDecompressedFiles(_BaseTestOpenCodePoint):

    def setUp(self):
        self.codepoint = open_codepoint(TEST_FILE_DIR)


class TestOpenCodePointZippedArchive(_BaseTestOpenCodePoint):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(suffix=__name__)
        zip_filebase = os.path.join(self.tmp_dir, 'codepo_gb')
        zip_archive = shutil.make_archive(zip_filebase, format='zip',
                                          root_dir=TEST_FILE_DIR)
        self.codepoint = open_codepoint(zip_archive)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
