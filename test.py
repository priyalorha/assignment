import unittest
from unittest import mock

from util import read_input_xml_find_files_location_to_be_downloaded, write_data_to_s3


class MyTestCase(unittest.TestCase):

    def test_read_input_xml_find_files_location_to_be_downloaded(self):
        data = read_input_xml_find_files_location_to_be_downloaded('select.xml')
        self.assertEqual(data['DLTINS_20210119_01of02.zip'],
                         'http://firds.esma.europa.eu/firds/DLTINS_20210119_01of02.zip')
        self.assertNotEqual(type(data), type('abc'))

    @mock.patch('util.write_data_to_s3', return_value={'ResponseMetadata':
                {'HTTPStatusCode': 200}})
    def test_write_data_to_s3(self, write_data_to_s3):
        res = write_data_to_s3(csv_buffer=b',issrfmr', bucket='priya', key='abcd')
        self.assertEqual(type(res), dict)
        self.assertNotEqual(res['ResponseMetadata']['HTTPStatusCode'], 402)


if __name__ == '__main__':
    unittest.main()
