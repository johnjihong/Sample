"""Sample Application Unit Tests."""

from __future__ import absolute_import

import unittest
from Sample.core import OpenWeatherMap


class TestOpenWeatherMap(unittest.TestCase):
    def setUp(self):
        self.client = OpenWeatherMap()

    def test_invalid_city_name(self):
        expected = {'cod': '404', 'message': 'city not found'}
        unknown = self.client.get('foobar')
        assert unknown == expected

    def test_valid_city_name(self):
        response = self.client.get('london')
        assert isinstance(response, dict)
        assert response['name'] == 'London'

    def test_invalid_country_code(self):
        response = self.client.get('london', country_code='foobar')
        assert isinstance(response, dict)
        assert response['name'] == 'London'

    def test_valid_country_code(self):
        response = self.client.get('london', country_code='GB')
        assert isinstance(response, dict)
        assert response['name'] == 'London'

    def test_invalid_unit(self):
        response = self.client.get('london', country_code='GB', unit='q')
        assert isinstance(response, dict)
        assert response['name'] == 'London'

    def test_valid_unit(self):
        response = self.client.get('london', country_code='GB', unit='C')
        assert isinstance(response, dict)
        assert response['name'] == 'London'


if __name__ == '__main__':
    unittest.main()
