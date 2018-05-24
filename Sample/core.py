"""Weather Data."""


from __future__ import absolute_import

import sys
import getpass
import requests
import configparser
from Sample.log import logger
from datetime import datetime
from urllib.parse import urlencode


class ExecutionContext:
    """Application Execution Context."""
    def __init__(self, config_file: str):
        self.settings = configparser.ConfigParser()
        self.settings._interpolation = configparser.ExtendedInterpolation()
        self.settings.read(config_file)

    @property
    def meta(self):
        """Basic meta data."""
        return {'user': getpass.getuser(), 'platform': sys.platform}


class OpenWeatherMap:
    """Fetch weather information from https://openweathermap.org restful endpoint."""

    def __init__(self, ectxt: ExecutionContext=None):
        self.ectxt = ectxt if ectxt else ExecutionContext('config.ini')
        self.template = '%s?{query}&appid=%s' % (self.ectxt.settings.get('Source', 'base_url'),
                                                 self.ectxt.settings.get('Source', 'app_key'))
        self.meta = {'C': self.ectxt.settings.get('Source', 'celsius'),
                     'F': self.ectxt.settings.get('Source', 'fahrenheit')}

    def get(self, city_name: str, country_code: str=None, unit: str=None) -> dict:
        """Display weather information for a given city_name (and country_code).
        It is possible to meet more than one weather condition for a requested location.
        The first weather condition is primary.

        :param city_name: str
        :param country_code: str
        :param unit: str, default to 'C', can either be 'F' (fahrenheit) or 'C' (celsius)
        :return: dict
        """
        if unit is None:
            unit = 'C'
        else:
            try:
                assert unit.upper() in self.meta.keys()
            except AssertionError:
                logger.info("unit can either be 'F' (fahrenheit) or 'C' (celsius).")
            finally:
                logger.info("unit reset to the default, i.e., 'C'.")
                unit = 'C'

        query = urlencode({'q': ','.join(filter(None, (city_name, country_code))), 'units': self.meta[unit.upper()]})
        url = self.template.format(query=query)
        response = requests.get(url)
        data = response.json()
        try:
            weather = '{city_name}, {current_time}, {primary_weather}, {temperature}\xb0C'.format(
                    city_name=data['name'],
                    current_time=datetime.fromtimestamp(data['dt']).strftime('%a %d %b %Y %H:%M'),
                    primary_weather=data['weather'][0]['description'],
                    temperature=data['main']['temp']
                )
        except KeyError:
            logger.error(data['message'])
        else:
            logger.info(weather)
        return data
