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
    
    
   """Datastream Restful API."""


import numpy as np
import pandas as pd
import dill as pickle
from enum import Enum
from io import BytesIO
from lxml import etree
from time import sleep
from typing import Union
from functools import partial
from dateutil.parser import parse
from urllib.parse import urlencode
from datetime import datetime, timedelta
import os, re, json, requests, asyncio, aiohttp, argparse, logging, math, aiofiles

__all__ = ['Datastream', 'DateKind', 'Date', 'DateFrequency', 'InstrumentProperty', 'DataType']

def create_event_loop():
    """Create async event loop."""
    try:
        import uvloop
    except ModuleNotFoundError:
        pass
    else:
        asyncio.set_event_loop_policy(uvloop.EvetLoopPolicy())
    finally:
        if asyncio.get_event_loop().is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())

        return asyncio.get_event_loop()


def set_proxy(proxy_username: str=None, proxy_password: str=None) -> dict:
    """Set http proxy.
    :param proxy_password: str, proxy password.
    :param proxy_username: str, proxy username.
    :return: dict

    Note: Computer name starts with 'E' is non server PC, vice versa.
    """
    if os.environ['COMPUTERNAME'].startswith('E'):
        if proxy_password is None:
            raise ValueError('Missing proxy_password.')
        else:
            proxy = '{}:{}@uk-proxy-01.systems.uk.hsbc:80'.format(proxy_username if proxy_username else os.getlogin(),
                                                                  proxy_password)
    else:
        proxy = 'uk-server-proxy-01.systems.uk.hsbc:80'

    return {
        'http': 'http://{}'.format(proxy),
        'https': 'https://{}'.format(proxy)
    }


class DateKind(Enum):
    TimeSeries = 'TimeSeries'
    Snapshot = 'Snapshot'


class Date(Enum):
    """The DSDateNames type specifies the date literals.
    Name	    Description	        Example Value
    BDATE	    Base date.
    SWDATE	    Start of Week.
    SMDATE	    Start of Month.
    SQDATE	    Start of Quarter.
    SYDATE	    Start of Year.
    LATESTDATE	Latest date.
    TODAY	    Intraday value.
    YRE	        End of Year.	    Ex: YRE, -2YRE, +1YRE
    MTE	        End of Month.	    Ex: MTE, -2MTE, +1MTE
    QTE	        End of Quarter.	    Ex: QTE, -2QTE, +1QTE
    WKE	        End of Week.	    Ex: WKE, -2WKE, +1WKE
    YRS	        Start of Year.	    Ex: YRS, -2YRS, +1YRS
    MTS	        Start of Month.	    Ex: MTS, -2MTS, +1MTS
    QTS	        Start of QuarTSr.	Ex: QTS, -2QTS, +1QTS
    WKS	        Start of Week.	    Ex: WKS, -2WKS, +1WKS
    YRM	        Middle of Year.	    Ex: YRM, -2YRM, +1YRM
    MTM	        Middle of Month.	Ex: MTM, -2MTM, +1MTM
    QTM	        Middle of Quarter.	Ex: QTM, -2QTM, +1QTM
    WKM	        Middle of Week.	    Ex: WKM, -2WKM, +1WKM
    """
    BDATE = 'BDATE'
    SWDATE = 'SWDATE'
    SMDATE = 'SMDATE'
    SQDATE = 'SQDATE'
    SYDATE = 'SYDATE'
    LATESTDATE = 'LATESTDATE'
    TODAY = 'TODAY'
    YRE = 'YRE'
    MTE = 'MTE'
    QTE = 'QTE'
    WKE = 'WKE'
    YRS = 'YRS'
    MTS = 'MTS'
    QTS = 'QTS'
    WKS = 'WKS'
    YRM = 'YRM'
    MTM = 'MTM'
    QTM = 'QTM'
    WKM = 'WKM'


class DateFrequency(Enum):
    """The DSDateFrequencyNames type specifies the frequency values.
    Name	Description
    D	    Daily.
    W	    Weekly.
    M	    Monthly.
    Q	    Quarterly.
    Y	    Yearly.
    7D	    7 days.
    7DPAD	7 days padded.
    7DME	7 days Middle Eastern.
    7DMEPAD	7 days Middle Eastern padded.
    """
    D = 'D'
    W = 'W'
    M = 'M'
    Q = 'Q'
    Y = 'Y'
    D7 = '7D'
    DPAD7 = '7DPAD'
    DME7 = '7DME'
    DMEPAD7 = '7DMEPAD'


class InstrumentProperty(Enum):
    """The DSInstrumentPropertyNames type specifies the instrument properties.
    Name	      Description	                      Example Value
    IsList	      Is the instrument a list.
    IsExpression  Is the instrument an expression.	  Expressions such as VOD(PL)+BARCH(PH)
    IsSymbolSet	  Is the instrument a set of symbols.  Set of symbols such as VOD,IBM,BARC
    ReturnName	  Return the instrument name.
    """
    IsList = 'IsList'
    IsExpression = 'IsExpression'
    IsSymbolSet = 'IsSymbolSet'
    ReturnName = 'ReturnName'


class DataType(Enum):
    """The DSSymbolResponseValueType type specifies the value type of a symbol response.
    Name	    Numeric Value	Description	Example Value
    Error	        0	        The value is an error.
    Empty	        1	        The value is empty.
    Bool	        2	        The value is a boolean value.
    Int	            3	        The value is an integer.
    DateTime	    4	        The value is a date time.
    Double	        5	        The value is a double.
    String	        6	        The value is a string.
    BoolArray	    7	        The value is an array of booleans.
    IntArray	    8	        The value is an array of integers.
    DateTimeArray	9	        The value is an array of date times.
    DoubleArray	    10	        The value is an array of doubles.
    StringArray	    11	        The value is an array of strings.
    ObjectArray	    12	        The value is an array of objects with the array item being a
                                heterogenous mixture of bool, datetime, double or string types.
    """
    Error = 0
    Empty = 1
    Bool = 2
    Int = 3
    DateTime = 4
    Double = 5
    String = 6
    BoolArray = 7
    IntArray = 8
    DateTimeArray = 9
    DoubleArray = 10
    StringArray = 11
    ObjectArray = 12


class Usage(Enum):
    User = 'User ID for which usage stats is provided'
    Hits = 'Number of Service Hits'
    Requests = 'Number of Requests'
    Datatypes = 'Number of returned Datatypes'
    Datapoints = 'Number of returned Datapoints'
    StartDate = "Start date considered for user's usage stats"
    EndDate = "End date considered for user's usage stats"


class Datastream:
    """Datastream Restful API."""

    __slots__ = ['username', 'password', 'proxies', 'proxy_username', 'proxy_password', 'endpoint', 'cache', 'park_time']

    def __init__(self, username: str, password: str, *, proxies: dict=None,
                 proxy_username: str=None, proxy_password: str=None, endpoint: str=None, park_time: int=None):
        self.username = username
        self.password = password
        self.proxies = proxies if proxies else set_proxy(proxy_username=proxy_username, proxy_password=proxy_password)
        self.cache = {'token': (None, datetime.today() - timedelta(days=1)), 'failed': [], 'result': []}
        self.endpoint = endpoint if endpoint else 'http://product.datastream.com/dswsclient/V1/DSService.svc/rest/'
        self.park_time = park_time if park_time else 300

    @property
    def usage(self):
        """Account usage review."""
        url = self.endpoint + 'Data?'
        with requests.Session() as session:
            params = {'instrument': 'STATS', 'token': self.token, 'datatypes': 'DS.USERSTATS'}
            response = session.get(url, params=params, proxies=self.proxies)

            if response.status_code == 200:
                data = json.loads(response.text)
                meta = {'User': Usage.User.value, 'Hits': Usage.Hits.value, 'Requests': Usage.Requests.value,
                        'Datatypes': Usage.Datatypes.value, 'Datapoints': Usage.Datapoints.value,
                        'Start Date': Usage.StartDate.value, 'End Date': Usage.EndDate.value}
                df = pd.concat([pd.DataFrame(data=x['SymbolValues'], index=[0]).assign(DataType=x['DataType'],
                                                                                       Definition=meta[x['DataType']])
                                    for x in data['DataTypeValues']], ignore_index=True)
                mask = (df.Type == DataType.DateTime.value)
                df.loc[mask, 'Value'] = df.loc[mask, 'Value'].apply(
                    lambda s: self.epoch_to_datetime(s, fmt='%Y-%b-%d %H:%M:%S %p')
                )
                return df[['DataType', 'Definition', 'Value']]
            else:
                raise requests.HTTPError(response.status_code)

    @property
    def token(self):
        token_value, token_expiry = self.cache['token']

        if token_value is None or datetime.now() > token_expiry:
            self.cache['token'] = self.create_token()

        return self.cache['token'][0]

    def create_params(self, instruments: list, *, datatypes: list=None,
                      datekind: str=None, start: str=None, end: str=None,
                      freq: str=None, token: str=None, props: str=None) -> dict:
        """Create request payload.
        :param instruments: list, the instruments.
        :param datatypes: list, the datatypes.
        :param datekind: str, defines whether the date is a snapshot (static) or a time series date,
                              it is enough to specify only start date for a snapshot date.
        :param start: str, the start date, the dates can be relative dates or absolute dates or string literals.
        :param end: str, the end date, the dates can be relative dates or absolute dates or string literals.
        :param freq: str, the frequency.
        :param token: str, the token.
        :param props: str, optional properties to customize behavior for the instruments.
        :return dict
        """
        params = {
            'token': token if token else self.token,
            'instrument': ','.join(instruments),
            'datekind': datekind if datekind else DateKind.TimeSeries.value,
            'datatypes': ','.join(set(datatypes)) if datatypes else '',
            'start': parse(start).strftime('%Y-%m-%d') if start else '',
            'end': parse(end).strftime('%Y-%m-%d') if end else '',
            'freq': freq if freq else '',
            'props': props if props else InstrumentProperty.IsSymbolSet.value,
            'format': 'json'
        }
        return params

    def create_token(self):
        """Create Datastream token."""
        logging.info('Creating token.')
        url = self.endpoint + 'Token?username={}&password={}&format=json'.format(self.username, self.password)
        with requests.Session() as session:
            response = session.get(url, proxies=self.proxies)

            if response.status_code == 200:
                data = json.loads(response.text)
                expiry = int(re.findall(r'\d+', data['TokenExpiry'])[0]) / 1000
                return data['TokenValue'], datetime.fromtimestamp(expiry)
            else:
                raise requests.HTTPError(response.status_code)

    @staticmethod
    def epoch_to_datetime(ds: str, *, fmt: str=None) -> Union[datetime, str]:
        """Convert Datastream DateTime string to datetime or datetime string.
        :param ds: str, Datastream DateTime string.
        :param fmt: str, datetime format.
        :return: datetime or str
        """
        bits = re.findall(r'/Date\((\d+)\+(\d+)\)/', ds)
        ms, tz = bits[0]
        epoch = int(ms) / 1000
        dt = datetime.utcfromtimestamp(epoch)
        tz = timedelta(hours=int(tz))
        t = dt + tz

        if fmt:
            return t.strftime(fmt)
        else:
            return t

    @staticmethod
    def get_query_size(params: dict) -> int:
        return len(urlencode(params))

    def create_tasks(self, instruments: list, *, request_size: int = None, max_request_size: int = None,
                     max_query_size: int = None, max_concr: int = None, verify: bool=None, **kwargs) -> list:
        """Create task group.
        :param instruments: list, Datastream instruments.
        :param datatypes: list, Datastream datatypes.
        :param request_size: int, number of instruments per request.
        :param max_request_size, int, the composite number of items (instruments x datatypes)
                                      will be a maximum of 2000.
        :param max_query_size, int, the maxQueryString set in the Datastream application instance,
                                    defaul to 2500. One could try to verify the
                                    configuration/system.webServer/security/requestFiltering/requestLimits@maxQueryString
                                    setting in the applicationhost.config or web.config file.
        :param max_concr: int, maximum size of concurrent requests for the account, default to 40.
        :return: list
        """
        try:
            instruments[0]
        except IndexError:
            logging.warning('Empty instruments.')
            return []

        max_concr = max_concr if max_concr else 40
        max_query_size = max_query_size if max_query_size else 2500
        max_request_size = max_request_size if max_request_size else 2000

        instruments = list(set(instruments))
        datatypes = list(set(kwargs.pop('datatypes', [''])))

        num_instrs = len(instruments)
        num_dt = len(datatypes)
        theo_instr_request_size = max_request_size / num_dt

        if request_size:
            try:
                assert request_size <= max_request_size
            except AssertionError:
                raise ValueError('request_size cannot be exceeded max_request_size.')
            else:
                instrs = [instruments[i:i + request_size] for i in range(0, num_instrs, request_size)]
                tasks = [task for task in [instrs[i:i + max_concr] for i in range(0, len(instrs), max_concr)] if task]
        else:
            params = self.create_params(instruments, datatypes=datatypes, token=kwargs.pop('token', None),
                                        datekind=kwargs.pop('datekind', None), start=kwargs.pop('start', None),
                                        end=kwargs.pop('end', None), freq=kwargs.pop('freq', None),
                                        props=kwargs.pop('props', None))
            params = {k: v if k != 'instrument' else '' for k, v in params.items()}

            instrs = []
            tasks = [[] for _ in instruments]

            i = 0
            for instr in instruments:
                instrs.append(instr)
                params['instrument'] = ','.join(instrs)
                query_size = self.get_query_size(params)

                if query_size <= max_query_size and len(instrs) <= theo_instr_request_size:
                    pass
                else:
                    tasks[i].append(instrs[:-1])

                    if verify:
                        params['instrument'] = ','.join(instrs[:-1])
                        query_size = self.get_query_size(params)
                        assert query_size <= max_query_size and len(instrs[:-1]) <= theo_instr_request_size

                    instrs = [instrs[-1]]

                if len(tasks[i]) >= max_concr:
                    i += 1

            tasks[i].append(instrs)
            tasks = list(filter(None, tasks))

        if verify:
            assert set([len(x) <= max_concr for x in tasks]) == {True}
            assert set([z for x in tasks for y in x for z in y]) == set(instruments)

        return tasks

    async def to_string(self, df: pd.DataFrame) -> str:
        """Convert dataframe to string.
        :param df: dataframe
        :return: str
        """
        records = df.to_records(index=False)
        lines = records.tolist()
        lines.insert(0, list(records.dtype.fields.keys()))
        lines = '\n'.join([','.join(['' if isinstance(y, float) and math.isnan(y) else str(y)
                                     for y in x])
                                        for x in lines])
        return lines

    async def json_to_dataframe(self, content: str, *, ignore_error_instruments: bool=None,
                                ignore_empty: bool=None, fmt: str=None, callback: bytes=None) -> pd.DataFrame:
        """Convert response json to dataframe.
        :param content: str, response json.
        :param ignore_error_instruments: bool, ignore error instruments.
        :param ignore_empty: bool, ignore row if Value is na.
        :param fmt: str, datetime format, default to %Y%m%d.
        :param callback: bytes, picklable function.
        :return: dataframe
        """
        dates = content['Dates']
        fmt = fmt if fmt else '%Y.%m.%d'

        try:
            n = len(dates)
        except TypeError:
            lst = [pd.DataFrame(y, index=[0]).assign(DataType=x['DataType'], Dates='')
                        for x in content['DataTypeValues'] for y in x['SymbolValues']]
        else:
            dates = [self.epoch_to_datetime(s, fmt=fmt) for s in dates]
            lst = [pd.DataFrame(y, index=range(n)).assign(DataType=x['DataType'], Dates=dates,
                                                          Instrument=content['Instrument'])
                        for x in content['DataTypeValues'] for y in x['SymbolValues']]

        df = pd.concat(lst)
        mask = (df.Type == DataType.DateTime.value)
        nas = ['NA', 'nan']
        df.loc[mask, 'Value'] = df.loc[mask, 'Value'].apply(lambda s: self.epoch_to_datetime(s, fmt=fmt))
        df.loc[~mask, 'Value'] = df.loc[~mask, 'Value'].astype(str).replace(to_replace=nas, value=np.nan, regex=True)
        df.Value = df.Value.fillna(value=np.nan)

        if ignore_error_instruments:
            df = df[df.Type != DataType.Error.value]

        if ignore_empty and df.empty is False:
            df = df[df.Type != DataType.Empty.value]

        callback = pickle.load(callback)
        if callback and df.empty is False:
            try:
                df = callback(df)
            except Exception as e:
                logging.error(f'Callback: {repr(e.__cause__)}')

        if df.empty is False:
            df.drop_duplicates(inplace=True)

        return df

    async def get_data(self, instruments: list, *, fn: str=None, **kwargs) -> pd.DataFrame:
        """Asynchronous retrieve data from the Datastream REST API.
        :param instruments: list, the instruments.
        :param fn, str, filename.
        :return: dataframe
        """
        url = self.endpoint + 'Data?'
        params = self.create_params(instruments, token=kwargs.pop('token', None),
                                    datekind=kwargs.get('datekind', None), datatypes=kwargs.pop('datatypes', ['']),
                                    start=kwargs.pop('start', None), end=kwargs.pop('end', None),
                                    freq=kwargs.pop('freq', None), props=kwargs.pop('props', None))

        ignore_error_instruments = kwargs.pop('ignore_error_instruments', None)
        ignore_empty = kwargs.pop('ignore_empty', None)
        fmt = kwargs.pop('fmt', None)
        callback = BytesIO(pickle.dumps(kwargs.pop('callback', None)))
        json_to_dataframe = partial(self.json_to_dataframe, ignore_empty=ignore_empty, fmt=fmt, callback=callback,
                                    ignore_error_instruments=ignore_error_instruments)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, proxy=self.proxies['http']) as response:
                    params['fn'] = fn

                    if response.status == 200:
                        if fn:
                            logging.info(f'Retrieving {fn}')

                        content = await response.json()
                        content.update({'Instrument': ','.join(instruments)})

                        df = await json_to_dataframe(content)

                        if fn:
                            if df.empty is True:
                                logging.info(f'Empty dataframe and {fn} will not be created.')
                            else:
                                lines = await self.to_string(df)
                                logging.info(f'Saving {fn}.')
                                async with aiofiles.open(fn, 'w') as outfile:
                                    await outfile.write(lines)
                        else:
                            return df
                    else:
                        self.cache['failed'].append(params)
        except asyncio.TimeoutError:
            self.cache['failed'].append(params)

    def fetch(self, instruments: list, *, retry_flag: bool=None, max_retries: int=None, **kwargs)-> list:
        """Fetch data from the Datastream REST API.
        :param instruments: list, the instruments.
        :param retry_flag: bool, retry the failed requests from the cache.
        :param max_retries: int, default to 1.
        :return: list
        """
        attempt = 0
        max_retries = max_retries if max_retries else 1
        park_time = kwargs.pop('park_time', self.park_time)

        if not retry_flag:
            self.cache['result'] = []

        fn = kwargs.pop('fn', None)
        if fn:
            try:
                assert re.findall(r'.*?.(csv)', fn) != []
            except AssertionError:
                raise ValueError('Invalid filename.')

        datatypes = kwargs.pop('datatypes', [''])

        create_tasks_kwargs = dict(
            datatypes=datatypes,
            max_request_size=kwargs.pop('max_request_size', None),
            max_query_size=kwargs.pop('max_query_size', None),
            request_size=kwargs.pop('request_size', None),
            max_concr=kwargs.pop('max_concr', None),
            verify=kwargs.pop('verify', None)
        )
        tasks = self.create_tasks(instruments, **create_tasks_kwargs)

        get_data_kwargs = dict(
            datatypes=datatypes,
            datekind=kwargs.pop('datekind', None),
            start=kwargs.pop('start', None),
            end=kwargs.pop('end', None),
            freq=kwargs.pop('freq', None),
            props=kwargs.pop('props', None),
            ignore_empty=kwargs.pop('token', None),
            token=kwargs.pop('token', None),
            callback=kwargs.pop('callback', None),
            ignore_error_instruments=kwargs.pop('ignore_error_instruments', None),
            fmt=kwargs.pop('fmt', None)
        )
        get_data = partial(self.get_data, **get_data_kwargs)

        c = 0
        lst = []
        lst_append = lst.append
        loop = create_event_loop()

        try:
            logging.info('Sending requests.')
            for i, task_info in enumerate(tasks):
                task = [get_data(x, fn=fn.replace('.csv', f'_{j + c + 1}.csv') if fn else None, **kwargs)
                            for j, x in enumerate(task_info)]
                data = loop.run_until_complete(asyncio.gather(*task))
                logging.info('{0:.0%} to the completion.'.format((i + 1) / len(tasks)))
                c += len(tasks[i])
                lst_append(data)
        except aiohttp.client_exceptions.ClientOSError:
            if attempt <= max_retries:
                get_data_kwargs['fn'] = fn
                self.cache['failed'].extend([dict(tuple(get_data_kwargs.items()) + tuple([('instrument', ','.join(x))]))
                                                for x in task_info])
                logging.info(f'Sleeping {park_time} before retry.')
                sleep(park_time)
                attempt += 1
            else:
                raise
        except:
            logging.exception('Event Loop', exc_info=True)
        else:
            result = [y for x in lst for y in x]
            self.cache['result'].extend(result)
            return self.cache['result']
        finally:
            loop.close()
            if self.cache['failed']:
                try:
                    tmp = self.cache['failed'][-1]
                except IndexError:
                    logging.info('Retry completed.')
                    return self.cache['result']
                else:
                    logging.info('Retrying the failed request.')
                    del self.cache['failed'][-1]
                    failed_instruments = tmp.pop('instrument').split(',')
                    self.fetch(failed_instruments, retry_flag=True, park_time=park_time, max_retries=max_retries,
                               request_size=math.trunc(len(failed_instruments) / 2), **tmp)

