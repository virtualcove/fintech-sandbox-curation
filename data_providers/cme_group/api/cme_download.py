import datetime
import gzip
from os.path import join, split
import requests

# Change these for your login & key
api_login = 'api_yourname'
api_password = 'yourkey'


def output_binary_file(filename, content, output_dir):
    with open(join(output_dir, filename), 'wb') as file:
        file.write(content)


def output_text_file(filename, content, output_dir):
    with open(join(output_dir, filename), 'wb') as file:
        file.write(content)


def specific_download(fid, output_dir):
    """
    Download a specific data file from CME

    Parameters
    ----------
    fid: str
        The fileID retrieved from the JSON file access by list_download
        e.g. '20151003-EOD_xcme_zl_opt_0-eth_p'
    output_dir: str
        Target folder for downloaded data
    """
    url = 'https://datamine.cmegroup.com/cme/api/v1/download?fid={0}'.format(fid)
    print ('Making specific request for data: ' + url)
    r = requests.get(url, auth=(api_login, api_password))

    filename = fid + '.csv.gz'
    print ('Request completed, writing to ' + filename)
    output_binary_file(filename, r.content, output_dir)


def list_download(date, dataset, exchange_code, output_dir, foi_indicator=None):
    """
    Download JSON list of all available .csv's for download.
    See http://www.cmegroup.com/market-data/datamine-api.html for list of values.
    
    Parameters
    ----------
    date: str
        Desired date in YYYYMMDD format
    dataset: str
        Desired dataset ("eod")
    exchange_code: str
        Code for desired exchange (e.g. 'xnym' = NYMEX)
    output_dir: str
        Target folder for downloaded data
    foi_indicator: str (optional)
        Future/Options indicator ('fut' = futures, 'opt' = options, 'idx' = indices)
    """
        
    url = 'https://datamine.cmegroup.com/cme/api/v1/list?dataset={0}&yyyymmdd={1}&exchangecode={2}'.format(dataset, date, exchange_code)

    if foi_indicator:
        url = '{0}&foiindicator={1}'.format(url, foi_indicator)
    print ('Making request for list data for: ' + date)
    r = requests.get(url, auth=(api_login, api_password))

    filename = '{0}_{1}_{2}'.format(date, dataset, exchange_code )
    if foi_indicator: 
        filename = '{0}_{1}'.format(filename, foi_indicator)
    filename = '{0}.csv'.format(filename)

    print ('Request completed, writing to ' + filename)
    output_text_file(filename, r.content, output_dir)


def batch_download(date, dataset, period, output_dir):
    """
    Download entire day's worth of data for given dataset and period
    See http://www.cmegroup.com/market-data/datamine-api.html for list of values.
    
    Parameters
    ----------
    date: str
        Desired date in YYYYMMDD format
    dataset: str
        Desired dataset ("eod")
    period: str 
        ??? ('f', 'e', or 'p')
    output_dir: str
        Target folder for downloaded data
    """
        
    url = 'https://datamine.cmegroup.com/cme/api/v1/batchdownload?dataset={0}&yyyymmdd={1}&period={2}'.format(dataset, date, period)

    print ('Making request for EOD Data for: ' + date)
    r = requests.get(url, auth=(api_login, api_password))

    filename = '{0}_full_{1}_{2}.csv.gz'.format(date, dataset, period)
    print ('Request completed, writing to ' + filename)
    output_binary_file(filename, r.content, output_dir)


this_dir, _ = split(__file__)
data_dir = join(this_dir, 'data')


#
# Examples
#

# Download list of all options available .csv's for a given date
# list_download('20161004', 'eod', 'xnym', data_dir, 'opt')

# Download an entire day's worth of options and futures
# batch_download('20161004', 'eod', 'p', data_dir)

# Download a month's worth of options data for Lumber 
# for day in range(1, 32):
#    date = '201610' + str(day).zfill(2)
#    specific_download(date + '-EOD_xcme_zl_opt_0-eth_p', data_dir)
