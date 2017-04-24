
import pandas as pd
import numpy as np
import json
import requests
import os.path

base_url = 'https://comtrade.un.org/api/get?'


def download_trade_data(filename, human_readable=False, verbose=True,
    period='recent', frequency='A', reporter='USA', partner='all', product='total', tradeflow='exports'):

    """
    Downloads records from the UN Comtrade database and saves them in a csv-file with the name "filename".

    There are two modes:
    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed
    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems

    Additional option: verbose = False in order to suppress both messages from the API and messages like '100 records downloaded and saved in filename.csv' (True is default)

    Parameters of the API call:
    Using parameter values suggested in the API documentation should always work.
    For the parameters reporter, partner and tradeflow more intuitive options have been added.
     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or 'YYYY-YYYY'/ 'YYYYMM-YYYYMM' or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'
     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)
     - reporter   [r]    : reporter code/ name (case-sensitive!) or list of reporter codes/ names or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)
     - partner    [p]    : partner code/ name  (case-sensitive!) or list of partner codes/ names or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)
     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all'
     - tradeflow  [rg]   : 'import[s]' or 'export[s]'; see https://comtrade.un.org/data/cache/tradeRegimes.json for further, lower-level options

     Information copied from the API Documentation (https://comtrade.un.org/data/doc/api/):

     Usage limits
     Rate limit (guest): 1 request every second (per IP address or authenticated user).
     Usage limit (guest): 100 requests per hour (per IP address or authenticated user).
     Parameter combination limit: ps, r and p are limited to 5 codes each. Only one of the above codes may use the special ALL value in a given API call.
     Classification codes (cc) are limited to 20 items. ALL is always a valid classification code.
     If you hit a usage limit a 409 (conflict) error is returned along with a message specifying why the request was blocked and when requests may resume.

     Stability
     Notice: this API may be considered stable. However, new fields may be added in the future.
     While this API is still subject to change, changes that remove fields will be announced and a method of accessing legacy field formats will be made available during a transition period.
     New fields may be added to the CSV or JSON output formats without warning. Please write your code that accesses the API accordingly.
     """

     # TODO multiple downloads

    # (1) replace more convenient input options by ones that can by understood by API
    #     e.g. replace country names by country codes

    reporter = transform_reporter(reporter)
    partner = transform_partner(partner)
    tradeflow = transform_tradeflow(tradeflow)
    # period = transform_period(period, frequency)

    # (2)

    dataframe = download_trade_data_base(human_readable=human_readable, verbose=verbose,
        period=period, frequency=frequency, reporter=reporter, partner=partner, product=product, tradeflow=tradeflow)

    # (3)

    if dataframe is not None:
        filename = filename if len(filename.split('.')) == 2 else filename + '.csv' # add '.csv' if necessary
        dataframe.to_csv(filename)
        if verbose: print('{} records downloaded and saved as {}.'.format(len(dataframe), filename))


def download_trade_data_base(human_readable=False, verbose=True,
    period='recent', frequency='A', reporter=842, partner='all', product='total', tradeflow=2):

    """

    Downloads records from the UN Comtrade database and returns pandas dataframe.

    There are two modes:
    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed
    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems

    Additional option: verbose = False in order to suppress messages from the API (True is default)

    Parameters of the API call:
    As documented in the API documentation.
    More intuitive options for the parameters reporter, partner and tradeflow are only available in the function 'download_trade_data'!
     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'
     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)
     - reporter   [r]    : reporter code or list of reporter codes or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)
     - partner    [p]    : partner code or list of partner codes or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)
     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all'
     - tradeflow  [rg]   : 1 (for imports) or 2 (for exports); see https://comtrade.un.org/data/cache/tradeRegimes.json for further options

    """

    fmt = 'csv' if human_readable else 'json'
    head = 'H' if human_readable else 'M'

    parameters = {
        'ps': period,
        'freq': frequency,
        'r': reporter,
        'p': partner,
        'cc': product,
        'rg': tradeflow,
        'px': 'HS',      # Harmonized System (as reported) as classification scheme
        'type': 'C',     # Commodities ('S' for Services)
        'fmt': fmt,      # format of the output
        'max': 50000,    # maximum number of rows -> what happens if number of rows is bigger?
                         # https://comtrade.un.org/data/dev/portal#subscription says it is 100 000
        'head': head     # human readable headings ('H') or machine readable headings ('M')
    }

    url = base_url + dict_to_string(parameters)

    if human_readable:

        dataframe = pd.read_csv(url)

    else:

        json_dict = requests.get(url).json()

        n_records = json_dict['validation']['count']['value']
        message = json_dict['validation']['message']

        if not json_dict['dataset']:
            if verbose: print('Error: empty dataset \n Message: {}'.format(message))
            dataframe = None

        else:
            if verbose and message: print('Message: {}'.format(message))
            dataframe = pd.DataFrame.from_dict(json_dict['dataset'])

    return dataframe

###############################################################################

def transform_reporter(reporter):
    # if single country code/ name, convert to list
    reporter = [reporter] if not isinstance(reporter, list) else reporter
    # replace country names by country codes
    reporter = [r if is_country_code(r) else find_reporter_code(r) for r in reporter]
    return reporter

def transform_partner(partner):
    # if single country code/ name, convert to list
    partner = [partner] if not isinstance(partner, list) else partner
    # replace country names by country codes
    partner = [p if is_country_code(p) else find_partner_code(p) for p in partner]
    return partner

def transform_tradeflow(tradeflow):
    # replace tradeflow "import(s)" or "export(s)" by the corresponding numbers (1 / 2)
    if isinstance(tradeflow, str):
        if 'export' in tradeflow.lower():
            tradeflow = 2
        elif 'import' in tradeflow.lower():
            tradeflow = 1
    return tradeflow

def transform_period(period, frequency):

    # possibilities:
    # ['recent'], 'recent' (similar: 'now' and 'all')
    # YYYY/ YYYYMM (both int and str, both as single element of list or single object)
    # 'YYYY-YYYY' or 'YYYYMM-YYYYMM' (only str, both as single element of list or single object)
    # [YYYY, 'YYYY-YYYY', 'YYYYY', ...] or some with YYYYMM
    # check whether format corresponds to frequency

    # TODO !

    period = [period] if not isinstance(period, list) else period
    if not len(period) == 1 and period[0] in ['recent', 'all', 'now']:
        for p in period:
            if '-' in p:
                p_splitted = '-'.split(p)
        if all([isdigit(p) or isinstance(p, int) for p in period]):
        else:
            raise ValueError("Input for period is a string but neither 'recent', 'all', 'now' nor a digit!'')

    return period


def is_country_code(inpt):
    """
    checks if inpt is a valid country code, i.e. an integer, an integer converted to a string or 'all'
    output: True or False
    """
    if isinstance(inpt, str):
        return inpt.lower() == 'all' or inpt.isdigit()
    else:
        return isinstance(inpt, int) or isinstance(inpt, np.int64)


def find_reporter_code(country):
    """
    see 'find_country_code'
    """
    return find_country_code(country, 'reporter')

def find_partner_code(country):
    """
    see 'find_country_code'
    """
    return find_country_code(country, 'partner')


def find_country_code(country, reporter_or_partner):
    """
    tries to find the country code corresponding to a country name
    procedure: try to find exact match, if not look for partial matches

    input: country name or part of country name (case-sensitive!)
    output: country code
    """

    # we use a local copy of the file with country codes so that we do not have to use
    # https://comtrade.un.org/data/cache/reporterAreas.json every time
    if not os.path.exists(reporter_or_partner + 'Areas.csv'):
        download_country_codes_file(reporter_or_partner)
    df = pd.read_csv(reporter_or_partner + 'Areas.csv', encoding='latin_1', index_col=0)

    # look for an exact match
    mask = (df.text == country)
    if sum(mask) == 1:
        code = df.index[mask].tolist()[0]
        return code

    # look for a partial match
    # this might be useful because some 'official' names of countries are not always that well-known
    # e.g. 'Bolivia (Plurinational State of)' instead of Bolivia'
    mask2 = (df.text.str.contains(country))
    if sum(mask2) > 0:
        print('There is no country in the json-file with the exact name "{}". '.format(country) + \
            'The following countries contain the word "{}". '.format(country) + \
            'If you think that one of the following countries is the one that you are looking for, press "y".')
        dict_matches = df[mask2].text.to_dict()
        for code, country in dict_matches.items():
            response = input('{} {} [y?] '.format(code, country))
            if response == 'y':
                return code

    # if no code could be found:
    raise LookupError('It was not possible to find a code that corresponds to the country {}.'.format(country))


def download_country_codes_file(reporter_or_partner):
    """
    downloads either the reporter or the partner file
    input: 'reporter' or 'partner'
    """
    url = 'https://comtrade.un.org/data/cache/{}Areas.json'.format(reporter_or_partner)
    json_dict = requests.get(url).json()
    df = pd.DataFrame.from_dict(json_dict['results'])
    df = df.set_index('id')
    df.drop('all', inplace=True)
    df.to_csv('{}Areas.csv'.format(reporter_or_partner))


def dict_item_to_string(key, value):
    """
    inputs: key-value pairs from a dictionary
    output: string 'key=value' or 'key=value1,value2' (if value is a list)
    examples: 'fmt', 'csv' => 'fmt=csv' or 'r', [124, 484] => 'r=124,484'
    """
    value_string = str(value) if not isinstance(value, list) else ','.join(map(str, value))
    return '='.join([key, value_string])


def dict_to_string(parameters):
    """
    input: dictionary of parameters
    output: string 'key1=value1&key2=value2&...'
    """
    return '&'.join(dict_item_to_string(key, value) for key, value in parameters.items())
