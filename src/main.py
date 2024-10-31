import polars as pl
import pathlib
import datetime

import json
import io
import requests

def process_response(json_string):
    d = json.loads(json_string)

    if (len(d) == 0):
        return pl.DataFrame([])

    data = d[0]['data']

    result_df = pl.DataFrame([])
    for e in data:
        heightDepth = ''
        if 'heightDepth' in e['stationElement']:
            heightDepth = str(e['stationElement']['heightDepth'])

        ordinal = ''
        if 'ordinal' in e['stationElement']:
            ordinal = str(e['stationElement']['ordinal'])

        var_name = e['stationElement']['elementCode'] + ':' + ordinal + ':' + heightDepth
        var_df = pl.read_json(io.StringIO(json.dumps(e['values'])))
        var_df = var_df.rename({'value': var_name})

        if result_df.is_empty():
            result_df = var_df
        else:
            result_df = result_df.join(var_df, on='date', how='left')

    return result_df

def main(args):
    print('main')

    ecodes = ['TAVG','TMAX', 'TMIN','PRCP','SMS:*','STO:*','SAL:*','RDC:*','BATT:*','WDIRV','WSPDV','RHUM','RHENC','SRADT']

    df = pl.DataFrame([])

    for ecode in ecodes:
        url = f'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=2198%3AWA%3ASCAN&elements={ecode}&duration=DAILY&beginDate=2013-07-24&endDate=2017-01-01&periodRef=END&centralTendencyType=NONE&returnFlags=false&returnOriginalValues=false&returnSuspectData=false'
        response = requests.get(url)

        if(response.status_code == 200):
            response_df = process_response(response.text)

            if response_df.is_empty():
                continue

            if df.is_empty():
                df = response_df
            else:
                df = df.join(response_df, on='date', how='left')
        else:
            print(f'Error: {response.status_code}')

        print(df.head())

    df.write_csv((path_output / 'SCAN_AllVariables_Daily_20130724-20170101_20241031.csv'))



if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'
    
    args = {
        'path_scan_json': (path_input / 'SCAN_AllVariables_Daily_20130724-20170101.json'),
        'path_output': path_output}

    main(args)