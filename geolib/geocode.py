import click
import googlemaps
import json
import os
import pandas as pd
import requests
import scourgify
import time
from haversine import haversine, Unit
from pathlib import Path
from tqdm import tqdm

def get_bing_geocode(api_key, address, city, state):
    # when parsing the json result, we use the known structure of the query result
    # this is fragile and will need to be maintained

    url = f"http://dev.virtualearth.net/REST/v1/Locations/US/{state}/{city}/{address}?key={api_key}"
    r = requests.get(url)
    result = json.loads(r.text)
    if r.status_code >= 400:
        return None, f'HTTP Error {result.status_code} ({result.reason}) for {(address, city, state)}'
    if 'resourceSets' not in result:
        return None, f'No Geocode: resourceSets not in result {(address, city, state)}'

    these_locations = result['resourceSets']
    try:
        for l in these_locations[0]['resources']:
            if l['address']['adminDistrict'] == 'SC':
                point = l['point']['coordinates']
                return (float(point[0]), float(point[1])), None
    except:
        raise ValueError('Error pulling bing geocode from response: {result}')

def get_google_geocode(api_key: str, address: str) -> dict:
    ''' Call google geocode api and return a dict containing the google official address google's lat/lon '''

    gmaps = googlemaps.Client(key=api_key)
    query_result = gmaps.geocode(address)
    if not query_result:
        return None, 'Error: Invalid Google geocode result for address: {address}'

    return parse_google_geocode(query_result)

def parse_google_geocode(query_result: dict) -> dict:
    ''' Parse the google geocode query and return the values needed using known query return structure '''

    # when parsing the json result, we use the known structure of the query result
    # this is fragile and will need to be maintained
    geocode = query_result[0]
    google_address = geocode['formatted_address']
    geometry = geocode['geometry']
    location = geometry['location']
    lat = location['lat']
    lon = location['lng']
    # print(location)
    return (float(lat), float(lon)), None

    
@click.command()
@click.option('--address-file', help='Input filename for csv with id, address, city, state columns')
@click.option('--geocode-file', default=None, help='File to output geocode. Locations with data in this file will not be geocoded (i.e. file provides a warm start')
@click.option('--default-state', help='The state code to use in cases where the state is not populated')
@click.option('--creds-file', default='creds.json', help='[Optional] Location of creds json file containing fields bing_api and google_api containing api keys (default="creds.json")')
@click.option('--wait-time', default=0.5, help='[Optional] Time to wait between api calls to keep the api gods from shutting us off (default: 0.5)')
def geocode(address_file, geocode_file, default_state, creds_file, wait_time):
    geocode_helper(address_file, geocode_file, default_state, creds_file, wait_time)

def geocode_helper(address_file, geocode_file, default_state, *, creds_file='creds.json', wait_time=0.5):
    creds = json.load(open(creds_file))
    input_headers = ['location', 'address']
    output_headers = ['location', 'address', 'normalized_address', 'bing_lat', 'bing_lon', 'google_lat', 'google_lon', 'haversine_m']
    input_fp = Path(address_file)
    if input_fp.exists():
        input_df = pd.read_csv(input_fp)
        missing_columns = [x for x in input_headers if x not in set(input_df.columns)]
        if missing_columns:
            raise ValueError(f'Address file: {missing_columns = }')
    else:
        raise ValueError('Address File does not exist: {address_file}')
    geocode_filepath = Path(geocode_file)
    known_geocodes = set()
    geocode_df = None
    if geocode_filepath.exists():
        geocode_df = pd.read_csv(geocode_file)
        missing_columns = [x for x in output_headers if x not in set(geocode_df.columns)]
        if missing_columns:
            raise ValueError(f'Geocode file: {missing_columns = }')
        known_geocodes = set(zip(geocode_df.location, geocode_df.address))
        
    geocodes = set(zip(input_df.location, input_df.address)) - known_geocodes
    if not geocodes:
        print('All {len(input_df)} rows are previously geocoded')

    rows = []
    
    for (location, address) in tqdm(geocodes):
        normalized_address_record = scourgify.normalize_address_record(address)
        norm_addr, norm_city, norm_state, norm_zip = (normalized_address_record["address_line_1"],
                                                      normalized_address_record["city"],
                                                      normalized_address_record["state"],
                                                      normalized_address_record['postal_code'])
        norm_state = norm_state if norm_state else default_state
        new_address = f'{norm_addr}, {norm_city}, {norm_state} {norm_zip}'
        bing_loc, bing_error = get_bing_geocode(creds['bing_api'], norm_addr, norm_city, norm_state)
        google_loc, google_error = get_google_geocode(creds['google_api'], new_address)
        distance = round(haversine(bing_loc, google_loc, unit=Unit.METERS))
        row = dict(zip(output_headers, (location, address, new_address, bing_loc[0], bing_loc[1], google_loc[0], google_loc[1], distance)))
        rows.append(row)
        these_results = pd.DataFrame(rows)
        new_results = pd.merge(input_df, pd.concat([geocode_df, these_results]) if geocode_df is not None else these_results, on=['location', 'address'], how='right')
        new_results.sort_values('location').to_csv(geocode_file, index=False)
        
        time.sleep(wait_time)

    
if __name__ == '__main__':
    geocode()
