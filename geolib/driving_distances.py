import click
import requests
import json
import time
import pandas as pd
from tqdm import tqdm
from haversine import haversine, Unit

def get_distance(source, dest, server='http://localhost:8080/ors/v2/directions/driving-car'):
    url = f'{server}?start={source[0]},{source[1]}&end={dest[0]},{dest[1]}'
    r = json.loads(requests.get(url).text)
    if 'error' in r:
        return None
    return r['features'][0]['properties']['segments'][0]['distance']    

# locations is a dictionary of location names and coordinates
# coordinates in order [longitude, latitude]
# sources and dests are lists of location names
# gets driving distances
def get_locations_and_distances(locations: dict, source_names: list, dest_names: list, *, key: str = None, server: str='https://api.openrouteservice.org/v2/matrix/driving-car'):
    # remove duplicates
    source_names= list(set(source_names))
    dest_names= list(set(dest_names))
    
    # list of location coordinates to be passed into helper
    loc_coordinates= []
    # iterator initialized at zero which will help build the index lists to be passed into helper
    # make list of source indices
    source_index= []
    for i, s in enumerate(source_names):
        temp= locations.get(s)
        # if the location is not in the locations dictionary, raise exception
        if temp == None:
            raise Exception(f"Source site '{s}' not found in locations") 
        # add the coordinate matching the source 's'
        loc_coordinates.append(temp)
        source_index.append(i)
    source_count = len(source_names)
        
    # make list of dest indices
    dest_index= []
    for i, d in enumerate(dest_names):
        temp= locations.get(d)
        # if the location is not in the locations dictionary, raise exception
        if temp == None:
            raise Exception(f"Destination site '{d}' not found in locations")
            
        # add the coordinate matching the destination 'd'
        loc_coordinates.append(temp)
        dest_index.append(i+source_count)
    
    # call the helper to get the distances as a list
    return (source_names, dest_names, get_distances_helper(loc_coordinates, source_index, dest_index, server, key))
    
def convert_to_df(source_names, dest_names, distances):
    # convert list of distances to pandas dataframe
    values= []
    for source, dist in zip(source_names, distances):
        these_values= {'source': source}
        for dest, d in zip(dest_names, dist):
            these_values[dest]= d
        values.append(these_values)
        
    df= pd.DataFrame(values)
    return df

# locations should be the coordinates of each location in the list
# sources and destinations should be lists of the indexes of the chosen locations in locations
# key is your authorization key
# the returned list of distances is in kilometers
def query_driving_distance_api(locations: list, sources: list, dests: list, server: str, key: str, *, metric="distance"): 
    # initialize the body
    body = {"locations":locations, "destinations":dests, "metrics":[metric], "sources":sources}
    
    # initialize the headers
    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Content-Type': 'application/json; charset=utf-8'
    }

    if key:
        headers['Authorization']= key

    # exits if there is an error
    # may want to revise this block to include different responses to specific errors
    try:
        call = requests.post(server, json=body, headers=headers)
    except requests.exceptions.RequestException as e:
        print(call.reason)
    
    # get the json of the call
    jason = json.loads(call.text)
    return jason

def get_distances_helper(locations: list, sources: list, dests: list, server: str, key: str, *, metric="distance") -> list:
    
    # initialize the body
    body = {"locations":locations, "destinations":dests, "metrics":[metric], "sources":sources}
    
    # initialize the headers
    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Content-Type': 'application/json; charset=utf-8'
    }

    if key:
        headers['Authorization']= key

    # exits if there is an error
    # may want to revise this block to include different responses to specific errors
    try:
        call = requests.post(server, json=body, headers=headers)
    except requests.exceptions.RequestException as e:
        print(call.reason)
    
    # get the json of the call
    jason = json.loads(call.text)
    return jason['distances']

def get_distances(locations: list, source_names: list, dest_names: list, *, server: str='https://api.openrouteservice.org/v2/matrix/driving-car', key: str=None, dataframe=pd.DataFrame()):
    if dataframe.empty:
        distances= get_locations_and_distances(locations, source_names, dest_names, key=key, server=server)
        return convert_to_df(distances[0], distances[1], distances[2]) 
    else:
        #display(dataframe)
        # get list of sources and destinations that are already in current dataframe
        df_dests= dataframe.columns
        df_sources= list(dataframe['source'])
    
        # use those to find all sources and destinations in the list that aren't in the current dataframe
        new_sources= list(set(source_names).difference(set(df_sources)))
        new_dests= list(set(dest_names).difference(set(df_dests)))
    
        # We need to keep track of the source and destination of each distance
        if new_dests != []:
            
            new_columns= get_locations_and_distances(locations, df_sources, new_dests, key=key, server=server)
            # add the columns
            # for each new destination
            for dest, dists in zip(new_columns[1], new_columns[2]):
                # Define a dictionary with key values of
                # an existing column and their respective
                # value pairs as the # values for our new column.
                dests = {}
                for source, d in zip(new_columns[0], dists):
                    dests[source]= d

                df2= pd.DataFrame([dests])
                # Provide 'Address' as the column name
                dataframe= pd.merge(dataframe, df2, how="outer", on=["source"])
        if new_sources != []:
            new_rows= get_locations_and_distances(locations, new_sources, dest_names, key=key, server=server)
            
            # add the rows
            # for each new source
            for source, dists in zip(new_rows[0], new_rows[2]):
                new_values= {'source': source}
                for dest, d in zip(new_rows[1], dists):
                    new_values[dest]= d
                df2 = pd.DataFrame([new_values])
                # dataframe.append(new_values, ignore_index=True)
                dataframe= pd.concat([dataframe, df2], ignore_index=True)

            return dataframe


def get_missing_origins(df):
    num_dests = len(set(df.id_dest))
    missing_orig_counts = df[pd.isnull(df.driving_m)].groupby('id_orig').count().reset_index()
    return set(missing_orig_counts.id_orig)

def estimate_origin(origin, df, locations_dict, distance_factor=1.3):
    # this is for origins that are not returning driving distances
    # for each destination, we'll have driving distances from all the known sources
    # we add the haversine distance from the bad origin to each of those origins
    # and take the minimum haversine+driving distance over all known origins
    print(locations_dict[origin])
    df = df.rename(columns={'id_orig':'midpoint'})
    df['id_orig'] = origin
    df['distance_to_mid'] = df.midpoint.apply(lambda midpoint:haversine(tuple(reversed(locations_dict[origin])), tuple(reversed(locations_dict[midpoint])), unit=Unit.METERS))
    df = df[df.distance_to_mid < 1000].copy()
    df['driving_distance'] = df.driving_m+df.distance_to_mid
    new_df = df.sort_values('driving_distance').groupby(['id_orig', 'id_dest']).first().reset_index()
    return new_df

def get_bad_locations(origins, destinations, locations_dict):
    # go through the set of locations and see if we get one
    # good distance result back
    # a bad location will not return a distance to anywhere
    # you will probably want to remove bad locations from the input
    import sys
    good_destination = None
    good_origin = None
    for o in origins:
        if good_origin: continue
        for d in destinations:
            r = get_distance(locations_dict[o], locations_dict[d])
            if r is not None:
                good_destination = d
                good_origin = o
                break

    bad_origins = set()
    bad_destinations = set()
    print('checking origins')
    for o in tqdm(origins):
        r = get_distance(locations_dict[o], locations_dict[good_destination])
        if r is None:
            bad_origins.add(o)

    print('checking destinations')
    for d in tqdm(destinations):
        r = get_distance(locations_dict[good_origin], locations_dict[d])
        if r is None:
            bad_destinations.add(o)

    return bad_origins, bad_destinations


@click.command()
@click.option('--sources-file', help='file containing lat/long of census blocks')
@click.option('--destinations-file', help='file containing lat/long of voting locations')
@click.option('--output-file', help='output file for distances')
@click.option('--check-bad-locations', default=False, help='do a thorough look for bad origins and destinations')
def get_all_distances(sources_file, destinations_file, output_file, check_bad_locations):
    origins = pd.read_csv(sources_file)
    destinations = pd.read_csv(destinations_file)
    source_names = list(origins.id)
    dest_names = list(destinations.id)
    locations_dict = {a:[c,b] for a, b, c in zip(origins.id, origins.lat, origins.lon)}
    locations_dict |= {a:[c,b] for a, b, c in zip(destinations.id, destinations.lat, destinations.lon)}
    
    # this first part runs through the input to see what doesn't have data
    if check_bad_locations:
        bad_origins, bad_destinations = get_bad_locations(source_names, dest_names, locations_dict)
        # you probably want to deal with these up front either by removing them or checking the locations manually
        # I recall that the bg centroids can be bad because they don't necessarily refer to any actual location
        print(bad_origins, bad_destinations)
        return
    df = None
    try:
        df = pd.read_csv(output_file)
    except:
        pass

    # looks like we bail if results are already calculated
    if df is not None:
        # see which sources are complete
        df = df[~pd.isnull(df.driving_m)].copy()
        my_origins = set(df.id_orig)
        for o in set(source_names)-my_origins:
            print('missing', o, tuple(reversed(locations_dict[o])))
        return

    # we need to do a few at a time to stay under the limit
    # we will do max 10 at a time, but one could push this
    # the run time doesn't increase tremendously when running locally by doing more calls
    source_increment = min(int(2500/len(dest_names))-1, 10)
    starting_index = 0

    from datetime import datetime
    individual_sources = set()
    dfs = []
    # do the next set
    while starting_index < len(source_names):
        start_time = datetime.now()
        try:
            these_sources = source_names[starting_index:starting_index+source_increment]
            this_df = get_distances(locations_dict, these_sources, dest_names, server=url)
            dfs.append(this_df)
        except KeyError:
            # if the call failed, we need to run through these sources one at a time
            individual_sources |= set(these_sources)
        starting_index += source_increment
        duration = (datetime.now()-start_time).total_seconds()
        print(f'{starting_index}/{len(source_names)}: {duration:.4f} seconds')
    df = pd.concat(dfs)
    df = df.melt(id_vars=['source'], var_name='dest_id', value_name='distance').rename(columns={'source':'id_orig', 'dest_id':'id_dest', 'distance':'driving_m'})
    df['source'] = 'driving distance'
    # for the sources that failed, we run through these one at a time
    if individual_sources:
        individual_rows = []
        for o in tqdm(individual_sources):
            time.sleep(0.1)
            for d in dest_names:
                try:
                    distance = get_distance(locations_dict[o], locations_dict[d])
                except:
                    continue
                if distance is not None:
                    individual_rows.append({'id_orig':o, 'id_dest':d, 'driving_m':distance, 'source':'driving_distance'})
            individual_df = pd.DataFrame(individual_rows)
            df = pd.concat([df, individual_df])
    df.to_csv(output_file, index=False)

    # here we take the origins for which we couldn't get data and estimate the distances
    missing_origins = get_missing_origins(df)
    print(missing_origins)
    if missing_origins:
        print(f'populating missing origins: {missing_origins}')
        estimated_dfs = []
        for origin in missing_origins:
            odf = estimate_origin(origin, df, locations_dict)
            odf['source'] = odf.midpoint.apply(lambda x:f'Missing distance, snapped to {x}: {tuple(reversed(locations_dict[x]))}')
            estimated_dfs.append(odf[['id_orig', 'id_dest', 'driving_distance', 'source']].rename(columns={'driving_distance':'driving_m'}))

        missing_origin_df = pd.concat(estimated_dfs)
        good_df = df[~pd.isnull(df.driving_m)].copy()
        good_df['source'] = 'driving distance'
        final_df = pd.concat([good_df, missing_origin_df])
        final_df.to_csv(output_file, index=False)
    
if __name__ == '__main__':
    get_all_distances()
