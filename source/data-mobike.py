import pandas as pd
import json
import sqlite3
import csv
from collections import OrderedDict
import numpy as np
from shapely.geometry import shape, Point

def search_district(longitude, latitude, json_file):
    point = Point(longitude, latitude)
    count = 0
    for feature in json_file['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return count
        count = count + 1
    return -1

conn = sqlite3.connect(r'UTSEUS-shanghai-dianping.db')

query = '''
SELECT longitude, latitude, c.category
FROM venues v
	join venue_categories vc on (v.business_id = vc.business_id)
    join categories c on (vc.category = c.category)
WHERE
	(c.parent = '美食' OR c.category = '美食')
    AND longitude > 115
    AND longitude < 123
    AND avg_price > 0
'''

df = pd.read_sql(query, conn)

# load GeoJSON file containing sectors
with open('UTSEUS-CENDUS-shanghai-neighborhood.geojson', encoding="utf8") as f:
    js = json.load(f)

dataset = pd.DataFrame()
data_list = []
df['district_number'] = np.vectorize(search_district)(df['longitude'], df['latitude'], js)

dataset = df

groupby_data = pd.DataFrame({'count' : dataset.groupby(['district_number']).size()}).reset_index()

df_mobike = pd.read_csv('../Data/UTSEUS-MOBIKE-shanghai_full.csv')
df_mobike = df_mobike[((df_mobike['end_time'] < '2016-08-28 09:00') & (df_mobike['end_time'] > '2016-08-28 07:00')) | ((df_mobike['end_time'] < '2016-08-29 09:00') & (df_mobike['end_time'] > '2016-08-29 07:00'))]

df_mobike['start_district'] = np.vectorize(search_district)(df_mobike['start_location_x'], df_mobike['start_location_y'], js)

df_mobike['end_district'] = np.vectorize(search_district)(df_mobike['end_location_x'], df_mobike['end_location_y'], js)

groupby_data_start = pd.DataFrame({'count' : df_mobike.groupby('start_district').size()}).reset_index()
groupby_data_end = pd.DataFrame({'count' : df_mobike.groupby('end_district').size()}).reset_index()

groupby_data_start.columns = ['district_number', 'start_point_count']
groupby_data_end.columns = ['district_number', 'end_point_count']

merged_data = pd.merge(groupby_data, groupby_data_start, how='outer', on='district_number')
merged_data = pd.merge(merged_data, groupby_data_end, how='outer', on='district_number')

merged_data = merged_data.drop(merged_data[(merged_data.count == -1) | (merged_data.start_point_count == -1) | (merged_data.end_point_count == -1)].index)

merged_data.to_csv('data-mobike-lunch.csv', sep='\t', encoding='utf-8')
