import pandas as pd
import json
import sqlite3
import csv
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

df['district_number'] = np.vectorize(search_district)(df['longitude'], df['latitude'], js)

groupby_data = pd.DataFrame({'count' : df.groupby(['district_number']).size()}).reset_index()

df_anjuke = pd.read_csv('../Data/UTSEUS-anjuke-real-estate.csv')
df_anjuke['district_number'] = np.vectorize(search_district)(df_anjuke['longitude'], df_anjuke['latitude'], js)

groupby_district = pd.DataFrame({'restaurants_count' : df_anjuke.groupby('district_number').size()}).reset_index()
groupby_district.columns = ['district_number', 'restaurants_count']

groupby_district.to_csv('data-real-estate.csv', sep='\t', encoding='utf-8')
