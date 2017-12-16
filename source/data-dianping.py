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
    longitude > 115
    AND longitude < 123
    AND avg_price > 0
'''

df = pd.read_sql(query, conn)

# load GeoJSON file containing sectors
with open('UTSEUS-CENDUS-shanghai-neighborhood.geojson', encoding="utf8") as f:
    js = json.load(f)

df['district_number'] = np.vectorize(search_district)(df['longitude'], df['latitude'], js)

groupby_data = pd.DataFrame({'count' : df.groupby(['district_number', 'category']).size()}).reset_index()

groupby_data.info()
groupby_data.to_csv('data-dianping.csv', sep='\t', encoding='utf-8')
