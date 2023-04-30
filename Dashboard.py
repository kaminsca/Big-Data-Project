from dash import Dash, html, dash_table, dcc
import plotly.express as px
import pandas as pd
import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import geopandas as gpd
import plotly.graph_objs as go
from shapely.geometry import Point, LineString



spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Learning_Spark") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Setup S3
aws_access_key_id = input("Enter aws access key id:\n");
aws_secret_access_key = input("Enter aws secret access key:\n");
aws_session_token = input("Enter aws session token:\n");
credentials = {
    'region_name': 'us-east-1',
    'aws_access_key_id': aws_access_key_id,
    'aws_secret_access_key': aws_secret_access_key,
    'aws_session_token': aws_session_token
}
session = boto3.session.Session(**credentials)
s3 = session.client('s3')


def downloadS3(bucket_name, directory_name):
    # create a local directory to store the downloaded files
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    # get a list of all objects in the directory
    objects = s3.list_objects(Bucket=bucket_name, Prefix=directory_name)

    # download each object in the directory
    for obj in objects['Contents']:
        # check if the object is a file
        if obj['Key'].endswith('/'):
            continue
        # create the file path
        file_path = os.path.join(directory_name, os.path.basename(obj['Key']))
        # download the file
        s3.download_file(bucket_name, obj['Key'], file_path)


# Read and combine parquet files from a directory into a spark df
def readParquetDirectoryToSpark(directory):
    file_path_list = []
    files = os.listdir(directory)
    for file in files:
        if ".parquet" in file and "part-" in file:
            file_path_list.append(directory + "/" + file)
    return spark.read.parquet(*file_path_list)


# Download data
downloadS3('bigdata-incident-project-clark', 'weather_data/year=2020')
#downloadS3('bigdata-incident-project-clark', 'traffic_data/')
#downloadS3('bigdata-incident-project-clark', 'data/')

roads = gpd.read_file('data/USA_Tennessee.geojson')
roads = roads[roads.County == 'DAVIDSON']
roads.plot()

# Convert LineString/MultiLineString to Point geometries
points = []
for geometry in roads.geometry:
    if geometry.geom_type == 'LineString':
        points.extend(Point(x, y) for x, y in geometry.coords)
    elif geometry.geom_type == 'MultiLineString':
        for line in geometry:
            points.extend(Point(x, y) for x, y in line.coords)

# Create GeoDataFrame of Point geometries
points_gdf = gpd.GeoDataFrame(geometry=points, crs=roads.crs)

# Convert GeoPandas plot to Plotly figure
fig = go.Figure(go.Scattermapbox(
    lat=points_gdf.geometry.y,
    lon=points_gdf.geometry.x,
    mode='markers',
    marker=dict(size=2, color='black'),
))

fig.update_layout(
    mapbox=dict(
        style='open-street-map',
        center=dict(lat=points_gdf.geometry.y.mean(), lon=points_gdf.geometry.x.mean()),
        zoom=10,
    )
)

# incident_data = spark.read.parquet('/content/nfd_incidents_xd_seg.parquet')\
#     .withColumn("date", to_date('time_utc')).withColumn("hour", hour("time_utc"))
# weather_data = spark.read.parquet('/content/weather/')\
#     .select('*').where("station_id == \'KBNA\' OR station_id == \'KJWN\'")
# traffic_data = spark.read.parquet('/content/traffic/')\
#     .withColumn("date", to_date('measurement_tstamp')).withColumn("hour", hour("measurement_tstamp"))
#
# incidents_plus_traffic = incident_data.join(traffic_data,[incident_data.date == traffic_data.date, incident_data.hour == traffic_data.hour, incident_data.XDSegID == traffic_data.xd_id], 'left').select('*').where('speed > 0')




app = Dash(__name__)


print("Creating layout")
app.layout = html.Div([
    html.Div(children='Roads:'),
    dcc.Graph(
        id='my-graph',
        figure= fig
    )
])

if __name__ == '__main__':
    print("Starting app server")
    app.run_server(debug=True)