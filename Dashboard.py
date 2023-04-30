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
import zipfile
from dash.dependencies import Input, Output


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
        if obj['Size'] > 0:
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
# print("downloading weather data")
# downloadS3('bigdata-incident-project-clark', 'weather_data/')
# print("downloading traffic data")
# downloadS3('bigdata-incident-project-clark', 'traffic_data/')
# print("unzipping traffic")
# if not os.path.exists("traffic_data"):
#     os.makedirs("traffic_data")
# traffic = zipfile.ZipFile('traffic.parquet.zip')
# for file in traffic.namelist():
#     if file.startswith('traffic.parquet/'):
#         traffic.extract(file, 'traffic_data')
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

# create pandas dataframe from spark dataframe
pandas_df = incidents_plus_traffic.toPandas()

# convert latitude and longitude to geometry column
pandas_df['geometry'] = gpd.points_from_xy(pandas_df.longitude, pandas_df.latitude)

# create datetime column
pandas_df['datetime'] = pd.to_datetime(pandas_df.time_utc, unit='ms')

# extract date, hour, and weekday columns
pandas_df['date'] = pandas_df.datetime.dt.date
pandas_df['hour'] = pandas_df.datetime.dt.hour
pandas_df['day_of_week'] = pandas_df.datetime.dt.dayofweek

# set the geometry column as the index
gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')

# group by geometry and date to get average response time
response_times = gdf.groupby(['geometry', 'date']).agg({'response_time_sec': 'mean'}).reset_index()

app = Dash(__name__)

# create layout
app.layout = html.Div([
    html.Div(children='Roads:'),
    dcc.Graph(
        id='my-graph',
        figure = fig
    ),
    html.H1(children='Response Time by Location'),
    dcc.Graph(id='map-graph'),
    dcc.Slider(
        id='date-slider',
        min=response_times['date'].min().strftime('%Y-%m-%d'),
        max=response_times['date'].max().strftime('%Y-%m-%d'),
        value=response_times['date'].max().strftime('%Y-%m-%d'),
        marks={date.strftime('%Y-%m-%d'): date.strftime('%Y-%m-%d') for date in response_times['date'].unique()},
        step=None
    )
])

# create callback to update map
@app.callback(
    Output('map-graph', 'figure'),
    Input('date-slider', 'value')
)
def update_map(selected_date):
    filtered_df = response_times[response_times['date'] == selected_date]
    new_fig = px.scatter_mapbox(filtered_df, lat='geometry.latitude', lon='geometry.longitude', color='response_time_sec',
                            hover_data={'geometry.latitude': False, 'geometry.longitude': False, 'response_time_sec': ':.2f'},
                            zoom=10, height=600)
    new_fig.update_layout(mapbox_style='open-street-map')
    return new_fig

if __name__ == '__main__':
    print("Starting app server")
    app.run_server(debug=True)