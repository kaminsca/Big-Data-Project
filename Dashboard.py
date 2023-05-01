from dash import Dash, html, dash_table, dcc
import plotly.express as px
import pandas as pd
import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import geopandas as gpd
import plotly.graph_objs as go
from shapely import MultiPoint
from shapely.geometry import Point, LineString
import zipfile
from dash.dependencies import Input, Output
from datetime import datetime
import datetime as dt





# Setup S3
# aws_access_key_id = input("Enter aws access key id:\n");
# aws_secret_access_key = input("Enter aws secret access key:\n");
# aws_session_token = input("Enter aws session token:\n");
# credentials = {
#     'region_name': 'us-east-1',
#     'aws_access_key_id': aws_access_key_id,
#     'aws_secret_access_key': aws_secret_access_key,
#     'aws_session_token': aws_session_token
# }
# session = boto3.session.Session(**credentials)
# s3 = session.client('s3')


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

"""Uncomment this section to download data from S3"""
# Download data
# print("downloading weather data")
# downloadS3('bigdata-incident-project-clark', 'weather_data/')
# print("downloading traffic data")
# downloadS3('bigdata-incident-project', 'data/traffic.parquet/')
# print("unzipping traffic")
# if not os.path.exists("traffic_data"):
#     os.makedirs("traffic_data")
# traffic = zipfile.ZipFile('traffic.parquet.zip')
# for file in traffic.namelist():
#     if file.startswith('traffic.parquet/'):
#         traffic.extract(file, 'traffic_data')
#downloadS3('bigdata-incident-project-clark', 'data/')
# print("done downloading")

pd.set_option('display.max_columns', None)
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Learning_Spark") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

print("loading data")
roads = gpd.read_file('data/USA_Tennessee.geojson')
roads.plot()
incident_data = spark.read.parquet("data/nfd_incidents_xd_seg.parquet")\
    .withColumn("date", to_date('time_utc')).withColumn("hour", hour("time_utc"))
weather_data = readParquetDirectoryToSpark("weather_data")\
    .select('*').where("station_id == \'KBNA\' OR station_id == \'KJWN\'")\
    .withColumn("date", to_date('timestamp_utc')).withColumn("hour", hour("timestamp_utc"))
# traffic_data = readParquetDirectoryToSpark("data/traffic.parquet")\
#     .withColumn("date", to_date('measurement_tstamp')).withColumn("hour", hour("measurement_tstamp")) \
#     .withColumnRenamed('xd_id', 'XDSegID')
# print("traffic_data 2021 count before group: ")
# count = traffic_data.count()
# traffic_data = traffic_data.groupBy(['date', 'hour']).agg({"congestion": "avg"})


print("data loaded, joining incidents and weather")
incidents_plus_weather = incident_data.join(weather_data, ['date', 'hour'], 'left')\
    .groupBy(['XDSegID', 'date']).agg({'response_time_sec': 'avg'})
# create pandas dataframe from spark dataframe
pandas_incidents_weather = incidents_plus_weather.toPandas()
#print(pandas_incidents_weather[pandas_incidents_weather['XDSegID'] == 136894283])
# Unique weather:
# ['Scattered clouds' 'Few clouds' 'Broken clouds' 'Overcast clouds'
#  'Light rain' 'Clear Sky' 'Moderate rain' 'Heavy rain' 'Mix snow/rain'
#  'Light snow' 'Snow' 'Heavy snow']
#print(pandas_df['description'].unique())


roads = roads[roads.County == 'DAVIDSON']
# Create a new GeoDataFrame with a single Point geometry for each row in the original 'roads' dataframe
points_gdf = gpd.GeoDataFrame(roads.assign(geometry=roads.geometry.apply(lambda g: MultiPoint([Point(x, y) for x, y in g.coords]))),
                              geometry='geometry', crs=roads.crs)

# Explode the Point geometries into separate rows
points_gdf = points_gdf.explode('geometry')

# Reset the index
points_gdf = points_gdf.reset_index(drop=True)

# Convert LineString/MultiLineString to Point geometries
# points = []
# for geometry in roads.geometry:
#     if geometry.geom_type == 'LineString':
#         points.extend(Point(x, y) for x, y in geometry.coords)
#     elif geometry.geom_type == 'MultiLineString':
#         for line in geometry:
#             points.extend(Point(x, y) for x, y in line.coords)

# Create GeoDataFrame of Point geometries
# points_gdf = gpd.GeoDataFrame(roads, geometry=points, crs=roads.crs)

# merge spatial and incidents/weather data
merged_gdf = points_gdf.merge(pandas_incidents_weather, on='XDSegID')
print("merged dataframe info:")
merged_gdf.info()
# print(merged_gdf['date'].unique())


# Get the minimum and maximum date values from the merged_gdf DataFrame
min_date = merged_gdf['date'].min()
max_date = merged_gdf['date'].max()

# Set the marks for the slider
marks = {int(datetime.combine(min_date, datetime.min.time()).timestamp()): str(min_date),
         int(datetime.combine(max_date, datetime.min.time()).timestamp()): str(max_date)}

# Set the default selected date
selected_date = datetime.fromisoformat('2017-01-01')

# Set the range for the slider
date_range = [int(datetime.combine(min_date, datetime.min.time()).timestamp()),
              int(datetime.combine(max_date, datetime.min.time()).timestamp())]


# Start Dash app
app = Dash(__name__)

# create layout
app.layout = html.Div([
    html.H1(children='Response Time'),
    dcc.Graph(
        id='response-time-by-date'
    ),
    dcc.Slider(
        id='date-slider',
        min=date_range[0],
        max=date_range[1],
        step=86400, # Number of seconds in a day
        value=selected_date.timestamp(),
        marks=marks,
    )
])

# create callback function
@app.callback(
    Output('response-time-by-date', 'figure'),
    [Input('date-slider', 'value')]
)
def update_figure(selected_date):
    print("selected date: {}".format(selected_date))
    # Convert the 'date' column of the merged_gdf dataframe to datetime objects
    merged_gdf['datetime'] = merged_gdf['date'].apply(lambda x: int(datetime.combine(x, datetime.min.time()).timestamp()))

    # Calculate the start and end dates of the time window
    days_window = 10
    start_date = selected_date - 86400 * days_window # 86,400 seconds in a day
    end_date = selected_date + 86400 * days_window
    print("start date: {}".format(start_date))
    print("end date: {}".format(end_date))
    # Filter the merged_gdf dataframe based on the time window
    filtered_df = merged_gdf[(merged_gdf['datetime'] >= start_date) & (merged_gdf['datetime'] <= end_date)]
    print(filtered_df)

    # Define the range of sizes you want to map the values to
    size_min = 2
    size_max = 10
    # Compute the minimum and maximum values of the 'avg(response_time_sec)' column
    min_time = filtered_df['avg(response_time_sec)'].min()
    max_time = filtered_df['avg(response_time_sec)'].max()
    # Compute the scaled values: https://stats.stackexchange.com/questions/281162/scale-a-number-between-a-range
    scaled_sizes = (filtered_df['avg(response_time_sec)'] - min_time) / (max_time - min_time) * (size_max - size_min) + size_min
    # Filter out negative and NaN values from scaled_sizes
    valid_sizes = scaled_sizes.apply(lambda x: x if x > 0 and not np.isnan(x) else 0)

    # Convert GeoPandas plot to Plotly figure
    fig = go.Figure(go.Scattermapbox(
        lat=filtered_df.geometry.y,
        lon=filtered_df.geometry.x,
        mode='markers',
        marker=dict(
            size=valid_sizes,
            color=filtered_df['avg(response_time_sec)'],
            colorscale='Viridis', # set the colorscale for the markers
            colorbar=dict(title='Average Response Time (sec)'),
            sizemode='diameter', # set the sizemode to adjust the diameter of markers
            sizemin=size_min, # set the minimum size of markers
            opacity=0.7 # set the opacity of markers
        ),
        text=filtered_df['avg(response_time_sec)']  # display the "avg response time" value as text on the map
    ))

    fig.update_layout(
        mapbox=dict(
            style='open-street-map',
            center=dict(lat=filtered_df.geometry.y.mean(), lon=filtered_df.geometry.x.mean()),
            zoom=10,
        )
    )

    return fig

if __name__ == '__main__':
    print("Starting app server")
    app.run_server()