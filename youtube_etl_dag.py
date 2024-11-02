from airflow.decorators import dag, task
import pandas as pd
from googleapiclient.discovery import build
import sqlalchemy as sa
import logging


logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')
# following need to be stored in some env file
API_KEY = "AIzaSyCQj7O1q8OUqRhH6Z_1a6qJ5W4Tm3M6ev8"
TABLE = "popular_videos"

@dag(
   dag_id="youtube_popular_videos",
   schedule=None,
   catchup=False    
)
def youtube_videos_taskflow_api():
    @task()
    def extract_video_data():
        try:
            youtube = build('youtube', 'v3', developerKey=API_KEY)
            request = youtube.videos().list(part='snippet,statistics', chart="mostPopular", maxResults=25)
            response = request.execute()
        except Exception as e:
            logging.error(e)
        else:
            try:
                data = response["items"]
                if not data:
                    logging.debug("videos list is empty")
                else:    
                    return data
                logging.info("data sent for transformation")
            except Exception as e:
                logging.error(e)

    @task()
    def transform_video_data(data):
        filtered_items=[]
        try:
            for i in data:
                collected = {}
                collected["publishedAt"]=i["snippet"]["publishedAt"]
                collected["channelId"]=i["snippet"]["channelId"]
                collected["channelName"]=i["snippet"]["title"]
                collected["viewCount"]=i["statistics"]["viewCount"]
                collected["likeCount"]=i["statistics"]["likeCount"]
                filtered_items.append(collected)
            logging.info("data sent for loading")
            return filtered_items
        except Exception as e:
            logging.error(e)

    @task()
    def load_video_data(data):

        try:
            # fill in ur own data
            engine = sa.create_engine("mysql+mysqldb://")
            logging.info("Database connected")
            refined_data = pd.DataFrame(data)        
            refined_data.to_sql(TABLE, engine, if_exists='replace')
            logging.info("Data dumped into the table")
        except Exception as e:
            logging.error(e)
    
    video_data = extract_video_data()
    filtered_videos = transform_video_data(video_data)
    load_video_data(filtered_videos)

youtube_videos_taskflow_api()
