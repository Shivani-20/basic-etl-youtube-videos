import os
from dotenv import load_dotenv 
import pandas as pd
from googleapiclient.discovery import build
import sqlalchemy as sa
import logging

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

def extract_video_data():
    load_dotenv()
    key = os.getenv("API_KEY")
    try:
        youtube = build('youtube', 'v3', developerKey=key)
        request = youtube.videos().list(part='snippet,statistics', chart="mostPopular", maxResults=50)
        response = request.execute()
    except Exception as e:
        logging.error(e)
    else:
        try:
            data = response["items"]
            transform_video_data(data)
            logging.info("data sent for transformation")
        except Exception as e:
            logging.error(e)
       

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
        load_video_data(filtered_items)
    except Exception as e:
        logging.error(e)


def load_video_data(data):

    try:
        url_object = sa.URL.create(
        drivername = "mysql+pymysql",
        host  = os.getenv("HOST"),
        username = os.getenv("USER"),
        password = os.getenv("PASSWD"),
        database = os.getenv("DATABASE")
    )
        engine = sa.create_engine(url_object)
        logging.info("Database connected")
    except Exception as e:
        logging.error(e)
   
    refined_data = pd.DataFrame(data)        
    refined_data.to_sql(os.getenv("TABLE"), engine, if_exists='replace')
    logging.info("Data dumped into the table")

extract_video_data()
