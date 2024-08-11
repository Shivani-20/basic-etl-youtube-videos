import os
from dotenv import load_dotenv 
import pandas as pd
from googleapiclient.discovery import build
import sqlalchemy as sa


def extract_video_data():
    load_dotenv()
    key = os.getenv("API_KEY")
    try:
        youtube = build('youtube', 'v3', developerKey=key)
        request = youtube.videos().list(part='snippet,statistics', chart="mostPopular", maxResults=50)
        response = request.execute()
    except Exception as e:
        print(e)
    else:
        try:
            data = response["items"]
            transform_channel_data(data)
        except Exception as e:
            print(e)
       

def transform_video_data(data):
    filtered_items=[]
    for i in data:
        collected = {}
        collected["publishedAt"]=i["snippet"]["publishedAt"]
        collected["channelId"]=i["snippet"]["channelId"]
        collected["channelName"]=i["snippet"]["title"]
        collected["viewCount"]=i["statistics"]["viewCount"]
        collected["likeCount"]=i["statistics"]["likeCount"]
        filtered_items.append(collected)
    load_channel_data(filtered_items)


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
    except Exception as e:
        print(e)
   
    refined_data = pd.DataFrame(data)        
    refined_data.to_sql("popular_videos", engine, if_exists='replace')

extract_channel_data()
