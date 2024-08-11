## GOAL:


* To create an etl pipeline that fetchs top 50 most popular youtube videos and store it in mysql database.
* To store only published date, channel id, channel name, viewCount, likeCount 

## SOLUTION :

1. Fetch snippet,statistics from google client youtube videos api
2. Extract items part of the response json
3. Transform this data to a list in which each object carries published date, channel id, channel name, viewCount, likeCount 
4. connect to mysql using pymysql driver and sqlalchemy
5. convert the above list to a dataframe
6. dump this data to a table popular_videos



