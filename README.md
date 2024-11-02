## PREREQUISITES

* Note: my mysql is present in windows but I'm running airflow on WSL so make a connection to MySQL From WSL using necessary client and connector libraries, namely: mysql-client-core-8.0 and apache-airflow-providers-mysql
* change sql_alchemy_conn value in airflow.cfg file:
sql_alchemy_conn = mysql+mysqldb://user:password@ip:3306/database

username, password of database created
ip -Go to Settings -> Network and Internet -> Status -> View Hardware and connection properties. Look for the name vEthernet (WSL)

## GOAL:

* To create an etl dag that fetchs top 50 most popular youtube videos and store it in mysql database.
* To store only published date, channel id, channel name, viewCount, likeCount 

## SOLUTION :

1. Fetch snippet,statistics from google client youtube videos api
2. Extract items part of the response json
3. Transform this data to a list in which each object carries published date, channel id, channel name, viewCount, likeCount 
4. connect to mysql using pymysql driver and sqlalchemy
5. convert the above list to a dataframe
6. dump this data to a table popular_videos

![Screenshot 2024-11-02 214605](https://github.com/user-attachments/assets/19f1c20c-b866-4c50-bb36-eb89fce22db4)

  

