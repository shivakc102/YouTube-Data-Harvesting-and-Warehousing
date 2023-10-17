import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
from PIL import Image
icon = Image.open('youtube_data_api.png')
st.set_page_config(page_title='YouTube API | Store | Migrate | Query ' ,page_icon=icon)
# function to get channel details
def get_channel_stats(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
               part='snippet,statistics,contentDetails',
               id=channel_ids)
    response = request.execute()
    for i in range(len(response['items'])):
        data = dict(  channel_id = response['items'][i]['id'],
                      channel_name = response['items'][i]['snippet']['title'],
                      channel_description = response['items'][i]['snippet']['description'],
                      channel_viewCount = response['items'][i]['statistics']['viewCount'],
                      channel_subscriberCount = response['items'][i]['statistics']['subscriberCount'],
                      Total_videos = response['items'][i]['statistics']['videoCount'],
                      playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
    return all_data
# function to get playlist id
def get_playlist_id(channel_detail):
    channal_details_pd = pd.DataFrame(channel_detail)
    playlist_Id = channal_details_pd['playlist_id'][0]
    return playlist_Id
# function to get video ids
def get_video_ids(youtube ,playlist_Id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_Id
    )
    response = request.execute()
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    return video_ids
# function to get video details
def get_video_details(youtube, video_ids):
    all_data = []
    request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids)
    response = request.execute()
    for i in range(len(response['items'])):
        data = dict(channel_id=response['items'][i]['snippet']['channelId'],
                    video_id=response['items'][i]['id'],
                    video_name=response['items'][i]['snippet']['title'],
                    video_description=response['items'][i]['snippet']['description'],
                    published_at=response['items'][i]['snippet']['publishedAt'],
                    view_count=response['items'][i]['statistics']['viewCount'],
                    like_count=response['items'][i]['statistics']['likeCount'],
                    favorite_count=response['items'][i]['statistics']['favoriteCount'],
                    comment_count=response['items'][i]['statistics']['commentCount'],
                    duration=response['items'][i]['contentDetails']['duration'])
        all_data.append(data)
    return all_data
# function to get comments details
def get_comments(youtube, video_ids):
    all_data = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            maxResults=5,
            videoId=video_id)
        response = request.execute()
        for cmt in response['items']:
            data = dict(video_id = cmt['snippet']['videoId'],
                       Comment_id = cmt['id'],
                       Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                       Commend_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       Comment_publishedAt = cmt['snippet']['topLevelComment']['snippet']['publishedAt'])
            all_data.append(data)
    return all_data
# youtube api key
my_api_key = "AIzaSyD6hwRwZIYJpk6Tn_QUFJpShbjAREgm1DU"
youtube = build('youtube','v3',developerKey=my_api_key)
# import & connect pymongo
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")

st.subheader("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
tab1, tab2, tab3, tab4 = st.tabs([":ice_cube: Data_Collection", ":ice_cube: Retrieve Data & Store MongoDB", ":ice_cube: Migrate data to a MySQL",":ice_cube: Query the SQL data"])
with tab1:
    st.caption('_Data has been retrieve with the help of youtube api and channel_id_')
    st.write("Here are some sample channel names and channel IDs:")
    data = [['codebasics','UCh9nVJoWXmFb7sLApWGcLPQ'],['A2D Channel','UCvyZS6W6zMJCZBVzF-Ei6sw'],['Your Mutual Funds','UC_ILjNy7fSF185BdvNFjGxA'],['Aravind Suriya | Tamil Finance','UCIPYexFm665KvqnUrtk2dpQ'],['Tamil Pokkisham','UCS84kz7Fs8bzRs6xcPY9lQQ'],['Yogam','UCimI17I9cM4ykd39M4xiy_A'],['Finance With Sharan','UCwVEhEzsjLym_u1he4XWFkg'],['APPLEBOX By Sabari','UCCaiUfpgquuFMmO3Sf5AsYQ'],['Cheran Academy','UCBTje3Q6Crs6fbWVoU9lSmw'],['Kaizen English','UC44aT4ek1daiUsw2o1XUxow']]
    df = pd.DataFrame(data, columns=['Channel_Name', 'Channel_Id'], index=range(1, 11))
    st.table(df)
with tab2:
    channel_id = st.text_input( 'Enter The Channel_Id' )
    submit = st.button('CLICK TO RETRIEVE & STORE')
    if submit:
        channel_detail = get_channel_stats(youtube,channel_id)
        playlist_Id = get_playlist_id(channel_detail)
        video_ids = get_video_ids(youtube , playlist_Id)
        video_details = get_video_details(youtube, video_ids)
        video_details_pd = pd.DataFrame(video_details)
        video_details_pd['published_at'] = video_details_pd['published_at'].str.split("-").str[0]
        video_details = video_details_pd.to_dict('records')
        comments_detail = get_comments(youtube, video_ids)
   
        new_db=client["Youtubedata"]

        new_collection1=new_db["channels_details"]
        new_collection2=new_db["video_details"]
        new_collection3=new_db["comments_detail"]

        new_collection1.insert_many(channel_detail)
        new_collection2.insert_many(video_details)
        new_collection3.insert_many(comments_detail)
        st.success("Congratulations on successfully retrieving data with the help of YouTube API and storing it in MongoDB")
        st.caption('check MogoDB')
with tab3:
    st.write("After Collecting data from multiple channel click the below button to migrate it to a MySQL Database")
    sub_mit = st.button("MIGRATE MongoDB DATA TO MySQL")
    from mysql.connector import Error
    if sub_mit:
        def create_server_connection(host_name, user_name, user_password):
            connection = None
            try:
                connection = mysql.connector.connect(
                    host = host_name,
                    user = user_name,
                    passwd = user_password
                )
                print("MySQL Database connection successful")
            except Error as err:
                print(f"Error :'{err}'")
            return connection
        # MySQL terminal password
        pw = "Kcshiva@96"
        # Database name
        db = "Youtube_data"
        connection = create_server_connection("localhost","root",pw)
        # create Youtube_data
        def create_database(connection, query):
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                print("Database create successfully")
            except Error as err:
                print(f"Error:'{err}'")
        create_database_query = "Create database Youtube_data"
        create_database(connection, create_database_query)
        # connect to batabase
        def create_db_connection(host_name, user_name, user_password, db_name):
            connection = None
            try:
                connection = mysql.connector.connect(
                        host = host_name,
                        user = user_name,
                        passwd = user_password,
                        database = db_name)
                print("MySQL datase connection successful")
            except Error as err:
                print(f"Error:'{err}'")
            return connection
        # Execute sql queries
        def execute_query(connection, query):
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                connection.commit()
                print("Query was succeddful")
            except Error as err:
                print(f"Error:'{err}'")
        create_channel_table = """ create table channel(
                channel_id VARCHAR(255) primary key,channel_name VARCHAR(255) ,channel_description TEXT ,
                channel_viewCount INT ,channel_subscriberCount INT,Total_videos INT,playlist_id VARCHAR(255));"""
        create_video_table = """ create table video(
                video_id VARCHAR(255) primary key ,channel_id VARCHAR(255),video_name VARCHAR(255),
                video_description TEXT,published_at INT,view_count INT,
                like_count INT,favorite_count INT,comment_count INT,duration VARCHAR(20),
                FOREIGN KEY (channel_id) REFERENCES channel(channel_id));"""
        create_comments_table = """ create table comments(
                video_id VARCHAR(255),Comment_id VARCHAR(255) PRIMARY KEY,Comment_text TEXT,
                Commend_author VARCHAR(255),Comment_publishedAt VARCHAR(255),
                FOREIGN KEY (video_id) REFERENCES video(video_id));"""
        # connect to the database
        connection = create_db_connection("localhost","root",pw,db)
        execute_query(connection,create_channel_table)
        execute_query(connection,create_video_table)
        execute_query(connection,create_comments_table)
        # create a cursor object for MySQL
        cursor_mysql = connection.cursor(buffered=True)
        new_db=client["Youtubedata"]
        new_collection11=new_db["channels_details"]
        for document in new_collection11.find():
            document.pop('_id', None)
            query = "INSERT INTO channel (channel_id,channel_name, channel_description, channel_viewCount, channel_subscriberCount, Total_videos, playlist_id) VALUES (%s, %s, %s, %s ,%s ,%s, %s)"
            values = tuple(document.get(key) for key in [ 'channel_id', 'channel_name','channel_description', 'channel_viewCount', 'channel_subscriberCount', 'Total_videos', 'playlist_id'])
            cursor_mysql.execute(query, values)
        connection.commit()
        new_collection22=new_db["video_details"]
        for document in new_collection22.find():
            document.pop('_id', None)
            query = "INSERT INTO video (channel_id, video_id, video_name,video_description,published_at,view_count,like_count,favorite_count,comment_count,duration) VALUES (%s, %s ,%s ,%s, %s, %s, %s, %s ,%s ,%s)"
            values = tuple(document.get(key) for key in ['channel_id','video_id','video_name','video_description','published_at','view_count','like_count','favorite_count','comment_count','duration'])
            cursor_mysql.execute(query, values)
        connection.commit()
        new_collection33=new_db["comments_detail"]
        for document in new_collection33.find():
            document.pop('_id', None)
            query = "INSERT INTO comments (video_id, Comment_id, Comment_text, Commend_author, Comment_publishedAt) VALUES (%s, %s, %s, %s, %s)"
            values = tuple(document.get(key) for key in ['video_id','Comment_id','Comment_text','Commend_author','Comment_publishedAt'])
            cursor_mysql.execute(query, values)
        connection.commit()
        st.success("Congratulations on successfully migrate data to MySQL")
with tab4:
    sub_mit = st.checkbox("Click to start")
    if sub_mit:
        st.write("##### Query the SQL data warehouse #####")
        INPUT = st.selectbox('select SQL querie qustion for visualization ',
        ['What are the names of all the videos and their corresponding channels?',
        'Which channels have the most number of videos, and how many videos do they have?',
        'What are the top 10 most viewed videos and their respective channels?',
        'How many comments were made on each video, and what are their corresponding video names?',
        'Which videos have the highest number of likes, and what are their corresponding channel names?',
        'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        'What is the total number of views for each channel, and what are their corresponding channel names?',
        'What are the names of all the channels that have published videos in the year 2022?',
        'Which videos have the highest number of comments, and what are their corresponding channel names?'])
        # connect to the database
        connection = mysql.connector.connect( 
                        host = "localhost",
                        user = "root",
                        passwd = "Kcshiva@96",
                        database = "Youtube_data")
        # create a cursor object for MySQL
        cursor_mysql = connection.cursor(buffered=True)
        if INPUT == 'What are the names of all the videos and their corresponding channels?':
            sql_query = """SELECT channel_name , video_name 
                        FROM youtube_data.channel c
                        JOIN youtube_data.video v
                        USING (channel_id);"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
        elif INPUT == 'Which channels have the most number of videos, and how many videos do they have?':
            sql_query = """SELECT channel_name, Total_videos
                            FROM youtube_data.channel
                            WHERE Total_videos = (SELECT MAX(Total_videos) FROM youtube_data.channel);"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            sql_query1 = "SELECT channel_name , Total_videos FROM youtube_data.channel"
            cursor_mysql.execute(sql_query1)
            df2 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            df2 = df2.set_index('channel_name')
            st.bar_chart(df2,use_container_width=True)
        
        elif INPUT == 'What are the top 10 most viewed videos and their respective channels?':
            sql_query = """SELECT channel_name , video_name , view_count
                            FROM youtube_data.channel c
                            RIGHT JOIN youtube_data.video v 
                            USING (channel_id)
                            ORDER BY view_count DESC LIMIT 10"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            st.bar_chart(df1,x='video_name',y='view_count')
        elif INPUT == 'How many comments were made on each video, and what are their corresponding video names?':
            sql_query = "SELECT video_name , comment_count FROM youtube_data.video;"
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            df1 = df1.set_index('video_name')
            st.bar_chart(df1,use_container_width=True)
        elif INPUT == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
            sql_query = """SELECT channel_name , video_name ,like_count
                            FROM youtube_data.channel c
                            JOIN youtube_data.video v
                            USING (channel_id) ORDER BY like_count DESC LIMIT 10"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            st.bar_chart(df1,x='video_name',y='like_count')
        elif INPUT == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            sql_query = "SELECT video_name , like_count FROM youtube_data.video;"
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            df1 = df1.set_index('video_name')
            st.bar_chart(df1,use_container_width=True)
        elif INPUT == 'What is the total number of views for each channel, and what are their corresponding channel names?':
            sql_query = "SELECT channel_name, channel_viewCount  FROM youtube_data.channel;"
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            df1 = df1.set_index('channel_name')
            st.bar_chart(df1,use_container_width=True)
        elif INPUT == 'What are the names of all the channels that have published videos in the year 2022?':
            sql_query = """SELECT channel_name, COUNT(*) AS total_videos
                            FROM youtube_data.channel c 
                            JOIN youtube_data.video v ON c.channel_id = v.channel_id
                            WHERE published_at LIKE '2023'
                            GROUP BY channel_name;"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            df1 = df1.set_index('channel_name')
            st.bar_chart(df1,use_container_width=True)
        elif INPUT == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
            sql_query = """SELECT channel_name , video_name , comment_count
                            FROM youtube_data.channel 
                            JOIN youtube_data.video 
                            USING (channel_id)
                            ORDER BY comment_count DESC LIMIT 10"""
            cursor_mysql.execute(sql_query)
            df1 = pd.DataFrame(cursor_mysql.fetchall(),columns=cursor_mysql.column_names)  
            st.dataframe(df1)
            st.subheader(":bar_chart: Data visualization")
            st.bar_chart(df1,x='video_name',y='comment_count')
       