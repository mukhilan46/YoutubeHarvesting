import pandas as pd
import plotly.express as px
import streamlit as st
import pymongo
import mysql.connector as sql
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
icon = Image.open("shadow_technology.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded")

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="0518")

mycursor = mydb.cursor(buffered=True)

# CREATE MYSQL DATABASE IF NOT EXISTS
mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data_warehouse")
mycursor.execute("USE youtube_data_warehouse")

# CREATE MYSQL TABLES
mycursor.execute("""
    CREATE TABLE IF NOT EXISTS channels (
        Channel_id VARCHAR(255),
        Channel_name VARCHAR(500),  -- Increase the length to 500 characters
        Playlist_id VARCHAR(255),
        Subscribers INT,
        Views INT,
        Total_videos INT,
        Description TEXT,
        Country VARCHAR(255)
    )
""")

mycursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        Channel_name VARCHAR(255),
        Channel_id VARCHAR(255),
        Video_id VARCHAR(255),
        Title VARCHAR(255),
        Tags TEXT,
        Thumbnail TEXT,
        Description TEXT,
        Published_date DATETIME,
        Duration TIME,
        Views INT,
        Likes INT,
        Comments INT,
        Favorite_count INT,
        Definition VARCHAR(255),
        Caption_status VARCHAR(255)
    )
""")

mycursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        Comment_id VARCHAR(255),
        Video_id VARCHAR(255),
        Comment_text TEXT,
        Comment_author VARCHAR(255),
        Comment_posted_date DATETIME,
        Like_count INT,
        Reply_count INT
    )
""")

# BRIDGING A CONNECTION WITH MONGODB ATLAS AND CREATING A NEW DATABASE (YOUTUBE_DATA)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.youtube_data

def migrate_data():
    try:
        # Migrate channel data
        mycursor.execute("DELETE FROM channels")
        # Inside the migrate_data() function
        for ch_data in db.channel_details.find():
            query = """INSERT INTO channels (Channel_id, Channel_name, Playlist_id, Subscribers, Views, Total_videos, Description, Country) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (
                ch_data["Channel_id"], 
                ch_data["Channel_name"],  
                ch_data["Playlist_id"], 
                ch_data["Subscribers"],
                ch_data["Views"], 
                ch_data["Total_videos"], 
                ch_data["Description"],  
                ch_data["Country"]
            )
            print("Executing query:", query)
            print("Values:", values)
            mycursor.execute(query, values)


        # Migrate video data
        mycursor.execute("DELETE FROM videos")
        for vid_data in db.video_details.find():
            query = """INSERT INTO videos (Channel_name, Channel_id, Video_id, Title, Tags, Thumbnail, Description, 
                       Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (
                vid_data["Channel_name"], 
                vid_data["Channel_id"], 
                vid_data["Video_id"], 
                vid_data["Title"], 
                vid_data["Tags"],
                vid_data["Thumbnail"], 
                vid_data["Description"], 
                vid_data["Published_date"], 
                vid_data["Duration"],
                vid_data["Views"], 
                vid_data["Likes"], 
                vid_data["Comments"], 
                vid_data["Favorite_count"],
                vid_data["Definition"], 
                vid_data["Caption_status"]
            )
            mycursor.execute(query, values)

        # Migrate comment data
        mycursor.execute("DELETE FROM comments")
        for com_data in db.comments_details.find():
            query = """INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, 
                       Like_count, Reply_count) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            values = (
                com_data["Comment_id"], 
                com_data["Video_id"], 
                com_data["Comment_text"],  
                com_data["Comment_author"],
                com_data["Comment_posted_date"], 
                com_data["Like_count"], 
                com_data["Reply_count"]
            )
            mycursor.execute(query, values)

        # Commit changes
        mydb.commit()
        print("Data migration successful.")

    except Exception as e:
        print("Error migrating data:", e)

# Call migrate_data function
migrate_data()

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyCKVVUv-f4U3ZQr3ROi42_bGwzhlA979ms"
youtube = build('youtube', 'v3', developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', maxResults=50, pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(part="snippet,contentDetails,statistics", id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies", videoId=v_id, maxResults=100, pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

# FUNCTION TO DISPLAY HOME PAGE CONTENT
def display_home_page():
    st.image("shadow_technology.png")
    st.write("""
        ## :blue[Domain] : Social Media
        ## :blue[Technologies used] : Python, MongoDB, Youtube Data API, MySQL, Streamlit
        ## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.
    """)
    st.image("shadow_technology.png")

# FUNCTION TO DISPLAY EXTRACT & TRANSFORM PAGE CONTENT
def display_extract_transform_page():
    st.write("### Enter YouTube Channel ID below:")
    channel_id = st.text_input("Hint: Go to the channel's homepage > Right-click > View page source > Find channel_id").split(',')
    
    if st.button("Extract Data"):
        ch_details = get_channel_details(channel_id)
        st.write(f'#### Extracted data from: {ch_details[0]["Channel_name"]} channel')
        st.table(ch_details)

    if st.button("Upload to MongoDB"):
        with st.spinner('Uploading to MongoDB. Please wait...'):
            ch_details = get_channel_details(channel_id)
            video_ids = get_channel_videos(channel_id)
            vid_details = get_video_details(video_ids)
            
            def comments():
                com_d = []
                for i in video_ids:
                    com_d += get_comments_details(i)
                return com_d
            
            comm_details = comments()
            
            collections1 = db.channel_details
            collections1.insert_many(ch_details)

            collections2 = db.video_details
            collections2.insert_many(vid_details)

            collections3 = db.comments_details
            collections3.insert_many(comm_details)
            
            st.success("Uploaded to MongoDB successfully!")

# FUNCTION TO DISPLAY VIEW PAGE CONTENT
def display_view_page():
    st.write("## :orange[Select any question to get insights]")
    questions = ['1. What are the names of all the videos and their corresponding channels?',
                 '2. Which channels have the most number of videos, and how many videos do they have?',
                 '3. What are the top 10 most viewed videos and their respective channels?',
                 '4. How many comments were made on each video, and what are their corresponding video names?',
                 '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                 '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                 '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                 '8. What are the names of all the channels that have published videos in the year 2022?',
                 '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                 '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
    selected_question = st.selectbox("Questions", questions)

    if selected_question == questions[0]:
        mycursor.execute("SELECT Title AS Video_Title, Channel_name AS Channel_Name FROM videos")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif selected_question == questions[1]:
        mycursor.execute("SELECT Channel_name AS Channel_Name, Total_videos AS Total_Videos FROM channels ORDER BY Total_videos DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        fig = px.bar(df, x="Channel_Name", y="Total_Videos", orientation='v', color="Channel_Name")
        st.plotly_chart(fig, use_container_width=True)

    elif selected_question == questions[2]:
        mycursor.execute("""
            SELECT Channel_name AS Channel_Name, Title AS Video_Title, Views 
            FROM videos
            ORDER BY views DESC
            LIMIT 10
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        fig = px.bar(df, x="Views", y="Video_Title", orientation='h', color="Channel_Name")
        st.plotly_chart(fig, use_container_width=True)

    elif selected_question == questions[3]:
        mycursor.execute("""
            SELECT v.Title AS Video_Title, COUNT(c.Comment_id) AS Comment_Count
            FROM videos v
            LEFT JOIN comments c ON v.Video_id = c.Video_id
            GROUP BY v.Title
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        fig = px.bar(df, x="Comment_Count", y="Video_Title", orientation='h')
        st.plotly_chart(fig, use_container_width=True)

    elif selected_question == questions[4]:
        mycursor.execute("""
            SELECT v.Title AS Video_Title, v.Likes AS Like_Count, v.Views AS View_Count, v.Channel_name AS Channel_Name
            FROM videos v
            WHERE v.Likes = (SELECT MAX(Likes) FROM videos)
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif selected_question == questions[5]:
        mycursor.execute("""
            SELECT v.Title AS Video_Title, v.Likes AS Like_Count, v.Views AS View_Count
            FROM videos v
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif selected_question == questions[6]:
        mycursor.execute("""
            SELECT c.Channel_name AS Channel_Name, SUM(v.Views) AS Total_Views
            FROM videos v
            JOIN channels c ON v.Channel_id = c.Channel_id
            GROUP BY c.Channel_name
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        fig = px.bar(df, x="Total_Views", y="Channel_Name", orientation='h', color="Channel_Name")
        st.plotly_chart(fig, use_container_width=True)

    elif selected_question == questions[7]:
        mycursor.execute("""
            SELECT Channel_name AS Channel_Name
            FROM videos
            WHERE YEAR(Published_date) = 2022
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif selected_question == questions[8]:
        mycursor.execute("""
            SELECT Channel_name AS Channel_Name, AVG(Duration) AS Average_Duration
            FROM videos
            GROUP BY Channel_name
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        fig = px.bar(df, x="Average_Duration", y="Channel_Name", orientation='h', color="Channel_Name")
        st.plotly_chart(fig, use_container_width=True)

    elif selected_question == questions[9]:
        mycursor.execute("""
            SELECT v.Title AS Video_Title, COUNT(c.Comment_id) AS Comment_Count
            FROM videos v
            LEFT JOIN comments c ON v.Video_id = c.Video_id
            GROUP BY v.Title
            ORDER BY Comment_Count DESC
        """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

# FUNCTION TO DISPLAY THE SELECTED PAGE
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", options=["Home", "Extract & Transform", "View"])

    if page == "Home":
        display_home_page()
    elif page == "Extract & Transform":
        display_extract_transform_page()
    elif page == "View":
        migrate_data()  # Automatically migrate data before displaying the view page
        display_view_page()

if __name__ == "__main__":
    main()
