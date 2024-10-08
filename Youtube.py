import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
icon = Image.open("D:\\Project Files\\YT Project & Orientation\\Project\\Youtube_logo.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={'About': """# This app is created by *MUKILAN ELANCHEZHIAN!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home", "Extract & Transform", "View"],
                           icons=["house-door-fill", "tools", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#33A5FF"},
                                   "icon": {"font-size": "30px"},
                                   "container": {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#33A5FF"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb+srv://mukhilan:Boo758595@guvi.cxdcqvo.mongodb.net/youtube_data?retryWrites=true&w=majority")
db = client.Youtube_Data
# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="127.0.0.1",
                   user="root",
                   password="Pegasus_Stark#46",
                   database="Youtube"
                   )
mycursor = mydb.cursor(buffered=True)
mycursor.execute("USE youtube_data_warehouse")

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDQYpHKd36QOoBloxRsemSzSITMepCzoTU"  # "AIzaSyDQYpHKd36QOoBloxRsemSzSITMepCzoTU"
youtube = build('youtube', 'v3', developerKey=api_key)


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

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
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

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
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
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
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
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


# HOME PAGE
if selected == "Home":
    # Title Image
    st.image("D:\\Project Files\\YT Project & Orientation\\Project\\title.png")
    col1, col2 = st.columns(2, gap='medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown(
        "## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    tab1, tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)


                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d


                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")

    # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)


    def insert_into_channels():
        collections = db.channel_details
        query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections.find({"channel_name": user_inp}, {'_id': 0}):
            mycursor.execute(query, tuple(i.values()))
        mydb.commit()


    def insert_into_videos():
        collections1 = db.video_details
        query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections1.find({"channel_name": user_inp}, {'_id': 0}):
            values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in
                      i.values()]
            mycursor.execute(query1, tuple(values))
            mydb.commit()


    def insert_into_comments():
        collections1 = db.video_details
        collections2 = db.comments_details
        query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

        for vid in collections1.find({"channel_name": user_inp}, {'_id': 0}):
            for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
                mycursor.execute(query2, tuple(i.values()))
                mydb.commit()


    if st.button("Submit"):
        try:
            insert_into_videos()
            insert_into_channels()
            insert_into_comments()
            st.success("Transformation to MySQL Successful !!")
        except:
            st.error("Channel details already transformed !!")

# VIEW PAGE
if selected == "View":

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
        mycursor.execute("""
                SELECT Title AS Video_Title, Channel_name AS Channel_Name 
                FROM videos
            """)
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif selected_question == questions[1]:
        mycursor.execute("""
                SELECT Channel_name AS Channel_Name, Total_videos AS Total_Videos 
                FROM channels 
                ORDER BY Total_videos DESC
            """)
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
