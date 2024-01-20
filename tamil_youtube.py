from  googleapiclient.discovery import build
import streamlit as st
import  pandas as pd
from pymongo import MongoClient
import mysql.connector

client=MongoClient('mongodb://localhost:27017/')
youtube_channel_collection=client.get_database('Tamil').get_collection('Youtube_channels')
youtube_video_collection=client.get_database('Tamil').get_collection('Youtube_videos')

config = {
  'user': 'root',
  'password': 'viyan',
  'host': '127.0.0.1',
  'database': 'tamil',
  'raise_on_warnings': True
}

sql_connection = mysql.connector.connect(**config)
cursor = sql_connection.cursor()

youtube=build("youtube", "v3", developerKey='AIzaSyA4CcRXwcP5QAXIhBeSVYD3iWoeqmhRINM')

channelId=None
selectedQuestion=None
st.sidebar.title("Quick view")
query = st.sidebar.checkbox("Query the DB")
questions=["What are the names of all the videos and their corresponding channels?",
           "Which channels have the most number of videos, and how many videos do they have?",
           "What are the top 10 most viewed videos and their respective channels?",
           "How many comments were made on each video, and what are their corresponding video names?",
           "Which videos have the highest number of likes, and what are their corresponding channel names?",
           "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
           "What is the total number of views for each channel, and what are their corresponding channel names?",
           "What are the names of all the channels that have published videos in the year 2022?",    #<
           "What is the average duration of all videos in each channel, and what are their corresponding channel names?", #<
           "Which videos have the highest number of comments, and what are their corresponding channel names?"]  #<
if(query):
    selectedQuestion=st.selectbox("",questions)
else:
    channelId=st.text_input("Enter the Channel Id")





if(channelId):
    channel_request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channelId)
    channel_response = channel_request.execute()
    channel_info={
        "channel_id":channel_response['items'][0]['id'],
        "channel_name":channel_response['items'][0]['snippet']['title'],
        "channel_views":channel_response['items'][0]['statistics']['viewCount'],
        "channel_videoCount" : channel_response['items'][0]['statistics']['videoCount'],
        "channel_description":channel_response['items'][0]['snippet']['description']
    }

    st.dataframe(pd.DataFrame(channel_info,index=[0]))
    playlist_response=youtube.playlistItems().list(part="snippet,contentDetails",playlistId=channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],maxResults=1000).execute()
    videoIds=[]
    for video in playlist_response['items']:
        videoIds.append(video['contentDetails']['videoId'])
        
    # st.write(videoIds)
    video_response=youtube.videos().list(part="snippet,contentDetails,statistics",id=videoIds).execute()

    videos_list=[]

    # st.json(video_response, expanded=False)
    for video in video_response['items']:
        video_info={
            "video_id":video['id'],
            "playlist_id":channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            "video_name":video['snippet']['title'],
            "published_date":video['snippet']['publishedAt'],
            "channel_id":video['snippet']['channelId'],
            "view_count":video['statistics']['viewCount'],
            "like_count":video['statistics']['likeCount'],
            # "dislike_count":video['statistics'][dislikecount],
            "favorite_count":video['statistics']['favoriteCount'],
            "comment_count":video['statistics']['commentCount'],
            "duration":video['contentDetails']['duration'],
            # "thumbnails":video['snippet']['thumbnails']['standard']['url'],
            "caption_status":video['contentDetails']['caption']
            }
        videos_list.append(video_info)

    st.dataframe(videos_list)
    
    if(st.button("Add to DB")):
        existing_channel = youtube_channel_collection.find_one({"channel_id": channel_info["channel_id"]})
        if not existing_channel:
            youtube_channel_collection.insert_one(channel_info)
            youtube_video_collection.insert_many(videos_list)

            channel_collection=youtube_channel_collection.find()
            st.write("This is Channel collection from MongoDB")
            st.dataframe(channel_collection)

            video_collection=youtube_video_collection.find()
            st.write("This is Channel collection from MongoDB")
            st.dataframe(video_collection)

            #cursor.execute()

            channel_dataframe=pd.DataFrame(channel_info,index=[0])
            video_dataframe=pd.DataFrame(videos_list)
            channel_dataframe['_id'] = channel_dataframe['_id'].astype(str)    
            video_dataframe['_id'] = video_dataframe['_id'].astype(str)  

            for _, row in channel_dataframe.iterrows():
                cursor.execute(f"INSERT INTO youtube_channel ({', '.join(channel_dataframe.columns)}) VALUES ({', '.join(['%s']*len(row))})", tuple(row))
            for _, row in video_dataframe.iterrows():
                cursor.execute(f"INSERT INTO youtube_video ({', '.join(video_dataframe.columns)}) VALUES ({', '.join(['%s']*len(row))})", tuple(row))

            # Commit changes and close the cursor
            sql_connection.commit()
            cursor.close()
            #channel_dataframe.to_sql(name='youtube_channel', con=sql_connection, if_exists='append', index=False)
        else:
            st.write(':red[**Channel Already added** ]')
 
if(selectedQuestion):
    question=None
    if selectedQuestion == questions[0]:
        question="SELECT channel_name as Channel, video_name as Video FROM youtube_video join youtube_channel on  youtube_video.channel_id = youtube_channel.channel_id"

    elif selectedQuestion ==questions[1]:
        question="SELECT channel_name, channel_videoCount FROM youtube_channel where channel_videoCount = (select max(channel_videoCount) from youtube_channel)"
    
    elif selectedQuestion ==questions[2]:
        question = "select video_name, view_count ,channel_name from youtube_video join youtube_channel on youtube_video.channel_id=youtube_channel.channel_id order by view_count desc limit 10"

    elif selectedQuestion ==questions[3]:
        question = "SELECT video_name,comment_count FROM tamil.youtube_video"

    elif selectedQuestion ==questions[4]:
       question ="select video_name, like_count ,channel_name from youtube_video join youtube_channel on youtube_video.channel_id=youtube_channel.channel_id order by like_count desc limit 5"
    
    elif selectedQuestion ==questions[5]:
        question ="SELECT video_name,like_count from tamil.youtube_video;"

    elif selectedQuestion ==questions[6]:
        question ="select channel_name,channel_views from youtube_channel"

    elif selectedQuestion ==questions[7]:
        question ="SELECT channel_name,count(video_name) FROM tamil.youtube_channel c join youtube_video v on c.channel_id = v.channel_id where date(replace(replace(published_date,'T',' '),'Z','')) >= '2023-12-24' group by channel_name;"

    elif selectedQuestion ==questions[8]:
        question ="select channel_name, time(sec_to_time(sum( Case when duration like '%H%' then str_to_date(duration, 'PT%HH%iM%sS') when duration like '%M%' then str_to_date(duration, 'PT%iM%sS') when duration like '%S%' then str_to_date(duration, 'PT%sS') end))) as Duration from youtube_video v join youtube_channel c on v.channel_id = c.channel_id group by channel_name;"

    elif selectedQuestion ==questions[9]:
        question ="select video_name, comment_count, channel_name from youtube_video v join youtube_channel c on v.channel_id = c.channel_id where comment_count = (select max(comment_count) from youtube_video)"


    if question:
        cursor.execute(question)
        data=cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)
        # Display the DataFrame in Streamlit
        st.dataframe(df)



    # st.write("This is Channel Response")
    # st.json(channel_response, expanded=False)
    # st.write("This is PlayList Response")
    # st.json(playlist_response, expanded=False)
    # st.write("This is Video Response")
    # st.json(video_response, expanded=False)