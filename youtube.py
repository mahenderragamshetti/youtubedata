from googleapiclient.discovery import build
import pandas as pd
import pymongo
import psycopg2
import streamlit as st
API_KEY = 'AIzaSyBbgJOkCFi4HYsbwGvdSZqBl0ZJGrjBm3k'
youtube = build('youtube', 'v3', developerKey=API_KEY)
def get_channel_stats(channel_id):
     request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
     )
     response = request.execute()
    
     for i in response['items']:
         data=dict(Channel_Name= i["snippet"]["title"],
                   Channel_Id=i["id"],
                   Subscribers=i['statistics']['subscriberCount'],
                   Views=i["statistics"]["viewCount"],
                   Total_Videos=i["statistics"]["videoCount"],
                   Channel_Description=i["snippet"]["description"],
                   Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
     return data      
channel_details=get_channel_stats("UCtpSwjGjZ8EIb53iQes3d1Q")

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='content_details'
    ).execute()

    if 'items' in response and response['items']:
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None

        while True:
            playlist_items = youtube.playlistItems().list(
                playlistId=playlist_id,
                part='contentDetails',
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in playlist_items['items']:
                video_ids.append(item['contentDetails']['videoId'])

            next_page_token = playlist_items.get('nextPageToken')

            if not next_page_token:
                break

    return video_ids
video_ids=get_video_ids('UCtpSwjGjZ8EIb53iQes3d1Q')

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            # Extract the URL of the default thumbnail
            default_thumbnail_url = item['snippet']['thumbnails']['default']['url']

            # Extract the favorite count
            favorite_count = item['statistics'].get('favoriteCount', None)

            # Extract the description or set a default value (empty string)
            description = item.get('snippet', {}).get('description', '')

            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=default_thumbnail_url,  # Use the extracted URL
                Description=description,
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount', None),
                Likes=item['statistics'].get('likeCount',None),
                Comments=item['statistics'].get('commentCount', None),
                Favorite_Count=favorite_count,
                Definition=item['contentDetails'].get('definition', None),
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data
video_details=get_video_info(video_ids)

def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data=dict(Comment_Id= item['snippet']['topLevelComment']['id'],
                          Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author= item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
                
    except:
        pass
    return comment_data
comment_details=get_comment_info(video_ids)
def get_playlist_details(channel_id):
    next_page_token=None
    all_data=[]
    while True:
        request=youtube.playlists().list(
               part='snippet,contentDetails',
               channelId=channel_id,
               maxResults=50,
               pageToken=next_page_token
        )
        response=request.execute()
        for item in response['items']:
            data=dict(playlist_Id=item['id'],
                     Title=item['snippet']['title'],
                     Channel_Id=item['snippet']['channelId'],
                     Channel_Name=item['snippet']['channelTitle'],
                     PublishedAt=item['snippet']['publishedAt'],
                     Video_Count=item['contentDetails']['itemCount'])
            all_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data

playlist_details=get_playlist_details('UCtpSwjGjZ8EIb53iQes3d1Q')

client=pymongo.MongoClient("mongodb+srv://ma143mahi:ma143mahi@cluster0.leyr2.mongodb.net/?retryWrites=true&w=majority")
db=client["ragamshetti"]

def channel_details(channel_id):
    ch_details=get_channel_stats(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    colll=db["channel_details"]
    colll.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

insert=channel_details('UCtpSwjGjZ8EIb53iQes3d1Q')

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="ma143mahi@",
    database="msd",
    port="5432"
)

cursor = mydb.cursor()
drop_query = '''DROP TABLE IF EXISTS channels'''
cursor.execute(drop_query)
mydb.commit()
create_query = '''CREATE TABLE IF NOT EXISTS channels (Channel_Name VARCHAR(100),
                                                       Channel_Id VARCHAR(80) PRIMARY KEY,
                                                       Subscribers BIGINT,
                                                       Views BIGINT,
                                                       Total_videos INT,
                                                       Channel_Description TEXT,
                                                       Playlist_id VARCHAR(80)
                                                       )'''
cursor.execute(create_query)
mydb.commit()

ch_list = []
db=client["ragamshetti"]
colll = db["channel_details"]
for ch_data in colll.find({}, {"_id": 0, "channel_information": 1}):
   
        ch_list.append(ch_data["channel_information"])
df=pd.DataFrame(ch_list)
df_no_duplicates = df.drop_duplicates(subset=["Channel_Id"])
def channels_table():
    for index, row in df_no_duplicates.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                           Channel_Id,
                                           Subscribers,
                                           Views,
                                           Total_Videos,
                                           Channel_Description,
                                           Playlist_Id
                                             )
                                             VALUES (%s,%s,%s,%s,%s,%s,%s)'''
    
        values = (row['Channel_Name'],
              row['Channel_Id'],
              row['Subscribers'],
              row['Views'],
              row['Total_Videos'],
              row['Channel_Description'],
              row['Playlist_Id'])

        cursor.execute(insert_query,values)
    
        mydb.commit()
channels_table()

    
drop_query = '''DROP TABLE IF EXISTS playlists'''
cursor.execute(drop_query)
mydb.commit()
create_query = '''CREATE TABLE IF NOT EXISTS playlists (playlist_Id varchar(100),
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''
cursor.execute(create_query)
mydb.commit()
pl_list=[]
db=client["dhoni"]
colll = db["channel_details"]
for pl_data in colll.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
df1=pd.DataFrame(pl_list)
df6=df1.drop_duplicates()

def playlist_table():
    mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="ma143mahi@",
    database="msd"
    )

    cursor = mydb.cursor()

    for index, row in df6.iterrows():
        insert_query = '''insert into playlists (playlist_Id,
                                             Title,
                                             Channel_Name,
                                             Channel_Id,
                                             PublishedAt,
                                             Video_Count
                                             )
                                             VALUES (%s, %s, %s,%s,%s,%s)'''
    
   
        values = (row['playlist_Id'],
              row['Title'],
              row['Channel_Name'],
              row['Channel_Id'],
              row['PublishedAt'],
              row['Video_Count'])
        cursor.execute(insert_query,values)
        mydb.commit()
playlist_table()

drop_query = '''drop table if exists videos'''
cursor.execute(drop_query)
mydb.commit()
create_query='''create table if not exists videos(Channel_Name varchar(100),
                                    Channel_Id varchar(100),
                                    Video_Id varchar(30),
                                    Title varchar(150),
                                    Tags text,
                                    Thumbnail varchar(200),
                                    Description text,
                                    Published_Date timestamp,
                                    Duration interval,
                                    Views bigint,
                                    Likes bigint,
                                    Comments int,
                                    Favorite_Count int,
                                    Definition varchar(10),
                                    Caption_Status varchar(50))'''
cursor.execute(create_query)
mydb.commit()
vi_list=[]
db=client["dhoni"]
colll = db["channel_details"]
for vi_data in colll.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
df2=pd.DataFrame(vi_list)

df2['Tags'] = df2['Tags'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
df9 = df2.drop_duplicates()

def videos_table():
      mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="ma143mahi@",
                        database="msd"
                         )

      cursor = mydb.cursor()

      for index,row in df9.iterrows():
          insert_query='''insert into videos(Channel_Name,
                                        Channel_id,
                                        Video_Id,
                                        Title,
                                        Tags,
                                        Thumbnail,
                                        Description,
                                        Published_Date,
                                        Duration,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favorite_Count,
                                        Definition,
                                        Caption_Status
                                        )
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
      values=(row['Channel_Name'],
              row['Channel_Id'],
              row['Video_Id'],
              row['Title'],
              row['Tags'],
              row['Thumbnail'],
              row['Description'],
              row['Published_Date'],
              row['Duration'],
              row['Views'],
              row['Likes'],
              row['Comments'],
              row['Favorite_Count'],
              row['Definition'],
              row['Caption_Status'])
      cursor.execute(insert_query,values)
      mydb.commit()
videos_table()

drop_query = '''drop table if exists comments'''
cursor.execute(drop_query)
mydb.commit()
create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(50),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_PublishedAt timestamp)'''
                                                     
cursor.execute(create_query)
mydb.commit()

com_list=[]
db=client["dhoni"]
colll = db["channel_details"]
for com_data in colll.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
df4=pd.DataFrame(com_list)
df8=df4.drop_duplicates()
def comments_table():
     
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="ma143mahi@",
                            database="msd"
                           )

    cursor = mydb.cursor()

    for index, row in df8.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                               Video_Id,
                                               Comment_Text,
                                               Comment_Author,
                                               Comment_PublishedAt 
                                               )
                                               values(%s, %s, %s, %s, %s)'''
        values = (row['Comment_Id'],
                 row['Video_Id'],
                 row['Comment_Text'],
                 row['Comment_Author'],
                 row['Comment_PublishedAt'])

        cursor.execute(insert_query, values)
        mydb.commit()
comments_table()
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def show_channels_table():     
    ch_list = []
    db=client["ragamshetti"]
    colll = db["channel_details"]
    for ch_data in colll.find({}, {"_id": 0, "channel_information": 1}):
           ch_list.append(ch_data["channel_information"])
    st1=st.dataframe(ch_list)
    return st1
def show_playlist_table():
    pl_list=[]
    db=client["ragamshetti"]
    colll = db["channel_details"]
    for pl_data in colll.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    st2=st.dataframe(pl_list)
    return st2
def show_videos_table():
    vi_list=[]
    db=client["ragamshetti"]
    colll = db["channel_details"]
    for vi_data in colll.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    st3=st.dataframe(vi_list)
    return st3
def show_comments_table():
    com_list=[]
    db=client["ragamshetti"]
    colll = db["channel_details"]
    for com_data in colll.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    st4=st.dataframe(com_list)
    
    return st4

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDb")
    st.caption("Api Integration")
    st.caption("Data Management using MongoDb and SQL")
channel_id=st.text_input("Enter the channel id")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["ragamshetti"]
    colll = db["channel_details"]
    for ch_data in colll.find({},{"_id":0,"Channel_information":1}):
        ch_ids.append(ch_data["Channel_information"]["channel_id"])
    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to SQL"):
    Table = tables
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","PLAYLISTS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()
mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="ma143mahi@",
                        database="msd"
                         )

cursor = mydb.cursor()
question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. Channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each video",
                                              "5. videos with highest like",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))


mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="ma143mahi@",
                        database="msd"
                         )
cursor = mydb.cursor()
if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))