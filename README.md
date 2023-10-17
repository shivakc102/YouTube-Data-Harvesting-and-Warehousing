
# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

##  PROJECT DESCRIPTION
This project retrieves data from the YouTube API with the help of an API key and channel ID. The collected data is stored in MongoDB and later migrated to the MySQL data warehouse. We can execute SQL queries against the database and retrieve data from it. All of these processes are done with Python code, but the results are displayed using the Streamlit framework, which provides a simple way to interact with the web application for this project.

This project is mainly useful for analyzing or comparing YouTube channels with the help of their API key and channel ID. It can also store data and records of YouTube channel video comments, among other things


## Tools installed
- VScode(source-code editor )
- mysql
- MongoDB 
- youtube api key
- python
## Import libraries 
* import streamlit as st
* import pandas as pd
* from googleapiclient.discovery import build
* import mysql.connector
* from mysql.connector import Error
* from PIL import Image

## Workflow in this project
- Create a project in the Google Developer Console. Enable the YouTube Data API V3 and generate an API key (this key helps to retrieve data from YouTube).
- Set up your Streamlight app with the help of Streamlight reference. When you enter a channel ID to the Streamline app, data is retrieved from YouTube. 
- The retrieving code has been given by the YouTube API reference. With the help of this code, you can retrieve after store the data to MongoDB. 
- After collecting channel details and storing them to MongoDB, migrate the data to your SQL database. Before you create a scheme in tabular format, store the data to MySQL. 
- After storing the data to MySQL, we can query the answers using SQL querys 

### Roadmap for this project
- Building a simple UI with Streamlit 
- Retrieving data from the YouTube API 
- Storing it in a MongoDB data lake 
- Migrating it to a MySQL data warehouse 
- Querying the data warehouse with SQL 
- Displaying the data in the Streamlit app.
## User guid

#### step 1 : Data_collection
- Go to the YouTube channel whose ID you want to find. Right-click anywhere on the page and select ‘View Page Source’. Press Ctrl + F to activate the search bar and type ‘channelid’ without quotes. The channel ID will be displayed next to the title as the channel name. Here, I have provided 10 sample channel IDs with channel names for reference

#### step 2 : Retrieve data and store MongoDB
- In the **Enter channel ID** box, paste the YouTube channel ID and click the **Click to Retrieve and Store** button. After clicking, you can see the message ‘successful’ if data is retrieve & stored.

#### step 3 : Migrate data to MySQL
- After collecting data from 10 YouTube channels, you can click the **Migrate MongoDB to MySQL** button. You can see the message ‘Successful’ if data is migrated to MySQL data warehouse.

#### step 4 : Query the SQL data
- Select the checkbox for ‘Click to start’. In the drop-down menu, there are some questions. By selecting one of them, you can see the result in tabular form and data visualization.
## Documentation & References
- [Streamlit Documentation](https://docs.streamlit.io/)
- [YouTube API Reference](https://developers.google.com/youtube/v3/docs)
- [MongoDB Documentation](https://www.mongodb.com/docs/atlas/getting-started/)
- [Python](https://docs.python.org/3/)


## Demo video

[CLICK HERE](https://drive.google.com/file/d/1wGrr-b3_zGd3ualZtgMJIB_3m1QsvXk1/view?usp=drive_link)
[Linkedin Demo video](https://www.linkedin.com/posts/shiva-prasaath-k-c_my-project-is-named-youtube-data-harvesting-activity-7120119143839780864-Q8Y9?utm_source=share&utm_medium=member_desktop)

