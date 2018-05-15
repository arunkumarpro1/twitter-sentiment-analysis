import json 
import tweepy
import socket
import requests
import time
import re

#Routine to get the latitude an longitude coordinates using Google API
def get_location(address):
    finList = "NA"
    if not address is None:
        addP = "address=" + address.replace("\s+ ","+")
        if not addP in location_dict:
            req = requests.get(GEOCODING_API_URL+addP+API_KEY)
            res = req.json()
            if res['status'] == 'OK':
                resu = res['results'][0]
                finList = resu['formatted_address']+";"+str(resu['geometry']['location']['lat'])+";"+str(resu['geometry']['location']['lng'])
            location_dict[addP] = finList
    return finList

#API keys
ACCESS_TOKEN = '180807657-GBdU6v5sKZCQshUuwwi8Q8hURfNqhhtJay2IX793'
ACCESS_SECRET = 'yX5rFnJauMGkdXibwMYFMWI7kuSTZ6j0H4LSE9uYfSvdO'
CONSUMER_KEY = 'KUtpyn73oeS61y41TNdJTO2jk'
CONSUMER_SECRET = 'RIAdJrA7YvaiX66KdbkGGSRNMDXFh9lknMhEyF21PWGhnPGbBk'
GEOCODING_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json?'
API_KEY = '&key=AIzaSyATV2Gp9eNzHJGSw24FKu68bYqumoyiQoA'
location_dict = {}

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

TCP_IP = 'localhost'
TCP_PORT = 9001

tag = ""

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        if not status.retweeted and 'RT @' not in status.text:
            tweet = ' '.join(re.sub("(@[A-Za-z0-9_]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",status.text).split())+":"+get_location(status.user.location)+":"+tag.lower()+"\n"
            print(status.entities)
            conn.send(tweet.encode('utf-8'))
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
while True:
    tag = input("Enter the hashtag")
    if myStream.running is True:
        myStream.disconnect()
    hashtag = '#'+tag
    myStream.filter(track=[hashtag], async=True)