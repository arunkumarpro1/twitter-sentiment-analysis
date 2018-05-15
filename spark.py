from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import ast
from pycorenlp import StanfordCoreNLP
import requests
import json
import time

url = 'http://localhost:9200/{}/tweets'
headers = {'Content-type': 'application/json'}

data = {
    "tweet" : "John",
    "sentiment" : "Doe",
    "timestamp" : 1355563265,
    "geo":{
        "location" : "Illinois, USA",
        "coordinates":{
            "lat":36.518375,
            "lon":-86.05828083
            }
        }
}

#Routine to get sentiment of tweet using Stanford CoreNLP
def get_sentiment(sentence):
    res = nlp.annotate(sentence,properties={'annotators': 'sentiment', 'outputFormat': 'json', 'timeout': 5000,})
    if not res["sentences"]:
        return "NA"
    else:        
        return res["sentences"][0]["sentiment"]

#Routine to send data to elastic search   
def mapper(x):
    words = x.split( ":" )
    sentiment = get_sentiment(words[0])
    if not sentiment == "NA":
        data['tweet'] = words[0]
        data['sentiment'] = sentiment
        data['timestamp'] = int(time.time())
        if not words[1] == "NA":
            place = words[1].split( ";" )
            data['location'] = place[0]
            data['geo']['coordinates']['lat'] = place[1]
            data['geo']['coordinates']['lon'] = place[2]
            response = requests.post(url.format(words[2]), data=json.dumps(data), headers=headers)
    return [words[0],sentiment,words[1]]
    

TCP_IP = 'localhost'
TCP_PORT = 9001

nlp = StanfordCoreNLP('http://localhost:9000')

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

words = dataStream.map(lambda x: mapper(x))
words.saveAsTextFiles("result", "txt")

ssc.start()
ssc.awaitTermination()