from pymongo import MongoClient
import datetime
import boto.sqs
from boto.sqs.message import Message
import cPickle
from datetime import date
import json
from collections import OrderedDict
import os
import boto
import boto
from boto.s3.key import Key
import re
import ast
import random
import sys

parseddic={}
aggregatedic={}
TERMS = {}
global conn
global q
global client
global db
global keyword
global ACCESS_KEY
global SECRET_KEY
#Setting up consumer function

def setupConsumer():
    global conn
    global q
    global client
    global db
    global keyword
    global ACCESS_KEY
    global SECRET_KEY

    ACCESS_KEY = "<Your AWS access Key>"
    SECRET_KEY ="<Your AWS secret Key>"
	
    keyword = 'Korea' #initialising keyword list

    sent_file = open('AFINN-111.txt') # load sentiments dict
    sent_lines = sent_file.readlines()
    for line in sent_lines:
        # s=line.split('') bug.
        s = line.split('\t')
        TERMS[s[0]] = s[1]
    sent_file.close()

    #connect to SQS queue
    conn= boto.sqs.connect_to_region('us-west-2',aws_access_key_id= ACCESS_KEY,
                                 aws_secret_access_key= SECRET_KEY)

   # q = conn.get_all_queues()
    q = conn.get_queue('Social-Analytic-Queue')
    queuecount = q.count()
    print "Queue count= "+str(queuecount)

    #connect to MongoDB
    client = MongoClient()
    client = MongoClient('localhost',27017)
    db = client['MyApp']

# find sentiment function
def findsentiment(tweet):
    splitTweet = tweet.split()
    sentiment = 0.0
    for word in splitTweet:
        if TERMS.has_key(word):
            sentiment=sentiment+float(TERMS[word])
    return sentiment

    #parse tweet function
def parseTweet(tweet):
    if tweet.has_key('created_at'):
        createdat = tweet['created_at']
        hourint = int(createdat[11:13])
        parseddic['hour'] = str(hourint)

        #Retweets
    parseddic['toptweets'] = {}
    if tweet.has_key('retweeted_status'):
        retweetcount = tweet['retweeted_status']['retweet_count']
        retweetscreenname=tweet['retweeted_status']['user']['screen_name'].encode('utf_8',errors='ignore') #not utf-8 its utf_8
        retweetname = tweet['retweeted_status']['user']['name'].encode('utf-8',errors='ignore')
        retweettext = tweet['retweeted_status']['text'].encode('utf-8',errors='ignore')
        retweetdic={}
        retweetdic['retweetcount']=retweetcount
        retweetdic['retweetscreenname']=retweetscreenname
        retweetdic['retweetname']=retweetname
        retweetdic['retweettext']=retweettext

        retweetdic['retweetsentiment']= findsentiment(retweettext)
        parseddic['toptweets']=retweetdic

            # test, sentiment

    if tweet.has_key('text'):
        text = tweet['text'].encode('utf_8',errors='ignore') # not 'utf-8', its 'utf_8'
        parseddic['text']=text
        sentiment=findsentiment(text)
        parseddic['sentimentscore'] = sentiment
        parseddic['positivesentiment']=0
        parseddic['negativesentiment']=0
        parseddic['neutralsentiment'] = 0

        if sentiment>0:
            parseddic['positivesentiment'] =1
        elif sentiment<0:
            parseddic['negativesentiment'] =1
        elif sentiment==0:
            parseddic['neutralsentiment']=1


            # Hash tags
    if tweet.has_key('entities'):
        res1=tweet['entities']
        taglist = res1['hashtags']
        hashtaglist=[]
        for tagitem in taglist:
            hashtaglist.append(tagitem['text'])

        parseddic['hashtags']=hashtaglist

## Analyze Tweet Function ##
def analyzeTweet(tweetdic):
    text = tweetdic['text']
    text = text.lower()

    if not aggregatedic.has_key(keyword):
        valuedic={'totaltweets':0,'positivesentiment':0,'negativesentiment':0,'neutralsentiment':0,'hashtags':
        {},'toptweets':{},'totalretweets':0,'hourlyaggregate':{'0':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'1':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'2':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'3':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'4':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'5':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'6':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'7':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'8':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'9':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'10':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'11':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'12':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'13':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'14':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'15':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'16':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'17':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'18':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'19':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'20':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'21':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'22':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0},'23':{'totaltweets':0,'positivesentiment':0,'negativesentiment':0,
                                                                    'neutralsentiment':0}}}
        aggregatedic[keyword]=valuedic


    ## counts
    valuedic=aggregatedic[keyword]
    valuedic['totaltweets']+=1
    valuedic['positivesentiment']+=tweetdic['positivesentiment']
    valuedic['negativesentiment']+=tweetdic['negativesentiment']
    valuedic['neutralsentiment']+=tweetdic['neutralsentiment']

	
    ##hourly aggregate
    hour = tweetdic['hour']
    valuedic['hourlyaggregate'][hour]['positivesentiment']+= tweetdic['positivesentiment']
    valuedic['hourlyaggregate'][hour]['negativesentiment']+= tweetdic['negativesentiment']
    valuedic['hourlyaggregate'][hour]['neutralsentiment']+= tweetdic['neutralsentiment']
    valuedic['hourlyaggregate'][hour]['totaltweets']+=1

    ## top hashtags
    tagsdic=valuedic['hashtags']
    for tag in tweetdic['hashtags']:
        if tagsdic.has_key(tag):
            tagsdic[tag]+=1
        else:
            tagsdic[tag]=1

    ## top tweets
    if tweetdic.has_key('toptweets'):
        if tweetdic['toptweets'].has_key('retweetscreenname'):
            toptweetsdic = valuedic['toptweets']
            retweetkey= tweetdic['toptweets']['retweetscreenname']

            if toptweetsdic.has_key(retweetkey):
                toptweetsdic[retweetkey]['retweetcount']= tweetdic['toptweets']['retweetcount']

            else:
                toptweetsdic[retweetkey]=tweetdic['toptweets']

    ## aggregate
    aggregatedic[keyword]=valuedic
## Post processing function
def postProcessing():
    valuedic = aggregatedic[keyword]

# top 10 hashtags
    keysdic = valuedic['hashtags']
    sortedkeysdic = OrderedDict(sorted(keysdic.items(),key =lambda x:x[1],reverse = True))
    tophashtagsdic = {}
    i = 0
    for item in sortedkeysdic:
        if i >9:
            break
        i = i+1
        tophashtagsdic[item] = keysdic[item]
    valuedic['hashtags']= tophashtagsdic

    ## total retweets & top 10 tweets ##
    toptweetsdic = valuedic['toptweets']
    for key in toptweetsdic:
        valuedic['totalretweets'] += toptweetsdic[key]['retweetcount']
    sortednames = sorted(toptweetsdic,key=lambda x:toptweetsdic[x]['retweetcount'],reverse=True)
    sortedtoptweetsdic = OrderedDict()
    i = 0
    for k in sortednames:
        if i > 99:
            break
        i = i+1
        sortedtoptweetsdic[k] =toptweetsdic[k]
    valuedic['toptweets']  = sortedtoptweetsdic
    #print valuedic['toptweets']

##Create key for mongoDB document ###
    valuedic['_id'] =str(date.today())+'/'+keyword
    valuedic['metadata'] = {'date':str(date.today()),'key':keyword}

## insert into MongoDV
    print "inserting data into MongoDB"
    postid = db.myapp_micollection.insert(valuedic)

# uploading to S3
def upload_to_s3(aws_accees_key, aws_secret_access_Key,file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    ''' Uploadings the given file with raw data to the AWS bucket and key specified
    Callback is a function of the form: def callback(complete,total)

  This callback should accept two integer parameters, the first representing the number of bytes that have been
    successfully transmitted to S3 and the second representing the size of the to be transmitted object
    Returns bolleand indicating success / failure of upload
    '''
     ## get the size of a file in bytes

    try:
        fid = file.fileno()
        size = os.fstat(fid).st_size
    except:
        file.seek(0,os.SEEK_END)
        size = file.tell()

    conn2 = boto.connect_s3(aws_accees_key, aws_secret_access_Key)
    bucket = conn2.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)
    file.close()
    if sent == size:
        return True
    return False

##### Main Function: Configure consumerCount ############
def main():

    print "Setting up consumer..."
    setupConsumer()
    print "Completed consumer setup.."

    # for file uploads
    filename= keyword+".txt"
    f = open(keyword+".txt",'w')
    key = f.name
    bucket = "mjlee-bucket"
### enter no.of tweets to consume

    consumeCount = 800
    print "Counsuming " + str(consumeCount) + " feeds.."

    consumeCount = consumeCount/10 ## get 10 in each batch
    for i in range(consumeCount):
        rs = q.get_messages(10) # gets max 10 msgs at a time
        if len(rs) >0:
            for m in rs:
               # post = m.get_body()
                deserializedpost = cPickle.loads(str(m.get_body())) #had to convert to string
                postdic = json.loads(deserializedpost)

                parseTweet(postdic)
                analyzeTweet(parseddic)
                f.write(str(m.get_body()))
            conn.delete_message_batch(q,rs)
    f.close()
    fileToUpload = open(filename,'r+')
    if upload_to_s3(ACCESS_KEY,SECRET_KEY,fileToUpload,bucket,key):
        print "Uploaded to S3!"
        os.remove(filename)
    queuecount = q.count()
    print "Remaining Queue count= " + str(queuecount)
    print "Completed consuming..."
    print "Starting post processing..."
    print postProcessing()
    print "Completed post processing..."
    print "Done!"


#### Entry Point ####
if __name__ == '__main__':
    main()




