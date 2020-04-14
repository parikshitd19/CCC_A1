from splitstream import splitfile
import json
from mpi4py import MPI
import string
import time
from iso639 import languages as langs

start = time.time()

 

#extract hashtags from tweet
def extract_hashtags(tweet):
    hashtags=[]

    #extracting hashtags from tweet
    if len(tweet['doc']['entities']['hashtags'])>0:
        hashtags.extend([hash['text'].lower() for hash in tweet['doc']['entities']['hashtags']])
    
    other_hashtags_keys=["retweeted_status","extended_tweet","quoted_status"]
    #extracting the hashtags from  the original tweet in a retweet,extended tweet, quoted tweet
    for ohk in other_hashtags_keys:
        if ohk in list(tweet["doc"].keys()):
            if "entities" in list(tweet["doc"][ohk].keys()) and  len(tweet["doc"][ohk]["entities"]["hashtags"])>0:
                hashtags.extend([hash['text'].lower() for hash in tweet["doc"][ohk]["entities"]["hashtags"]])

    hashtags=list(set(hashtags))
    return hashtags


#Updating a Dictionary
def add_to_dict(dictionary,element):
    if element in dictionary.keys():
        dictionary[element]+=1
    else:
        dictionary[element]=1
    return dictionary

#Extracting data from a tweet
def tweet_processing(tweet,lang_dict,hashtag_dict):
    if len(tweet['doc']['entities']['hashtags'])>0:
        list_of_hashtags=extract_hashtags(tweet)
        for hashtag in list_of_hashtags:
            hashtag_dict=add_to_dict(hashtag_dict,hashtag)

    if tweet['doc']['lang']==tweet['doc']['metadata']['iso_language_code']:
        lang_dict=add_to_dict(lang_dict,tweet['doc']['lang'])
    
    return lang_dict,hashtag_dict

#Combine 2 dictionaries
def combine_dict(dict1,dict2):
    for key in dict2.keys():
        if key in dict1.keys():
            dict1[key]+=dict2[key]
        else:
            dict1[key]=dict2[key]
    return dict1

#MPI Variables
comm = MPI.COMM_WORLD
comm.Barrier()
size = comm.size
rank = comm.rank

#The file is parsed by a single process as a stream ensuring the whole file is not read into the memory at once
if rank == 0:
    data=splitfile(open("data/bigTwitter.json","r"), format="json", startdepth=2)

#Number of Tweets
count=0
#Dictionary of Languages
final_lang_dict={}
#Dictionary of HashTags
final_hashtag_dict={}
#Tweets Container
chunk=[]
#Package Size
package_size=1
#chunk size
chunk_size=size*package_size

flag=True


received_tweet=None
if rank==0:
    #loop through data
    for s in data:
        tweet=json.loads(s)
        chunk.append(tweet)
        #once the chunk size can be equally distributed send it
        if len(chunk)==chunk_size:
            chunk=[chunk[x:x+package_size] for x in range(0,chunk_size,package_size)]
            received_tweets=comm.scatter(chunk,root=0)
            #process tweet
            for received_tweet in received_tweets:
                final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
            chunk=[] 
    if len(chunk)>0:
        for t in chunk:
            final_lang_dict,final_hashtag_dict=tweet_processing(t,final_lang_dict,final_hashtag_dict)
    
    recvd=comm.scatter([0]*size,root=0)
    #recv all final comms from ranks    
    for i in range(1,size):
        r=comm.recv(source=i,tag=i)
    
        final_lang_dict=combine_dict(final_lang_dict,r[0])
        final_hashtag_dict=combine_dict(final_hashtag_dict,r[1])
    #sort and print hashtag and language dict   
    h_sorted = {x: y for x,y in sorted(final_hashtag_dict.items(), key = lambda item:item[1],reverse=True)[0:10]}
    l_sorted = {x: y for x,y in sorted(final_lang_dict.items(), key = lambda item:item[1],reverse=True)[:10]}
    #print hashtags
    print("Hashtags - Top 10:")
    for i,(name,count) in enumerate(h_sorted.items(),1):
        print("{}. {},{}".format(i,name,count))
    #print languages 
    print("Languages - Top 10:")
    for i,(name,count) in enumerate(l_sorted.items(),1):
        if name == "und":
            print("{}. undefined(und),{}".format(i,count))
        elif name == "in":
            print("{}. Indonesian(in),{}".format(i,count))
        else:
            full_name = langs.get(part1=name).name
            print("{}. {}({}),{}".format(i,full_name,name,count))
    end = time.time()
    print("Time in seconds = ", end - start)

else:
    while flag:
        received_tweets=comm.scatter(None,root=0)
        if received_tweets==0:
            flag=False
            comm.send([final_lang_dict,final_hashtag_dict],dest=0,tag=rank)
        else:
            for received_tweet in received_tweets:
                final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
            
