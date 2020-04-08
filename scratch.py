from splitstream import splitfile
import json
from mpi4py import MPI

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
		for hash in tweet['doc']['entities']['hashtags']:
			hashtag_dict=add_to_dict(hashtag_dict,hash['text'])

	if tweet['doc']['lang']==tweet['doc']['metadata']['iso_language_code']:
		lang_dict=add_to_dict(lang_dict,tweet['doc']['lang'])
	return lang_dict,hashtag_dict

#MPI Variables
comm = MPI.COMM_WORLD
comm.Barrier()
size = comm.size
rank = comm.rank


data=splitfile(open("data/smallTwitter.json","r"), format="json", startdepth=2)

#Number of Tweets
count=0
#Dictionary of Languages
final_lang_dict={}
#Dictionary of HashTags
final_hashtag_dict={}
#Number of tweets in a chunk
buff_size=4999
#Number of Buffers
buffers=0
#Tweets Container
chunk=[]



if rank==0:
	print("I am here in 0")
	for s in data:
		tweet=json.loads(s)
		count+=1
		if len(chunk)<buff_size:
			chunk.append(tweet)
		else:
			buffers+=1
			comm.send(chunk,dest=1)
			chunk=[]
	if len(chunk)>0:
		buffers+=1
		comm.send(chunk,dest=1)
		chunk=[]
	for i in range(1,buffers+1):
		result=(comm.recv(source=1,))
		final_lang_dict.update(result[0])
		final_hashtag_dict.update(result[1])
		print(count)
		print(final_lang_dict)
		print(final_hashtag_dict)
elif rank==1:
	print("I am here in 1")
	chunk = comm.recv(source=0)
	#Dictionary for Languages
	lang_dict={}
	#Dictionary of Hashtag
	hashtag_dict={}
	for tweet in chunk:
		lang_dict,hashtag_dict=tweet_processing(tweet,lang_dict,hashtag_dict)
	comm.send([lang_dict,hashtag_dict],dest=0)




