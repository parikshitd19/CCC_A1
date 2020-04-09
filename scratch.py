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


data=splitfile(open("data/smallTwitter.json","r"), format="json", startdepth=2)

#Number of Tweets
count=0
#Dictionary of Languages
final_lang_dict={}
#Dictionary of HashTags
final_hashtag_dict={}
#chunk size
chunk_size=size
#Tweets Container
chunk=[]
flag=True


received_tweet=None
if rank==0:
	for s in data:
		tweet=json.loads(s)
		chunk.append(tweet)
		if len(chunk)==chunk_size:
			if rank==0:
				received_tweet=comm.scatter(chunk,root=0)
			else:
				received_tweet=comm.scatter(None,root=0)
			final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
			chunk=[]
	if len(chunk)>0:
		for t in chunk:
			final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
	recvd=comm.scatter([0]*size,root=0)
	for i in range(1,size):
		r=comm.recv(source=i,tag=i)
		final_lang_dict=combine_dict(final_lang_dict,r[0])
		final_hashtag_dict=combine_dict(final_hashtag_dict,r[1])
	h_sorted = {x: y for x,y in sorted(final_hashtag_dict.items(), key = lambda item:item[1],reverse=True)[0:10]}
	l_sorted = {x: y for x,y in sorted(final_lang_dict.items(), key = lambda item:item[1],reverse=True)[:10]}
	print(h_sorted)
	print(l_sorted)
else:
	while flag:
		received_tweet=comm.scatter(None,root=0)
		if received_tweet==0:
			flag=False
			comm.send([final_lang_dict,final_hashtag_dict],dest=0,tag=rank)
		else:
			final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
			


# for s in data:
# 	if len(chunk)<chunk_size:
# 		tweet=json.loads(s)
# 		chunk.append(tweet)
# 	if len(chunk)==chunk_size:
# 		if rank==0:
# 			received_tweet=comm.scatter(chunk,root=0)
# 		else:
# 			received_tweet=comm.scatter(None,root=0)
# 		final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
# 		chunk=[]

# if len(chunk)>0:
# 	if rank==0:
# 		for t in chunk:
# 			final_lang_dict,final_hashtag_dict=tweet_processing(received_tweet,final_lang_dict,final_hashtag_dict)
			

# if rank!=0:
# 	comm.send([final_lang_dict,final_hashtag_dict],dest=0,tag=rank)

# if rank==0:
# 	for i in range(1,size):
# 		r=comm.recv(source=i,tag=i)
# 		final_lang_dict=combine_dict(final_lang_dict,r[0])
# 		final_hashtag_dict=combine_dict(final_hashtag_dict,r[1])
# 	h_sorted = {x: y for x,y in sorted(final_hashtag_dict.items(), key = lambda item:item[1],reverse=True)[0:10]}
# 	l_sorted = {x: y for x,y in sorted(final_lang_dict.items(), key = lambda item:item[1],reverse=True)[:10]}
# 	print(h_sorted)
# 	print(l_sorted)




# if rank==0:
# 	for s in data:
		
# 		tweet=json.loads(s)
# 		if len(chunk)<chunk_size:
# 			chunk.append(tweet)
# 		else:
# 			print("here 1")
# 			received_tweet=comm.scatter(chunk,root=0)
# 			print("scattered")
# 			processed_result=tweet_processing(received_tweet,{},{})
# 			result=comm.gather(processed_result,root=0)
# 			print(len(result))
# 			chunk=[]
# 			print("here 2")
# else:
# 	print("recived")
# 	received_tweet=comm.scatter(None,root=0)
# 	# print(rank)
# 	# print(received_tweet)
# 	processed_result=tweet_processing(received_tweet,{},{})
# 	result=comm.gather(processed_result,root=0)
# 	comm.Barrier()


# if rank==0:
# 	print("I am here in 0")
# 	for s in data:
# 		tweet=json.loads(s)
# 		count+=1
# 		if len(chunk)<buff_size:
# 			chunk.append(tweet)
# 		else:
# 			buffers+=1
# 			chunk.append(buffers)
# 			print("sending"+str(buffers))
# 			comm.send(chunk,dest=max(1,buffers%max_buffers),tag=buffers)
# 			chunk=[]
# 	if len(chunk)>0:
# 		buffers+=1
# 		chunk.append(buffers)
# 		comm.send(chunk,dest=max(1,buffers%max_buffers),tag=buffers)
# 		chunk=[]
# 	for i in range(1,buffers+1):
# 		print("uuhi")
# 		result=(comm.recv(source=max(1,i%max_buffers)))
# 		final_lang_dict.update(result[0])
# 		final_hashtag_dict.update(result[1])
# 		print(count)
# 		print(final_lang_dict)
# 		print(final_hashtag_dict)
# else:
# 	chunk = comm.recv(source=0)
# 	#Dictionary for Languages
# 	tag_num=chunk[-1]
# 	print("receving"+str(tag_num))
# 	del chunk[-1]
# 	lang_dict={}
# 	#Dictionary of Hashtag
# 	hashtag_dict={}
# 	for tweet in chunk:
# 		lang_dict,hashtag_dict=tweet_processing(tweet,lang_dict,hashtag_dict)
# 	print("sending back"+str(tag_num))
# 	comm.send([lang_dict,hashtag_dict],dest=0)




