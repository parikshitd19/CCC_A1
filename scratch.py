from splitstream import splitfile
import json
import sys

def add_to_dict(dictionary,element):
	if element in dictionary.keys():
		dictionary[element]+=1
	else:
		dictionary[element]=1
	return dictionary

def tweet_processing(tweet,lang_dict,hashtag_dict):
	if len(tweet['doc']['entities']['hashtags'])>0:
		for hash in tweet['doc']['entities']['hashtags']:
			hashtag_dict=add_to_dict(hashtag_dict,hash['text'])

	if tweet['doc']['lang']==tweet['doc']['metadata']['iso_language_code']:
		lang_dict=add_to_dict(lang_dict,tweet['doc']['lang'])
	return lang_dict,hashtag_dict


data=splitfile(open("data/smallTwitter.json","r"), format="json", startdepth=2)

count=0
lang_dict={}
hashtag_dict={}

for s in data:
	tweet=json.loads(s)
	lang_dict,hashtag_dict=tweet_processing(tweet,lang_dict,hashtag_dict)
	count+=1



print(count)
print(lang_dict)
print(hashtag_dict)