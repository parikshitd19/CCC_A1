import faulthandler; faulthandler.enable()
import ijson
import string
from mpi4py import MPI
import os
import io


#mpi variables
comm = MPI.COMM_WORLD
comm.Barrier()
size = comm.size
rank = comm.rank


#f_name = "/home/ljsimon/CCC_A1/data/tinyTwitter.json"
#f_name = "/home/ljsimon/CCC_A1/data/smallTwitter.json"
#f_name = "/home/ljsimon/CCC_A1/data/bigTwitter.json"
f_name = "data/smallTwitter.json"


#open file
f = open(f_name, "rb")

def add_to_dict(dictionary,element):
	if element in dictionary.keys():
		dictionary[element]+=1
	else:
		dictionary[element]=1
	return dictionary

def chunk_parser(languages, hashtags,f,line_len,start,end):
    parser = ijson.parse(f,buf_size=line_len)
    #prefix to look for strings
    tweet_iso = "rows.item.doc.metadata.iso_language_code"
    tweet_hash = "rows.item.doc.entities.hashtags.item.text"
    retweet_hash = "rows.item.doc.retweeted_status.entities.hashtags.item.text"
    try:
        #find each part of json in stream
        f_size = os.path.getsize(f_name)
        if rank > 1:
            print("stage 1:",rank,start,end)
            f.seek(0)
            for prefix,event,value in parser:
                if f.tell() == line_len:
                    f.seek(start)
                    break
                elif f.tell() > line_len:
                    print("fail",f.tell())
                    break
        elif rank == 1:
            print("stage 1:",rank)
            f.seek(start)
        else:
            f.seek(start)
            end = f_size
        for prefix,event,value in parser:
            #print(prefix, event, value)
            #print(rank, f.tell(),end,(f.tell()<end))
            if f.tell() < end:
                #check for language
                if prefix == tweet_iso:
                    languages = add_to_dict(languages,value) 
                if prefix == tweet_hash or prefix == retweet_hash:
                    #make lower
                    value = value.lower()
                    hashtags = add_to_dict(hashtags,value) 
            else:
                print("stage 2:",rank)
                break
    #catch exception
    except KeyError as k:
        print(k)
    except Exception as e:
        print(rank)
        print("broke")
        print(e.traceback())
    return languages,hashtags

def final_output(l_result,h_result):
    print("Languages total : ", sum(l_result.values()))
    print("Hashtags total : ", sum(h_result.values())) 
    h_sorted = {x: y for x,y in sorted(h_result.items(), key = lambda item:
        item[1],reverse=True)[0:10]}
    l_sorted = {x: y for x,y in sorted(l_result.items(), key = lambda item:
        item[1],reverse=True)[:10]}
    print("Hashtags - Top 10:")
    for i,(name,count) in enumerate(h_sorted.items(),1):
        print("{}. {},{}".format(i,name,count))

    print("Languages - Top 10:")
    for i,(name,count) in enumerate(l_sorted.items(),1):
        print("{}. {},{}".format(i,name,count))

####################################
#use No. of processes to split lines in file
####################################
if rank == 0:
    if size > 1:
        #read file length
        f_size = os.path.getsize(f_name)
        print(f_size)
        #t_size doesn't include master
        t_size = size-1
        last_lines = 0
        duration = f_size/t_size
        splits = [[int(i * duration),int((i+1) * duration)] for i in range(t_size)]

        for i,split in enumerate(splits):
            start,end = split
            if len(splits) == i+1:
                break
            f.seek(end)
            f.readline()
            splits[i][1] = f.tell() 
            splits[i+1][0] = f.tell()
        print("Splits:", splits)
        print("###########################")
        for j,chunk in enumerate(splits,1):
            comm.send(chunk,dest=j)
        h_result = dict()
        l_result = dict()
        #add all dictionaries together
        for i in range(t_size):
            result = (comm.recv(source=i+1,))
            h_result = {key: h_result.get(key,0) + result[0].get(key,0) for key in
                    set(h_result) | set(result[0])}
            l_result = {key: l_result.get(key,0) + result[1].get(key,0) for key in
                    set(l_result) | set(result[1])}
        final_output(l_result,h_result)
    else:
        hashtags = dict()
        languages = dict()
        languages, hashtags = chunk_parser(languages,hashtags,f,63556,0,0)
        final_output(languages,hashtags)


####################################
else:
    ################
    chunk = comm.recv(source=0)
    start = chunk[0]
    end = chunk[1]
    ################
    #misc
    hashtags = dict()
    languages = dict()
    #set starting pint for file
    f.seek(0)
    f.seek(1024)
    f.readline()
    line_len = f.tell()
    print(start,end)
    languages, hashtags = chunk_parser(languages,hashtags,f,line_len,start,end)
    comm.send([hashtags,languages],dest=0)
    #print(rank,hashtags)
    #print(rank,languages)


