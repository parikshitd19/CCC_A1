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


f_name = "data/tinyTwitter.json"
#f_name = "data/smallTwitter.json"
#f_name = "data/bigTwitter.json"


#open file
f = open(f_name, "rb")


####################################
#use No. of processes to split lines in file
####################################
if rank == 0:
    #read file length
    f_size = os.path.getsize(f_name)
    print(f_size)
    #t_size doesn't include master
    t_size = size-1
    last_lines = 0
    duration = f_size/t_size
    splits = [[int(i * duration),int((i+1) * duration)] for i in range(t_size)]
    print(splits)

    for i,split in enumerate(splits):
        start,end = split
        if len(splits) == i+1:
            break
        f.seek(end)
        f.readline()
        splits[i][1] = f.tell() 
        splits[i+1][0] = f.tell()
    print(splits)
    print("###########################")
    for j,chunk in enumerate(splits,1):
        comm.send(chunk,dest=j)
    '''
    else:
        #need fix here for only one node
        print("What do you do if theres only one node????")
    '''
    h_result = dict()
    l_result = dict()
    for i in range(t_size):
        result = (comm.recv(source=i+1,))
        h_result = {key: h_result.get(key,0) + result[0].get(key,0) for key in
                set(h_result) | set(result[0])}
        l_result = {key: l_result.get(key,0) + result[1].get(key,0) for key in
                set(l_result) | set(result[1])}
    h_sorted = {x: y for x,y in sorted(h_result.items(), key = lambda item:
        item[1],reverse=True)[0:10]}
    l_sorted = {x: y for x,y in sorted(l_result.items(), key = lambda item:
        item[1],reverse=True)[:10]}

    for i,(name,count) in enumerate(h_sorted.items(),1):
        print("{}. {},{}".format(i,name,count))

    for i,(name,count) in enumerate(l_sorted.items(),1):
        print("{}. {},{}".format(i,name,count))

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
    f.seek(start)
    print(rank, "this is the start", start, f.tell())
    parser = ijson.parse(f)
    try:
        #find each part of json in stream
        print("breaks after this",f.tell(),rank)
        f_size = os.path.getsize(f_name)
        print(f_size)
        for prefix,event,value in parser:
            if rank == 2:
             print(rank, f.tell(),end,(f.tell()<end))
            if f.tell() < end:
                #check for language
                if prefix == "rows.item.doc.metadata.iso_language_code":
                        #add 1 if already in dict
                        if value in languages:
                            languages[value] = languages[value]+1
                        #add to dict
                        else:
                            languages[value] = 1
                if prefix == "rows.item.doc.entities.hashtags.item.text":
                        #create new word
                        word = str()
                        #make lower
                        value = value.lower()
                        if value in languages:
                            hashtags[value] = hashtags[value]+1
                        else:
                            hashtags[value] = 1
            else:
                break
    #catch exception
    except Exception as e:
        print(e)

    comm.send([hashtags,languages],dest=0)
    #print(rank,hashtags)
    #print(rank,languages)



