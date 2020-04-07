import ijson
import string
from mpi4py import MPI
import os

#mpi variables
comm = MPI.COMM_WORLD
comm.Barrier()
size = comm.size
rank = comm.rank


f_name = "data/tinyTwitter.json"
#f_name = "data/smallTwitter.json"
#f_name = "data/bigTwitter.json"





####################################
#use No. of processes to split lines in file 
####################################
if rank == 0:
    #open file
    f = open(f_name,"r")
    #read file length
    f_size = os.path.getsize(f_name)
    print(f_size)
    #t_size doesn't include master
    t_size = size-1
    last_lines = 0
    t_size = 3
    duration = f_size/t_size
    splits = [[int(i * duration),int((i+1) * duration)] for i in range(t_size)]
    print(splits)

    for i,split in enumerate(splits):
        start,end = split
        if len(splits) == i+1:
            break
        f.seek(end)
        f.readline()
        splits[i][1] = f.tell() -1
        splits[i+1][0] = f.tell() 
    print(splits)
    print("###########################")
    f.close()
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
    print(h_result)
    print(l_result)


####################################
else:
    ################
    #open file
    f = open(f_name,"r")
    chunk = comm.recv(source=0)
    start = chunk[0]
    end = chunk[1]
    ################
    #misc
    hashtags = dict()
    languages = dict()
    f.seek(start)
    parser = ijson.parse(f)
    try:
        #find each part of json in stream
        print(rank,f.tell(),start,end)
        while f.tell() <= end:
            print(f.tell(),rank)
            for prefix,event,value in parser:
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
    #catch exception
    except Exception as e:
        print("")

    comm.send([hashtags,languages],dest=0)
    #print(rank,hashtags)
    #print(rank,languages)

            

