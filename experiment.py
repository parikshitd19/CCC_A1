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


#open file
f = open(f_name,"r")



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
    t_size = 3
    duration = f_size/t_size
    split = [(int(i * duration),int((i+1) * duration)) for i in range(t_size)]
    print(split)
    #check if split will get integers
    #split based on processes given
    if size > 1:
        lines = f_len/t_size
        for i in range(t_size):
            #if last add last few lines taken from last if statement
            if i == t_size-1:
                split.append([(lines*i)+1,(lines*(i+1)) + last_lines])
            else:
                split.append([(lines*i)+1,lines*(i+1)])
        print(split)
        #send out to each process
        for j,chunk in enumerate(split,1):
            comm.send(chunk,dest=j)
    else:
        #need fix here for only one node
        print("What do you do if theres only one node????")
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
    chunk = comm.recv(source=0)
    f.seek(0)
    parser = ijson.parse(f)
    start = int(chunk[0]) - 1
    end = int(chunk[1]) - 1
    ################
    #misc
    #punctuation = string.punctuation
    hashtags = dict()
    languages = dict()
    try:
        j = 0
        #find each part of json in stream
        for prefix,event,value in parser:
            #check for language
            if prefix == "rows.item.doc.metadata.iso_language_code":
                if j >= start and j <= end:
                    #add 1 if already in dict
                    if value in languages:
                        languages[value] = languages[value]+1
                    #add to dict
                    else:
                        languages[value] = 1
                    j += 1
                else:
                    j += 1
                    pass
            if prefix == "rows.item.doc.entities.hashtags.item.text":
                if j >= start and j <= end:
                    #create new word
                    word = str()
                    #make lower
                    value = value.lower()
                    if value in languages:
                        hashtags[value] = hashtags[value]+1
                        break
                    else:
                        hashtags[value] = 1
                        break
                    '''
                    #loop over word to check for punctuation
                    for i,letter in enumerate(value):
                        #if punctuation found, add and break
                        if (letter in punctuation):
                            if word in languages:
                                hashtags[word] = hashtags[value]+1
                                break
                            else:
                                hashtags[word] = 1
                                break
                        #if length of word reached, add and break
                        elif (len(value) == i+1):
                            word = word + letter
                            if word in languages:
                                hashtags[word] = hashtags[value]+1
                                break
                            else:
                                hashtags[word] = 1
                                break
                        #add letter to created word
                        else:
                            word = word + letter
                    '''
                else:
                    pass
    #catch exception
    except Exception as e:
        print("")

    comm.send([hashtags,languages],dest=0)
    #print(rank,hashtags)
    #print(rank,languages)

            

