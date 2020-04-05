import ijson
import string

punctuation = string.punctuation
hashtags = dict()
languages = dict()

f = open("data/tinyTwitter.json")
#f = open("data/smallTwitter.json")
#f = open("data/bigTwitter.json")
parser = ijson.parse(f)


try:
    #find each part of json in stream
    for prefix,event,value in parser:
        #print(prefix ,value)
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
#catch exception
except:
    print("")

print(hashtags)
print(languages)

        

