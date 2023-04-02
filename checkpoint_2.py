import re
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import multiprocessing as myprocessor
import time
import numpy as np

mystops=stopwords.words('english')
mystems=SnowballStemmer('english')


class DictionaryClass(object):
        def __init__(self,x,N=0,S=0,k=0,a=None):
            self.word_code=k  #wordcode
            self.doc_freq=N #documentfrequency
            self.global_freq=S  #globalfrequency
            self.word=x
            self.inverted_indices=[]
            if a is None:
                a={}

        #after word has already appeared once
        def frequency_contin(self):
            self.global_freq=self.global_freq+1
        
        #first time word appears
        def first_time_found(self,a=None):
            self.global_freq=self.global_freq+1
            self.doc_freq=self.doc_freq+1

        def invert_index(self):
            #add docid nd tf to 'unigram'
            temp_inv=''
            for j in self.inverted_indices:
                temp_inv=temp_inv +"(" + str(j.doc_id) + ","+ str(j.tf) + ")"
            return temp_inv


class DocFreqWiki:
        def __init__(self,x,a=None):       
            self.doc_id=x       #wordcode
            self.tf=1         #frequency
            if a is None:
                a={}


def myread(currWiki):
    print(currWiki) #current chunk process
    mytic=time.perf_counter()
    
    
    dictionaryfile=open("dictionary"+ str(currWiki)+".txt", "w+")
    unigramfile=open("unigram" + str(currWiki) + ".txt","w+")
    dictionaryHash={}
    word_coding=0

    #                                               PATTERN DEFINE
    pattern1='https://\D+'                          #removes links, except for their id 
    pattern2='\W+'                                  #removes special characters
    pattern3=r"[a-zA-Z]*\d+[a-zA-Z]*|\d[a-zA-Z]+\d" #removes ANY digit found in the file, even if associated with letters
    pattern4="\w*_\w*"                              #removes anything with an underscore
    pattern5="(\slt\s|\sgt|onlyinclude)"            #removes specific "lt onlyinclude gt lt onlyinclude gt"
    pattern6="(\s[b-hj-zB-HJ-Z]\s)"                 #removes any single character that isnt the word "a" or "i"

    #single-digit chunk case
    if currWiki<10:
            currWiki = "0" + str(currWiki)
        
    #iteration through chunks
    with open("wiki2022_small.0000"+str(currWiki)) as wiki_line:
        for x in wiki_line:


            # use the patterns and REMOVE them 
            regex1 = re.sub(pattern1,' ',x)
            # keep wiki id 
            likethis=regex1.split()
            link_id=likethis[0]
            str(regex1)

            regex2 = re.sub(pattern2,' ',regex1)
            regex3 = re.sub(pattern3,' ',regex2)
            regex4 = re.sub(pattern4,' ',regex3)
            regex5 = re.sub(pattern5,' ',regex4)
            regex6 = re.sub(pattern6,' ',regex5)
            regex7 = ' '.join([i for i in regex6.split() if i not in mystops])

            #list-ify current wiki line
            searching=regex7.split()
            temp_doclist={}

            #DICTIONARY file(s)
            #casing doesnt affect frequency, stem
            for y in searching:
                y=y.lower()
                y=mystems.stem(y)
                if y not in dictionaryHash:

                    dictionaryHash[y]=DictionaryClass(y,1,1,0)
                    temp_doclist[y]=DocFreqWiki(link_id)
                else:
                    if y not in temp_doclist:
                        temp_doclist[y]=DocFreqWiki(link_id)
                        dictionaryHash[y].first_time_found()
                    else:
                        dictionaryHash[y].frequency_contin()
                        temp_doclist[y].tf+=1


            for x in temp_doclist:
                dictionaryHash[x].inverted_indices.append(temp_doclist[x])

#    print("got to dict listsort")
    #assigning word codes
    dictionaryList=sorted(dictionaryHash.keys())
    for x in dictionaryList:
        dictionaryfile.write(x + "\n")
        dictionaryHash[x].word_code=word_coding
        word_coding=word_coding+1

    #UNIGRAM file(s)

    unigram=list(dictionaryHash.values())
    unigram.sort(key=lambda x: x.word_code, reverse=False)

    #display
    for x in unigram:
        unigramfile.write(str(x.word_code)+ '  ' + x.word + '  ' +  str(x.doc_freq) + '  ' +  str(x.invert_index()) + '\n')

    mytoc=time.perf_counter()
    heretime=mytoc-mytic
 #   print(heretime)

#Map/Reduce
def combination(currWiki,currWiki_next):
    first_l=[]
    second_l=[]
    with open ("dictionary" + str(currWiki) + ".txt") as first:
        for x in first:
            first_l.append(x)


    with open ("dictionary" + str(currWiki_next) + ".txt") as second:
        for x in second:
            second_l.append(x)




    third_l=[]
    third_l=first_l+second_l
    third_l=np.unique(third_l)
    third_l.sort()

    with open ("dictionary" + str(currWiki) + ".txt", "w+") as first:
        for x in third_l:
            first.write(x)

#Multiprocessing
def main():
    process_pool=myprocessor.Pool()
    process_pool.map(myread,range(0,32))
    mylisty=list(range(0,32))
    while len(mylisty) >=1:
        myb=mylisty[:len(mylisty)//2]
        mya=mylisty[len(mylisty)//2:]
        process_pool.starmap(combination,zip(myb,mya))
        mylisty=myb


if __name__=='__main__':
    main()