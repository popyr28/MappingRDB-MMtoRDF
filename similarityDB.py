from nltk.corpus import wordnet, stopwords, brown
import re
import Levenshtein
import textdistance
import numpy as np
import crawlInstance as ins
from nltk.corpus.reader.wordnet import wup_similarity
from nltk.tag.sequential import UnigramTagger
from operator import itemgetter
from pprint import pprint

#Load property ontology
with open('parseResult/attDBpedia.txt', encoding='utf8') as f:
    listProp = [line.rstrip() for line in f]

with open('parseResult/instances.txt', encoding='utf8') as f:
    listInstance = [line.rstrip() for line in f]

with open('parseResult/schema.txt', encoding='utf8') as f:
    listSchema = [line.rstrip() for line in f]

#break term into token (1 word)
def tokenization(term):#return list of token
    termList = []
    #using regular expression, split with divider by capital letter
    termList = re.findall(r'[a-zA-Z](?:(?![a-z])|[a-z]*)', term)
    return termList

#remove all stopword like a, is, the, dll
stopWords = set(stopwords.words('english'))
def removeStopWords(term): #return term list that doesn't contain stopword
    termList = []
    term = [t.lower() for t in term]
    for t in term:
        if t not in stopWords:
            termList.append(t)
        else:
            pass
    return termList

#function for convert treebank tag to WordNet POS tag
def getWordnetPos(treebank_tag):
    # print(treebank_tag)
    if treebank_tag == None:
        return wordnet.NOUN
    elif treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV

#function for add tag on term
train_sents = brown.tagged_sents()[:6000]
def tagPos(term): #return word on synset wordnet and its tag
    wn = []
    tagger = UnigramTagger(train_sents)
    wordTag = tagger.tag(term) #add tag using unigram tagger
    for word, tag in wordTag: #looping for word and tag that the result from unigram tagger
        wnTag = getWordnetPos(tag) #get wordNet tag
        allSynsets = wordnet.synsets(word, wnTag) #get all synset wordNet that based on word, wnTag
        score = []
        if(len(allSynsets) != 0): #if that contain any synset
            for synset in allSynsets:   #finding the score similarity of levenshtein distance of word and synset
                temp = synset.name().split('.')[0]   
                result = (Levenshtein.distance(word, temp))
                score.append([result, word, temp])
            Idx = score.index(min(score, key=itemgetter(0))) #idx is for index of synset that have minimum score
            wn.append(wordnet.synsets(word, wnTag)[Idx])
        else :
            return word
    return wn #return list word and tag of a term

fileMatch = open("matchTerm/matchAllPropAtt.txt", "a+", encoding='utf8') #it will save score of matching between term
fileReachTh = open("matchTerm/resultMatchPropAtt.txt", "a+", encoding='utf8') #it will save match score that more than threshold between attribute table & property (DBpedia & DM)
fileHighest = open("matchTerm/resultPropAttHighest.txt", "a+", encoding='utf8') # it will save highest match score between 

def matchTerm(attribute, properties):
    matchingResult = []
    loop = 0
    maxScore = []
    term1 = attribute
    term1 = tagPos(removeStopWords(tokenization(term1))) #term1 contain pair of word and tag 
    for term2 in properties: #looping all term on properties
        property = term2
        term2 = tagPos(removeStopWords(tokenization(term2))) #term2 contain pair of word and tag 
        vectorLength = max(len(term1), len(term2)) #define maksimum length of term1 and term2
        V1 = np.zeros((vectorLength))
        V2 = np.zeros((vectorLength))
        sigma = 0
        C1 = 0
        C2 = 0
        gamma = 1.8

        #this code for matching term1 to term2
        i = 0
        for word1 in term1:
            helper = np.zeros((vectorLength))
            j = 0
            for word2 in term2:
                if type(term1) == str or type(term2) == str:
                    helper[j] = 0
                else:
                    helper[j] = (wup_similarity(word1, word2))
                j+=1
            V1[i] = np.amax(helper)
            i+=1

        #this code for matching term2 to term1
        k = 0
        for word2 in term2:
            helper = np.zeros((vectorLength))
            l = 0
            for word1 in term1:
                if type(term1) == str or type(term2) == str:
                    helper[l] = 0
                else:
                    helper[l] = (wup_similarity(word2, word1))
                l+=1
            V2[k] = np.amax(helper)
            k+=1

        C1 = np.count_nonzero(V1)
        C2 = np.count_nonzero(V2)
        
        #vector count
        S = np.dot(V1, V2)
        sumC = C1 + C2
        if sumC > 0 :
            sigma = sumC/gamma
        elif sumC == 0:
            sigma = vectorLength/2
        score = S/sigma
        maxScore.append([score, attribute, term1, property, term2])
        print(f"Loop ke-{loop}, Matching {term1} and {term2}")

        fileMatch.write(f"-- {attribute:50s} -- {property:50s} -- {score}\n")
        
        loop+=1
    
    fileMatch.write("\n===================== NEXT ATTRIBUTE =====================\n")

    matchingResult.append(max(maxScore, key=itemgetter(0)))
    fileHighest.write(f"-- {matchingResult[0][1]:50} -- {str(matchingResult[0][2]):50} -- {matchingResult[0][3]:50} -- {str(matchingResult[0][4]):50}-- {matchingResult[0][0]}\n")

    if (matchingResult[0][0] >= 0.5) :
        fileReachTh.write(f"-- {matchingResult[0][1]:50} -- {str(matchingResult[0][2]):50} -- {matchingResult[0][3]:50} -- {str(matchingResult[0][4]):50} -- {matchingResult[0][0]}\n")
        return matchingResult
    else:
        return None

#function for matching field of database and individual ontology
fileRDBinstance = open('matchTerm/matchAllRDBinstances.txt', "a+", encoding='utf8')
fileRDBInstHighest = open('matchTerm/matchHighestRDBinstances.txt', "a+", encoding='utf8')
fileMatchRDBInst = open('matchTerm/matchRDBinstances.txt', "a+", encoding='utf8')

def matchingIndividual(value, ind):  #matching value rdb & individu ontology
    matchingRes = []
    maxScoreInd = []
    termV_tmp = str(value)
    termV = termV_tmp.lower()
    for i in ind:
        termI = i.lower()
        score = textdistance.ratcliff_obershelp(termV, termI)
        matchingRes.append([score, value, i])
        fileRDBinstance.write(f"; {str(value):50} ; {str(i):50} ; {score}\n")
    fileRDBinstance.write("\n===================NEXT INDIVIDUAL==================\n")

    maxScoreInd.append(max(matchingRes, key=itemgetter(0)))
    fileRDBInstHighest.write(f"; {str(maxScoreInd[0][1]):50}; {str(maxScoreInd[0][2]):50} ; {maxScoreInd[0][0]}\n")
    if (maxScoreInd[0][0] > 0.8):
        fileMatchRDBInst.write(f"; {str(maxScoreInd[0][1]):50}; {str(maxScoreInd[0][2]):50} ; {maxScoreInd[0][0]}\n")
        return maxScoreInd[0][2]
    else:
        return None

#function for matching value of descriptive metadata to field of database
def matchMetadata(value1, value2):
    term1 = value1.lower()
    term2 = value2.lower()
    res = textdistance.ratcliff_obershelp(term1, term2)
    return res
