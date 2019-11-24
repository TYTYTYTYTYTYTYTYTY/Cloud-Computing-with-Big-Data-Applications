
# coding: utf-8

# In[13]:


#loading the dataset

from pyspark import SparkContext

#sc = SparkContext("local", "Simple App")
mydata = sc.textFile("file:/home/cloudera/training_materials/dev1/data/devicestatus.txt") #load file into spark

#setting the delimiter
def splitLine(line):
	delimiter = line[19]
	return line.split(delimiter)

#splitting the data into records
splitRecords = mydata.map(splitLine)
#filtering out the records less than length of 14
validLengthRecords = splitRecords.filter(lambda words: len(words) == 14)
#Filtering the valid cordinates from the data
ValidCoordinates = validLengthRecords.filter(lambda words: (words[-1] != "0" or words[-2] != "0"))

separateModel = ValidCoordinates.map(lambda words: words[0:1] + words[1].split(" ", 1)+ words[2:])
#print separateModel.take(5)

Extracted_Data = separateModel.map(lambda words: [words[-2],words[-1]]+ words[0:4])

print Extracted_Data.take(5)
#for i in Extracted_Data.take(5):print(i)
#saving the data to specified directory
Extracted_Data.saveAsTextFile("/loudacre/devicestatus_etl")
#Sperately storing the longitude and latitude values for visualisation
Extracted_Data.saveAsTextFile("file:/home/cloudera/Desktop/step1/long_lat_data.txt")











