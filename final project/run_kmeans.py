
import sys 
import re
import math 
from pyspark import SparkContext 


def add_points(point1, point2):

    # directly adding latitude and longitude must be wrong, convert to cartesian coordinates then add 

    # if len(point1) == len(point2) and len(point1) ==2:
    #     point1 = Sphe2cart(point1)
    #     point2 = Sphe2cart(point2)

    #     lon, lat = Cart2Sphe((point1[0]+ point2[0],point1[1]+ point2[1],point1[2]+ point2[2]))


    # return((math.degrees(lon), math.degrees(lat)))
    return (point1[0]+point2[0],point1[1]+point2[1])

def Sphe2cart(coordinates):

    #convert to radious coordinates
    longitude , latitude = math.radians(coordinates[0]), math.radians(coordinates[1])

    x,y,z = math.cos(latitude)*math.cos(longitude), math.cos(latitude)*math.sin(longitude), math.sin(latitude)

    return((x,y,z))


def Cart2Sphe(xyz):

    longitude = math.atan2(xyz[0],xyz[1])
    latitude = math.atan2(xyz[2],math.sqrt(xyz[0]**2 + xyz[1]**2))

    return ((longitude,latitude))


def get_Eculidian_Distance(point1, point2):

    Cart1 = Sphe2cart(point1)
    Cart2 = Sphe2cart(point2)
    magnitude = 0

    for i in range(3):
        magnitude += (Cart2[i] - Cart1[i]) ** 2 

    return math.sqrt(magnitude)

def get_GreatCircle_Distance(point1, point2):

    from_phi = math.radians(point1[0])
    to_phi = math.radians(point2[0])
    delta_phi = math.radians(point2[0] - point1[0])
    delta_lambda = math.radians(point2[1] - point1[1])

    a = math.sin(delta_phi/2) * math.sin(delta_phi/2) + math.cos(from_phi) * math.cos(to_phi) * math.sin(delta_lambda/2) * math.sin(delta_lambda/2)
    delta_sigma = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


    return(6378.1 * delta_sigma)


def get_Distance(point1, point2, method):
    if method == "Eucl":
        return get_Eculidian_Distance(point1, point2)
    if method == "Circle":
        return get_GreatCircle_Distance(point1, point2)
    else:
        return 0

def get_Cloest_Point(point, center_list, method):
    dist = []
    for center in center_list:
        dist.append(get_Distance(center, point, method))

    return dist.index(min(dist))







def main(data, k, method, output_directory, output_directory_centroids):

    old_center_list = data.takeSample(False, k)
    #print(old_center_list)
    centroids_RDD = sc.parallelize(old_center_list)

    stop = False
    converge_threshold = 0.1 

    data_old = data.map(lambda point: (get_Cloest_Point(point, old_center_list, method),point)).persist()
    # print(data_old.collect())

    while not stop:
        data_new = data_old.map(lambda line: (line[0], (line[1], 1)))\
            .reduceByKey(lambda line1, line2: (add_points(line1[0],line2[0]), line1[1]+line2[1]))\
            .map(lambda line: (line[0], (line[1][0][0]/line[1][1], line[1][0][1]/line[1][1])))\
            .sortByKey(True)\
            .map(lambda line:line[1])\
            .persist()


        new_ceneter_list = data_new.collect()
        print(new_ceneter_list)

        converged = True

        for i in range(k):
            if converged and get_Distance(new_ceneter_list[i], old_center_list[i], method) < converge_threshold:
                converged = True
            else:
                converged = False


        if converged:
            stop= True

        else:
            old_center_list = new_ceneter_list
            data_old = data.map(lambda point: (get_Cloest_Point(point, old_center_list,method),point)).persist()

            centroids_RDD = sc.parallelize(old_center_list)
            centroids_RDD.persist()

            continue



    data_out = data.map(lambda point: (get_Cloest_Point(point,new_ceneter_list,method),point))

    data_out.saveAsTextFile(output_directory)

    centroids_RDD.saveAsTextFile(output_directory_centroids)















if __name__ == '__main__':

    if len(sys.argv) != 5: 
      print ("utility : <input_directory>, <k>, <distance_type(Ecul or Cricle)>, <output_directory(data with cluster label)>, <output_directory(cluster center list)>")

      exit(-1)

    else:

        input_file = sys.argv[1]
        k  = int(sys.argv[2])
        method = sys.argv[3]
        output_directory = sys.argv[4]
        output_directory_centroids = sys.argv[5]



        sc = SparkContext()

        data =sc.textFile(sys.argv[1])

        main(data, k, method, output_directory, output_directory_centroids)


    # input_file = "/home/cloudera/final proj/long_lat_data.txt"
    # k =3
    # method = "Eucl"
    # output_directory = "file/home/cloudera/final proj/output1"
    # output_directory_centroids = "file/home/cloudera/final proj/output_cent1"

    sc= SparkContext()


    mydata = sc.textFile(input_file) #load file into spark

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

    data = separateModel.map(lambda words: [words[-2],words[-1]])

    data1=data.map(lambda line:line.split("', u'"))\
    .map(lambda line: (float(line[0]),float(line[1])))

    print data1.take(1)
    main(data1, k, method, output_directory, output_directory_centroids)

    sc.stop()



