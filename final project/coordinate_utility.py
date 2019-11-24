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
    dist = list()
    for center in center_list:
        dist.append(get_Distance(center, point, method))

    return dist.index(min(dist))


print(add_points((30,100),(40,150)))









        

          
         



