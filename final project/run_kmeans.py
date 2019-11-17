from coordinate_utility import *
import sys 
import re
from pyspark import SparkContext 

def main(data, k, method, output_directory, output_directory_centroids):

	old_center_liset = data.takeSample(False, k)
	centroids_RDD = sc.parallelize(old_center_liset)

	stop = False
	converge_threshold = 0.1 

	data_old = data.map(lambda point: (get_Cloest_Point(point, center_liset, method),point)).persist()

	while not stop:
		data_new = data_old.map(lambda line: (line[0], (line[1], 1)))\
			.reduceByKey(lambda line1, line2: (add_points(line1[0],line2[0]), line1[1]+len[1]))\
			.map(lambda line: (line[0], (line[1][0]/line[2], line[1][1]/line[2])))\
			.sortByKey(True)\
			.map(lambda line:line[1])\
			.presist()


		new_ceneter_list = data_next.collect()

		converged = True

		for i in range(k):
			if converged and get_Distance(new_ceneter_list[i], old_center_list[i]) < converge_threshold:
				converged = True
			else:
				converged = False


		if converged:
			stop= True

		else:
			old_center_list = new_ceneter_list
			data_old = data.map(lambda point: (get_Cloest_Point(point, old_center_list,method),point)).persist()

			centroids_RDD = sc.parallelize(old_center_liset)
			centroids_RDD.presist()

			continue



	data_out = data.map(lambda point: (get_Cloest_Point(point,new_ceneter_list,method),point))

	data_out.saveAsTextFile(output_directory)

	centroids_RDD.saveAsTextFile(output_directory_centroids)















if __name__ == '__main__':

	if len(sys.argv) != 5: 
		print ("utility : <input_directory>, <k>, <distance_type(Ecul or Cricle)>, <output_directory (data with cluster label)>, <output_directory, (cluster center list)>")

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


