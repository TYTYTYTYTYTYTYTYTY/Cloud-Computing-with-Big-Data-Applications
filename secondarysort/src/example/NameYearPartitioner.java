package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/**
	 * Partition Name/Year pairs according to the first string (last name) in the string pair so 
	 * that all keys with the same last name go to the same reducer, even if  second part
	 * of the key (birth year) is different.
	 */
	
	// partition by first names with continuous initial
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
		char head = key.getLeft().charAt(0);
		int ascii = (int) head; // get ascii for first letter of firstname
		for(int i =0; i<numReduceTasks;i++){
		    if(ascii <=65 + (26/numReduceTasks)*(i+1)){ // "A" is 65
		    return i;	
		    }
		}
		return numReduceTasks; //if nothing strange in key, will never get here
	}
}
