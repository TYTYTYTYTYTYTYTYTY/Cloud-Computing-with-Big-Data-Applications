package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
      String line = value.toString().toLowerCase(); // put everything to lower case 
	  
	  String previousWord = "";

	  for (String word : line.split("\\W+")) {
		  if (word.length() > 0) {
			  // check end of the sentence
		  }
		    if(previousWord != ""){ 
		    	// if it is not the first word 
		    	context.write(new Text(previousWord+","+word), new IntWritable(1)); 
		    	// combine the current word and the adjacent previous word to a word pair
		    }
		    previousWord = word; 
		    // if it is the first word
    
      }
   }
}
