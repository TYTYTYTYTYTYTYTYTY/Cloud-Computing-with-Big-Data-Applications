package stubs;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class topNListReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

	private TreeMap<Integer, Integer> top_n_items; // treemap auto sorting 
	private Map<Integer, String> film_table; // hashtable key:film_id value:film_name 
	private Configuration conf;
	private int N;
	private String filmNamesPath; // film name input file 
	
	public void setup(Context context){
		top_n_items = new TreeMap<Integer, Integer>(Collections.reverseOrder()); // change to descending order 
		conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));//top N pass by tool runner 
		filmNamesPath = conf.get("fileNamesPath"); // file name pass by tool runner 
		
	}
	
	@Override 
	public void reduce(NullWritable key, Iterable<Text> values, Context context){
		for(Text value:values){
			String line =value.toString();
			int rate_sum = Integer.parseInt(line.split("\\s+")[0]);
			int film_id  = Integer.parseInt(line.split("\\s+")[1]);
			top_n_items.put(rate_sum, film_id);	// rate sum as key to be automatically sorted 	
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		int counter =0 ;
		Iterator<Entry<Integer, Integer>> iter = top_n_items.entrySet().iterator();
		Path filePath = new Path(filmNamesPath);
		film_table = fill_movie_name(filePath);
		// iterate through the first n in tree map
		while (counter < N && iter.hasNext()) {
	    	counter++;
			Map.Entry<Integer,Integer> movie = (Map.Entry<Integer,Integer>)iter.next();
	        // putback movie name to output
	        context.write(new IntWritable(movie.getKey()), new Text(film_table.get(movie.getValue())));
	       
	        iter.remove(); 
	    }
	}
	
	private Map<Integer, String> fill_movie_name(Path input_file) throws IOException{
		Map<Integer, String> temp_table = new HashMap<Integer, String>();
		BufferedReader reader = null;  
	      try { 
	    	  FileSystem fileSystem = FileSystem.get(new Configuration());
	          reader = new BufferedReader(new InputStreamReader(fileSystem.open(input_file)));  
	          String line = null;  
	          System.out.println(line);
	          while ((line = reader.readLine()) != null) {  
	        	 String film_id = line.split(",")[0];
	        	 String film_name = line.split(",")[2];
	        	 temp_table.put(Integer.parseInt(film_id), film_name);
	          }  
	          reader.close();  
	      }
	      finally{}
		return temp_table;
	}
	
	
}