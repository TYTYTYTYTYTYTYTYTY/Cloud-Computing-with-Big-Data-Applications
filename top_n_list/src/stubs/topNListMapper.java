package stubs;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class topNListMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Integer>  top_n_items; //treemap auto ordering 
	private Configuration conf;
	private int N;
	
	public void setup(Context context){
		top_n_items = new TreeMap<Integer, Integer>(Collections.reverseOrder()); // change to descending order 
		conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));//top N pass by config
	}
	
	public void map(LongWritable key, Text values , Context context){
		String line;
		line = values.toString();
		int rate;
		rate = Integer.parseInt(line.split("\\s+")[1]);
		int film_id;
		film_id = Integer.parseInt(line.split("\\s+")[0]);
		top_n_items.put(rate, film_id);
		
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
		int counter =0 ;
		Iterator iter = top_n_items.entrySet().iterator();
		// iterate through the hashmap 
		
		while(counter < N && iter.hasNext()){
			counter++;
			Map.Entry item = (Map.Entry)iter.next();
	        context.write(NullWritable.get(), new Text(item.getKey()+"&"+item.getValue()));
	        iter.remove(); // release space from the treemap 
		}
	}
	
	
}