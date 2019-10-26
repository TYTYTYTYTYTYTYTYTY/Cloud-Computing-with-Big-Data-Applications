package stubs;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class topNListMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, String>  top_n_items; //treemap auto ordering 
	private Configuration conf;
	private int N;
	
	public void setup(Context context){
		top_n_items = new TreeMap<Integer,String>(Collections.reverseOrder());; // change to descending order 
		conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));//top N pass by config
	}
	
	public void map(LongWritable key, Text values , Context context){
		String line;
		line = values.toString().trim();
		int rate;
		rate = (int)Double.parseDouble(line.split("\t")[1]);
		String film_id;
		film_id = line.split("\t")[0];
		top_n_items.put(rate, film_id +","+ rate);
		if (top_n_items.size() > N) {
	         top_n_items.remove(top_n_items.lastKey());// cut tail
	      }
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
		for (String str : top_n_items.values()) {
	         context.write(NullWritable.get(), new Text(str));
	    }
    }
}