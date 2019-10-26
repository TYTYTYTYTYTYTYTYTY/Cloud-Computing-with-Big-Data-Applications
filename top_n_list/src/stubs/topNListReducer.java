package stubs;

import java.io.BufferedReader;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer's input are local top N with their frequency
 * We use a single reducer to creates the final top N.s
 * input of reducer is Nullwritable and text (same as mapperer)
 * output of the reducer is intwritable and text
 */
public class topNListReducer extends
Reducer<NullWritable, Text, IntWritable, Text> implements Configurable{
	
	private int N ; 
	private Configuration conf;
	// used two sorted map to store 
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
	private SortedMap<String, String> title = new TreeMap<String, String>();
	
	public void setConf(Configuration configuration) {
		File f= new File("/home/cloudera/workspace/top_n_list/movie_titles.txt");
		top = new TreeMap<Integer, String>(Collections.reverseOrder());
		try{
			BufferedReader b = new BufferedReader(new FileReader(f));
			String line;
			while((line = b.readLine()) != null)
			{
				String film_id;
				film_id = line.split(",")[0];
				String film_title;
				film_title = line.split(",")[2];
				title.put(film_id,film_title);
			}
			b.close();
		}
		catch(FileNotFoundException e){	}
		catch(IOException e){}
	}
	
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String line = value.toString().trim();
			String[] elems = line.split(",");
			String id = elems[0];
			int rate = Integer.parseInt(elems[1]); 
			top.put(rate, id);
			// keep only top N
			if (top.size() > N) {
				top.remove(top.lastKey());
			}
		}
		
		
		
	}
	/**
	 * this method set up the N as configuration parameter
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		this.N = Integer.parseInt(conf.get("N")); 
	}


	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
	    List<Integer> keys = new ArrayList<Integer>(top.keySet());
	    for(int i=0; i<=keys.size()-1 ; i++){
		context.write(new IntWritable(keys.get(i)), new Text(title.get(top.get(keys.get(i)))));
	    }
	}
}