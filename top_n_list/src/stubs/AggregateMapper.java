package stubs;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
		String line = value.toString().trim();
		if(line.split(",").length !=3){
			return;
		}
		String film_id = line.split(",")[0];
		int rate = (int)Double.parseDouble(line.split(",")[2]); // need sum not avg, thus use int 
		context.write(new Text(film_id), new IntWritable(rate) );
		
	}
	
}
