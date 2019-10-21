package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class AggregateReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable>{
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int rate_sum = 0;
		for (IntWritable value:values){
			rate_sum += value.get();
		}
		context.write(key, new IntWritable(rate_sum));
	}
}
