package stubs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class topNList extends Configured implements Tool{
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new topNList(),args );
		
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = getConf();

		 String Job1_OutputPath = "aggregate/";
		 conf.get("N");
		 conf.set("fileNamesPath","hdfs://quickstart.cloudera:8020/user/cloudera/movie_titles.txt");
		 if (args.length != 2) {
		     System.out.printf("Usage: -D N=<N most popular movies> <input dir> <output dir>\n");
	        System.exit(-1);
		}
		
		Job job1 = new Job(conf);
		
		job1.setJarByClass(topNList.class);
		
		job1.setJobName("Aggregate");
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(Job1_OutputPath));
		
		job1.setMapperClass(AggregateMapper.class);
		job1.setReducerClass(AggregateReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		
		
		Job job2 = new Job(conf);
		
		FileInputFormat.addInputPath(job2, new Path(Job1_OutputPath)); // job2 takes the input from job1
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.setJarByClass(topNList.class);
		
		job2.setJobName("topNList");
		
		
		job2.setMapperClass(topNListMapper.class);
		job2.setReducerClass(topNListReducer.class);

		job2.setMapOutputKeyClass(NullWritable.class);
	    job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		
		
		
		
		boolean job1_success = job1.waitForCompletion(true);
		// job2 waiting for job1 to be finished
		if(job1_success) System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	   	return 0;
	}
	
	
}
