package cn.py.frident;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/fridend"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/fridend/result"));
		
		if(job.waitForCompletion(true)){
			Job job2=Job.getInstance(conf);
			job2.setMapperClass(SecFriendMapper.class);
			job2.setReducerClass(SecFriendReducer.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);
			
			FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.80.72:9000/fridend/result"));
			FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.80.72:9000/fridend/secresult"));
			
			job2.waitForCompletion(true);
			
		}
		
		
	}
}
