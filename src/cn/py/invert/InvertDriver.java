package cn.py.invert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InvertDriver.class);
		job.setMapperClass(InvertMapper.class);
		job.setReducerClass(InvertRduce.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/invert"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/invert/result"));
		
		//--使用了MR框架提供的job链机制
		if(job.waitForCompletion(true)){
			Job job2 = Job.getInstance(conf);
			
			job2.setMapperClass(InvertMapper2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setReducerClass(InvertReduce2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job2,new Path("hdfs://192.168.80.72:9000/invert/result"));
			FileOutputFormat.setOutputPath(job2,new Path("hdfs://192.168.80.72:9000/invert/result2"));
			
			job2.waitForCompletion(true);
		};
	}
}
