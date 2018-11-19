package cn.py.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TotalDriver.class);
		job.setMapperClass(TotalSortMapper.class);
		job.setReducerClass(TotalReduce.class);
		
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TotalPartition.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/totalsort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/totalsort/result"));
		
		job.waitForCompletion(true);
	}

}
