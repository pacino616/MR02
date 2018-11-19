package cn.py.profit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitDriver {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ProfitDriver.class);
		job.setMapperClass(ProfitMapper1.class);
		job.setReducerClass(ProfitRduce1.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Profit.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/profit"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/profit/result"));
		
		//--使用了MR框架提供的job链机制
		if(job.waitForCompletion(true)){
			Job job2 = Job.getInstance(conf);
			
			job2.setMapperClass(ProfitMapper2.class);
			job2.setMapOutputKeyClass(Profit.class);
			job2.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.setInputPaths(job2,new Path("hdfs://192.168.80.72:9000/profit/result"));
			FileOutputFormat.setOutputPath(job2,new Path("hdfs://192.168.80.72:9000/profit/result2"));
			
			job2.waitForCompletion(true);
		};
	}
}
