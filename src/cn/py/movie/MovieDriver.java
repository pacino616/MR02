package cn.py.movie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MovieDriver.class);
		job.setMapperClass(MovieMapper.class);
		
		job.setMapOutputKeyClass(Movie.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/movie"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/movie/result"));
		
		job.waitForCompletion(true);
	}
}
