package cn.py.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordDriver {	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WordDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//����combine����࣬������趨��Ĭ����û��combine���̵�
		//cmmbine���������úϲ�������MapTask��ǰ����
		//���Լ���reduceTask�ĺϲ�����
		//ʹ��combine���ƣ����ܸı����Ľ��
		job.setCombinerClass(WordCombine.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/work"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/work/result"));
		
		job.waitForCompletion(true);
	}

}	
