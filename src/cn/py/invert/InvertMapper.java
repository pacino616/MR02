package cn.py.invert;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] words = line.split(" ");
		FileSplit split = (FileSplit) context.getInputSplit();
		
		String name = split.getPath().getName();
		for (String word : words) {
			context.write(new Text(word+"-"+name), new IntWritable(1));
		}
	}
}
