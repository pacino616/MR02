package cn.py.invert;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertRduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
//		String line = key.toString();
//		String name = line.split(".")[0];
//		String word = line.split(".")[1];
		int count = 0 ;
		for (IntWritable value : values) {
			count = count+value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
