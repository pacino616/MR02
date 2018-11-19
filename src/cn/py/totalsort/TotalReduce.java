package cn.py.totalsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalReduce extends Reducer<IntWritable, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int result = 0;
		for (IntWritable value : values) {
			result = result+value.get();
		}
		context.write(new Text(key.toString()), new IntWritable(result));
	}
}
