package cn.py.frident;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Options.BooleanOption;

public class SecFriendReducer extends Reducer<Text,IntWritable,Text,NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
		Boolean flag=true;
		
		for(IntWritable value:values){
			if(value.get()==1){
				flag=false;
				break;
			}
		}
		if(flag){
			context.write(key, NullWritable.get());
		}
	}
}
