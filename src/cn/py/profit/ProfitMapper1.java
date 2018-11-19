package cn.py.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		//竖线要转义
		String[] sort = line.split("\\|")[1].split(" ");
		String name = sort[0];
		int fee = Integer.parseInt(sort[1]);
		context.write(new Text(name), new IntWritable(fee));
	}
}
