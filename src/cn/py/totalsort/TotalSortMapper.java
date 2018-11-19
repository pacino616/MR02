package cn.py.totalsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * IntWritable默认按照数字大小排序
 * Text默认按照字典排序
 *
 */
public class TotalSortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] nums = line.split(" ");
		for (String num : nums) {
			context.write(new IntWritable(Integer.parseInt(num)), new IntWritable(1));
		}
	}
}
