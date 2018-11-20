package cn.py.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Item>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Item>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] str = line.split(" ");
		System.err.println(str[0]);
		Item item = new Item();
		
		FileSplit split = (FileSplit)context.getInputSplit();
		String name = split.getPath().getName();
		if(name.startsWith("order")){
			item.setOrderId(str[0]);
			item.setOrderTime(str[1]);
			item.setProductId(str[2]);
			item.setNum(Integer.parseInt(str[3]));
		}else {
			item.setProductId(str[0]);
			item.setName(str[1]);
			item.setPrice(Double.parseDouble(str[2]));
		}
		
		context.write(new Text(item.getProductId()), item);
	}
}
