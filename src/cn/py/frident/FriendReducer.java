package cn.py.frident;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer extends Reducer<Text,Text,Text,IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> friendsList=new ArrayList<>();
		
		for(Text value:values){
			friendsList.add(value.toString());
			if(key.toString().compareTo(value.toString())<0){
				context.write(new Text(key+"-"+value),
						new IntWritable(1));
			}else{
				context.write(new Text(value+"-"+key),
						new IntWritable(1));
			}
			
		}
		
		for(int i=0;i<friendsList.size();i++){
			for(int j=0;j<friendsList.size();j++){
				String f1=friendsList.get(i);
				String f2=friendsList.get(j);
				if(f1.compareTo(f2)<0){
					context.write(new Text(f1+"-"+f2),
							new IntWritable(2));
				}
			}
		}
		
		
	}
}
