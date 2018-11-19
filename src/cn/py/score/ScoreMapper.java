package cn.py.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Score>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Score>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		Score s = new Score();
		String name = line.split(" ")[1];
		int score = Integer.parseInt(line.split(" ")[2]);
		s.setName(name);
		//���֪����ǰ�ķ�������һ�Ʒ���
		//����ͨ����ȡ��ǰMapTask�������Ƭ��Ϣ����ȡ�ļ���
		FileSplit split = (FileSplit) context.getInputSplit();
		String path = split.getPath().getName();
		if("chinese.txt".equals(path)){
			s.setChinese(score);
		}else if("english.txt".equals(path)){
			s.setEnglish(score);
		}else{
			s.setMath(score);
		}
		context.write(new Text(name), s);
	}
}
