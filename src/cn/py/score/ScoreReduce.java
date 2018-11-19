package cn.py.score;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReduce extends Reducer<Text, Score, Score, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<Score> values, Reducer<Text, Score, Score, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Score s = new Score();
		for (Score value : values) {
			s.setName(value.getName());
			s.setChinese(s.getChinese()+value.getChinese());
			s.setEnglish(s.getEnglish()+value.getEnglish());
			s.setMath(s.getMath()+value.getMath());
		}
		context.write(s, NullWritable.get());
	}
}
