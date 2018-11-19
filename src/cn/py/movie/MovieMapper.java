package cn.py.movie;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Movie, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Movie, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String movieName = line.split(" ")[0];
		int movieScore = Integer.parseInt(line.split(" ")[1]);
		Movie m = new Movie();
		m.setMovieName(movieName);
		m.setMovieScore(movieScore);
		context.write(m, NullWritable.get());
	}
}
