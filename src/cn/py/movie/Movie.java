package cn.py.movie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Movie implements WritableComparable<Movie>{
	
	private String movieName;
	private int movieScore;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(movieName);
		out.writeInt(movieScore);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.movieName = in.readUTF();
		this.movieScore = in.readInt();
	}

	@Override
	public int compareTo(Movie o) {
		return o.movieScore-this.movieScore;
	}

	public String getMovieName() {
		return movieName;
	}

	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}

	public int getMovieScore() {
		return movieScore;
	}

	public void setMovieScore(int movieScore) {
		this.movieScore = movieScore;
	}

	@Override
	public String toString() {
		return "Movie [movieName=" + movieName + ", movieScore=" + movieScore + "]";
	}
	
	
	
}
