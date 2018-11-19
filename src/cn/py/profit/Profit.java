package cn.py.profit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Profit implements WritableComparable<Profit>{
	
	private String name;
	private int fee;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(fee);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.fee = in.readInt();
	}

	@Override
	public int compareTo(Profit o) {
		return o.fee-this.fee;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getFee() {
		return fee;
	}

	public void setFee(int fee) {
		this.fee = fee;
	}

	@Override
	public String toString() {
		return "Profit [name=" + name + ", fee=" + fee + "]";
	}
	
	
	
}
