package cn.py.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Item implements Writable,Cloneable{
	
	private String orderId="";
	private String orderTime="";
	private String productId="";
	private int num;
	private String name="";
	private double price;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(orderTime);
		out.writeUTF(productId);
		out.writeInt(num);
		out.writeUTF(name);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.orderTime = in.readUTF();
		this.productId = in.readUTF();
		this.num = in.readInt();
		this.name = in.readUTF();
		this.price = in.readDouble();
	}

	public Item clone(){
		Item o = null;
		try {
			o = (Item)super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return o;
	}
	
	
	
	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getOrderTime() {
		return orderTime;
	}

	public void setOrderTime(String orderTime) {
		this.orderTime = orderTime;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "Item [orderId=" + orderId + ", orderTime=" + orderTime + ", productId=" + productId + ", num=" + num
				+ ", name=" + name + ", price=" + price + "]";
	}
	
	
	
}
