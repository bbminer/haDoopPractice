package com.min.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class A implements Writable {
	private String phone;
	private long upPackNum;
	private long downPackNum;
	private long upPayLoad;
	private long downPayLoad;

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public long getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(long upPackNum) {
		this.upPackNum = upPackNum;
	}

	public long getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(long downPackNum) {
		this.downPackNum = downPackNum;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.downPackNum);
		out.writeLong(this.upPackNum);
		out.writeLong(this.downPayLoad);
		out.writeLong(this.upPayLoad);
		out.writeUTF(this.phone);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		phone = in.readUTF();
		downPackNum = in.readLong();
		upPackNum = in.readLong();
		downPayLoad = in.readLong();
		upPayLoad = in.readLong();
	}

	@Override
	public String toString() {
		return "A [phone=" + phone + ", upPackNum=" + upPackNum + ", downPackNum=" + downPackNum + ", upPayLoad="
				+ upPayLoad + ", downPayLoad=" + downPayLoad + "]";
	}
}
