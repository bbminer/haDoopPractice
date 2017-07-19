package com.min.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo implements Writable {
	private String info;
	private int flag;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(info);
		out.writeInt(flag);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.info = in.readUTF();
		this.flag = in.readInt();
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}
}
