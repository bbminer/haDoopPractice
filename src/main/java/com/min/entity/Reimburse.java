package com.min.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Reimburse implements Writable {
	private String recordId;
	private String reimburseTime;
	private float reCost;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(recordId);
		out.writeUTF(reimburseTime);
		out.writeFloat(reCost);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		recordId = in.readUTF();
		reimburseTime = in.readUTF();
		reCost = in.readInt();
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getReimburseTime() {
		return reimburseTime;
	}

	public void setReimburseTime(String reimburseTime) {
		this.reimburseTime = reimburseTime;
	}

	public float getReCost() {
		return reCost;
	}

	public void setReCost(float reCost) {
		this.reCost = reCost;
	}

	@Override
	public String toString() {
		return "recordId=" + recordId + ", reimburseTime=" + reimburseTime + ", reCost=" + reCost;
	}
	
}
