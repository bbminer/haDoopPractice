package com.min.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class SortEntity implements WritableComparable<SortEntity> {
	private int first;
	private int second;

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(first);
		out.writeInt(second);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.first = in.readInt();
		this.second = in.readInt();
	}

	public int compareTo(SortEntity o) {
		// TODO Auto-generated method stub
		if (first != o.first) {
			return this.first - o.first;
		}
		return this.second - o.second;
	}
}
