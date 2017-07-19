package com.min.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Max implements Writable {

	private String recordId;
	private String hospitalId;
	private String diseaseId;
	private String departmentId;
	private String doctorId;
	private int flag;
	private String startTime;
	private String endTime;
	private float allCost;
	private int isRecovery;
	private String reimburseTime;
	private float reCost;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(recordId);
		out.writeUTF(hospitalId);
		out.writeUTF(diseaseId);
		out.writeUTF(departmentId);
		out.writeUTF(doctorId);
		out.writeInt(flag);
		out.writeUTF(startTime);
		out.writeUTF(endTime);
		out.writeFloat(allCost);
		out.writeInt(isRecovery);
		out.writeUTF(reimburseTime);
		out.writeFloat(reCost);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		recordId = in.readUTF();
		hospitalId = in.readUTF();
		diseaseId = in.readUTF();
		departmentId = in.readUTF();
		doctorId = in.readUTF();
		flag = in.readInt();
		startTime = in.readUTF();
		endTime = in.readUTF();
		allCost = in.readFloat();
		isRecovery = in.readInt();
		reimburseTime = in.readUTF();
		reCost = in.readFloat();
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

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getHospitalId() {
		return hospitalId;
	}

	public void setHospitalId(String hospitalId) {
		this.hospitalId = hospitalId;
	}

	public String getDiseaseId() {
		return diseaseId;
	}

	public void setDiseaseId(String diseaseId) {
		this.diseaseId = diseaseId;
	}

	public String getDepartmentId() {
		return departmentId;
	}

	public void setDepartmentId(String departmentId) {
		this.departmentId = departmentId;
	}

	public String getDoctorId() {
		return doctorId;
	}

	public void setDoctorId(String doctorId) {
		this.doctorId = doctorId;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public float getAllCost() {
		return allCost;
	}

	public void setAllCost(float allCost) {
		this.allCost = allCost;
	}

	public int getIsRecovery() {
		return isRecovery;
	}

	public void setIsRecovery(int isRecovery) {
		this.isRecovery = isRecovery;
	}

	@Override
	public String toString() {
		return "Max [reimburseTime=" + reimburseTime + ", reCost=" + reCost + ", recordId=" + recordId + ", hospitalId="
				+ hospitalId + ", diseaseId=" + diseaseId + ", departmentId=" + departmentId + ", doctorId=" + doctorId
				+ ", flag=" + flag + ", startTime=" + startTime + ", endTime=" + endTime + ", allCost=" + allCost
				+ ", isRecovery=" + isRecovery + "]";
	}

}
