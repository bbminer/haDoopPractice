package com.min.HaDoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.min.entity.Max;

public class Base {

	public static class Map12 extends Mapper<Object, Text, Text, Max> {
		Text keys = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Max>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputSplit = context.getInputSplit();
			String name = ((FileSplit) inputSplit).getPath().getName();
			String[] split = value.toString().split("\t");
			keys.set(split[0]);
			Max record = new Max();
			record.setRecordId(split[0]);
			if (name.contains("record")) {
				record.setHospitalId(split[1]);
				record.setDiseaseId(split[2]);
				record.setDepartmentId(split[3]);
				record.setDoctorId(split[4]);
				record.setFlag(Integer.valueOf(split[5]));
				record.setStartTime(split[6]);
				record.setEndTime(split[7]);
				record.setAllCost(Float.valueOf(split[8]));
				record.setIsRecovery(Integer.valueOf(split[9]));
			} else if (name.contains("reimburse")) {
				record.setReimburseTime(split[1]);
				record.setReCost(Float.valueOf(split[2]));
			}
			context.write(keys, record);
		}
	}

	public static class reduce1s extends Reducer<Text, Max, Text, Text> {
		float sum = 0.0f;
		Text tes = new Text();
		Text te = new Text();

		@Override
		protected void reduce(Text key, Iterable<Max> value, Reducer<Text, Max, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Max maxs = null;
			for (Max max : value) {
				sum += max.getReCost();
				maxs = max;
				System.out.println(max);
			}
			tes.set(String.format("%d", maxs.getFlag()));
			StringBuilder builder = new StringBuilder();
			builder.append(" ");
			builder.append(maxs.getHospitalId());
			builder.append(" ");
			builder.append(maxs.getDiseaseId());
			builder.append(" ");
			builder.append(maxs.getDepartmentId());
			builder.append(" ");
			builder.append(maxs.getDoctorId());
			builder.append(" ");
			builder.append(maxs.getFlag());
			builder.append(" ");
			builder.append(maxs.getStartTime());
			builder.append(" ");
			builder.append(maxs.getEndTime());
			builder.append(" ");
			builder.append(maxs.getAllCost());
			builder.append(" ");
			builder.append(maxs.getIsRecovery());
			builder.append(" ");
			builder.append(maxs.getReimburseTime());
			builder.append(" ");
			builder.append(sum);
			te.set(builder.toString());
			context.write(tes, te);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "eee");
		job.setJarByClass(Base.class);

		job.setMapperClass(Map12.class);
		// job.setCombinerClass(reduce1s.class);
		job.setReducerClass(reduce1s.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Max.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/out");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileOutputFormat.setOutputPath(job, name);
		FileInputFormat.addInputPath(job, new Path("/thumbs/"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
