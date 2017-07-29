package com.min.HaDoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test2 {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			kText.set(split[0]+" "+split[2]);
			vText.set(split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5]);
			context.write(kText, vText);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Text vText = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (Text text : arg1) {
				String[] split = text.toString().split(" ");
				sum += Integer.valueOf(split[2]);
			}
			vText.set(sum + "");
			arg2.write(arg0, vText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "eee");
		job.setJarByClass(Test2.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/test/out3");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileOutputFormat.setOutputPath(job, name);
		FileInputFormat.addInputPath(job, new Path("/test/test.txt"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
