package com.min.hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class I {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		FileSplit fileSplit;
		Text textK = new Text();
		Text textV = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			fileSplit = (FileSplit) context.getInputSplit();
			StringTokenizer sTokenizer = new StringTokenizer(value.toString());
			while (sTokenizer.hasMoreTokens()) {
				String word = sTokenizer.nextToken();
				String url = fileSplit.getPath().getName();
				textK.set(word + " " + url);
				textV.set("1");
				context.write(textK, textV);
			}
		}
	}

	// 本地的reduce，二者效果相同，是对reduced的优化,在本地不需要网络IO的reduce
	public static class Com extends Reducer<Text, Text, Text, Text> {
		Text kText = new Text();
		Text vText = new Text();


		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (Text text : arg1) {
				sum += Integer.valueOf(text.toString());
			}
			String[] data = arg0.toString().split(" ");
			kText.set(data[0]);
			vText.set(data[1] + "[" + sum + "]");
			arg2.write(kText, vText);
		}
	}

	public static class redce extends Reducer<Text, Text, Text, Text> {
		Text vText = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String s = "";
			for (Text text : arg1) {
				s += ("--" + text.toString());
			}
			vText.set(s);
			arg2.write(arg0, vText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(I.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Com.class);
		job.setReducerClass(redce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/min/"));
		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/in");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
