package com.min.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RenXing {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		Text kText = new Text("key");
		Text vText = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			vText.set(split[1] + " " + split[2] + " " + split[3]);
			context.write(kText, value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long sum1 = 0;
			long sum2 = 0;
			long sum3 = 0;
			for (Text text : arg1) {
				String[] split = text.toString().split(" ");
				sum1 += Integer.valueOf(split[1]);
				sum2 += Integer.valueOf(split[2]);
				sum3 += Integer.valueOf(split[3]);
			}
			vText.set(sum1 + " " + sum2 + " " + sum3);
			arg2.write(kText, vText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/test/renxing.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/test/renxing"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
