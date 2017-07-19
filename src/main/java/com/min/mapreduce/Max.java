package com.min.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Max {
	public static class MaxMap extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] split = string.split(" ");
			if (split.length == 2) {
				context.write(new Text(split[0]), new Text(split[1]));
			}
		}
	}

	public static class MaxRecude extends Reducer<Text, Text, Text, Text> {
		Text max = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text text : arg1) {
				int i = max.compareTo(text);
				if (i < 0) {
					max.set(text.toString());
				}
			}
			arg2.write(arg0, max);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "max");
		job.setJarByClass(Max.class);
		job.setMapperClass(MaxMap.class);
		job.setCombinerClass(MaxRecude.class);
		job.setReducerClass(MaxRecude.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/test1.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/max"));
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
