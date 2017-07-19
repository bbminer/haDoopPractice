package com.min.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Chain {
	public static class Map1 extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			Integer of = Integer.valueOf(split[1]);
			if (of < 10000) {
				context.write(new Text(split[0]), new IntWritable(of));
			}
		}
	}

	public static class Map2 extends Mapper<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (value.get() > 100) {
				context.write(key, value);
			}
		}
	}

	public static class reduces extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable intWritable : arg1) {
				arg2.write(arg0, intWritable);
			}
		}
	}

	public static class Map3 extends Mapper<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (key.toString().length() <= 3) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(Chain.class);

		ChainMapper.addMapper(job, Map1.class, Object.class, Text.class, Text.class, IntWritable.class, configuration);
		ChainMapper.addMapper(job, Map2.class, Text.class, IntWritable.class, Text.class, IntWritable.class,
				configuration);
		ChainReducer.setReducer(job, reduces.class, Text.class, IntWritable.class, Text.class, IntWritable.class,
				configuration);
		ChainReducer.addMapper(job, Map3.class, Text.class, IntWritable.class, Text.class, IntWritable.class,
				configuration);

		FileInputFormat.addInputPath(job, new Path("/sss.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/al0"));
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
