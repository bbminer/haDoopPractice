package com.min.mpsort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MPSort {
	public static class M extends Mapper<Object, Text, IntWritable, IntWritable> {
		IntWritable vvalue = new IntWritable();
		IntWritable kkey = new IntWritable();
		int i = 0;

		// 输入一行文本数据
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			vvalue.set(Integer.valueOf(string));
			kkey.set(++i);
			context.write(vvalue, kkey);
		}
	}

	// 输出排序的结果，和在原数据中的次序
	public static class R extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable intWritable : arg1) {
				arg2.write(arg0, intWritable);
			}
		}
	}

	// 输出排序的结果，和排名名次
	public static class R1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		int index = 1;
		IntWritable i = new IntWritable();

		@SuppressWarnings("unused")
		@Override
		protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0;
			for (IntWritable intWritable : arg1) {
				count++;
				i.set(index);
				arg2.write(arg0, i);
			}
			index += count;
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "count");
		job.setJarByClass(MPSort.class);
		job.setMapperClass(M.class);

		job.setReducerClass(R.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem system = FileSystem.get(configuration);
		Path path = new Path("/sort");
		if (system.exists(path)) {
			system.delete(path, true);
		}
		FileInputFormat.addInputPath(job, new Path("/sort.txt"));
		FileOutputFormat.setOutputPath(job, path);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
