package com.min.mpsort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//二次排序，需要排序的数据有两列
//自定义排序规则
public class MPSort3 {

	// 重写分区策略
	/* 默认的分区策略：当前的key的hash值的模%reduce的分区总数 */
	// key被写成整体，reduce变得没有意义，因此重写策略，将实体类的第一个值作为分区的key
	public static class FirstPartitioner extends Partitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			String[] split = key.toString().split(" ");
			int keyNum = Integer.valueOf(split[0]);
			return keyNum % numPartitions;
		}
	}

	public static class M extends Mapper<Object, Text, Text, IntWritable> {
		Text vvalue = new Text();
		IntWritable kkey = new IntWritable();
		int i = 0;

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			vvalue.set(split[0] + " " + split[1]);
			kkey.set(++i);
			context.write(vvalue, kkey);
		}
	}

	public static class R extends Reducer<Text, IntWritable, Text, Text> {
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, Text>.Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			arg2.write(arg0, vText);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MPSort3.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(M.class);
		job.setReducerClass(R.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem system = FileSystem.get(configuration);
		Path path = new Path("/sort3");
		if (system.exists(path)) {
			system.delete(path, true);
		}
		FileInputFormat.addInputPath(job, new Path("/sort2.txt"));
		FileOutputFormat.setOutputPath(job, path);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
