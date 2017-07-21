package com.min.mpsort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MPSort4 {
	// 重写mr的分区策略
	// 默认的分区策略：当前key的hash值的摸%reduce的分区总数
	// 原来是按照发过来的key值%reduce分区数得到每个reduce获取的map值
	// 但是，在二次排序中，key被写成一个整体，那么
	// reduce分区就不能把第一个值相同的放进一个reduce里面（reduce没有意义）
	// 所以，重写reduce的分区策略，将map发送过来的key拆解
	// 拿到里面的第一个数值
	// 重写分区策略
	public static class GroupCompare implements RawComparator<Text> {

		public int compare(Text o1, Text o2) {
			String[] data = o1.toString().split(" ");
			String[] data1 = o2.toString().split(" ");
			int first = Integer.valueOf(data[0]);
			int first1 = Integer.valueOf(data1[0]);

			System.out.println("o(︶︿︶)o 唉   1");
			return first - first1;
		}

		// 一个字节一个字节的比直到找到一个不同的字节对比取值
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			System.out.println("o(︶︿︶)o 唉   2");
			return 0;
		}

	}

	public static class FirstPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			System.out.println("o(︶︿︶)o 唉   3");
			String[] data = key.toString().split(" ");
			int keynum = Integer.valueOf(data[0]);
			return keynum % numPartitions;
		}

	}

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String[] data = value.toString().split(" ");
		//	IntPair intPair = new IntPair();
			/*
			 * intPair.setFirst(Integer.parseInt(data[0]));
			 * intPair.setSecond(Integer.parseInt(data[0]+" "+data[1]));
			 */
			context.write(new Text(data[0] + " " + data[1]), new Text(data[1]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			for (Text e : arg1) {
				arg2.write(arg0, e);
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path("/cout3"), true);
		Job job = Job.getInstance(conf);
		// 默认k%reduce分区数决定的
		// 决定map里面的key怎样分到reduced
		job.setNumReduceTasks(2);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupCompare.class);
		job.setJarByClass(MPSort4.class);
		job.setJobName("SecondSort");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/sort2.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/sort4"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
