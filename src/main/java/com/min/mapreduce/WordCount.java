package com.min.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	// map:切割
	// reduce：聚合
	/**
	 * @author Administrator map类 继承
	 *         Mapper，<object:map的读入key的类型，Text:map的读入value的类型，
	 *         Text:map的写出key的类型，IntWritable:map的写出value的类型> map方法
	 *         作用：可以一行一行去读文本内容，根据需求分割文档
	 */
	public static class WcMap extends Mapper<Object, Text, Text, IntWritable> {
		Text t = new Text();
		IntWritable it = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 切割内容
			StringTokenizer sTokenizer = new StringTokenizer(value.toString());
			while (sTokenizer.hasMoreTokens()) {
				t.set(sTokenizer.nextToken());
				context.write(t, it);
			}
		}
	}

	// reduce聚合过程：相同key的键值对放在一起计数
	public static class McRecude extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable intWritable : value) {
				sum += intWritable.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WcMap.class);
		job.setCombinerClass(McRecude.class);
		job.setReducerClass(McRecude.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/hellows"));
		FileOutputFormat.setOutputPath(job, new Path("/o"));
		//系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}