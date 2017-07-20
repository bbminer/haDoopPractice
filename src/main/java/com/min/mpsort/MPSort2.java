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
import com.min.entity.SortEntity;

//二次排序，需要排序的数据有两列
//自定义排序规则
public class MPSort2 {
	public static class M extends Mapper<Object, Text, SortEntity, IntWritable> {
		IntWritable vvalue = new IntWritable();
		IntWritable kkey = new IntWritable();
		int i = 0;

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, SortEntity, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split(" ");
			SortEntity sortEntity = new SortEntity();
			sortEntity.setFirst(Integer.valueOf(split[0]));
			sortEntity.setSecond(Integer.valueOf(split[1]));
			kkey.set(++i);
			context.write(sortEntity, kkey);
		}
	}

	public static class R extends Reducer<SortEntity, IntWritable, Text, Text> {
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void reduce(SortEntity arg0, Iterable<IntWritable> arg1,
				Reducer<SortEntity, IntWritable, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			kText.set(String.format("%d %d", arg0.getFirst(),arg0.getSecond()));
			arg2.write(kText, vText);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MPSort2.class);
		job.setMapperClass(M.class);

		job.setReducerClass(R.class);

		job.setMapOutputKeyClass(SortEntity.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem system = FileSystem.get(configuration);
		Path path = new Path("/sort2");
		if (system.exists(path)) {
			system.delete(path, true);
		}
		FileInputFormat.addInputPath(job, new Path("/sort2.txt"));
		FileOutputFormat.setOutputPath(job, path);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
