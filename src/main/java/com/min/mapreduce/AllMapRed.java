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

import com.min.entity.A;

public class AllMapRed {
	public static class AMap extends Mapper<Object, Text, Text, A> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, A>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();

			String[] split = string.split("\t");
			Text mKey = new Text(split[3]);
			A a = new A();
			a.setDownPackNum(Long.valueOf(split[7]));
			a.setUpPackNum(Long.valueOf(split[6]));
			a.setUpPayLoad(Long.valueOf(split[8]));
			a.setDownPayLoad(Long.valueOf(split[9]));
			a.setPhone(split[1]);
			context.write(mKey, a);
			System.out.println(a);
		}
	}

	public static class AReduce extends Reducer<Text, A, Text, A> {
		long upPackNums = 0;
		long downPackNums = 0;
		long upPayLoads = 0;
		long downPayLoads = 0;
		String phones = null;

		@Override
		protected void reduce(Text arg0, Iterable<A> arg1, Reducer<Text, A, Text, A>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (A a0 : arg1) {
				System.out.println(a0);
				upPackNums += a0.getUpPackNum();
				downPackNums += a0.getDownPackNum();
				upPayLoads += a0.getUpPayLoad();
				downPayLoads += a0.getDownPayLoad();
				phones = a0.getPhone();
			}
			A aa = new A();
			aa.setDownPackNum(downPackNums);
			aa.setUpPackNum(upPackNums);
			aa.setUpPayLoad(upPayLoads);
			aa.setDownPayLoad(downPayLoads);
			aa.setPhone(phones);
			arg2.write(arg0, aa);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration,"opop");
		job.setJarByClass(AllMapRed.class);
		
		job.setMapperClass(AMap.class);
		job.setCombinerClass(AReduce.class);
		job.setReducerClass(AReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(A.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(A.class);

		FileInputFormat.addInputPath(job, new Path("/yi.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/al"));
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
