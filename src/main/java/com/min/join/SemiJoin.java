package com.min.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.min.entity.Record;

//半连接
public class SemiJoin {

	public static class M extends Mapper<Object, Text, Text, Record> {
		Set<String> keySet = new HashSet<String>();
		Text kText = new Text();
		Text vText = new Text();

		@Override
		protected void setup(Mapper<Object, Text, Text, Record>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			URI[] cacheFiles = context.getCacheFiles();
			String joinKey = "";
			for (URI uri : cacheFiles) {
				if (uri.toString().contains("record.txt")) {
					FileSystem fileSystem = FileSystem.get(context.getConfiguration());
					FSDataInputStream stream = fileSystem.open(new Path(uri.getPath()));
					InputStreamReader iReader = new InputStreamReader(stream);
					BufferedReader bReader = new BufferedReader(iReader);
					while ((joinKey = bReader.readLine()) != null) {
						String[] split = joinKey.split("\t");
						keySet.add(split[0]);
					}
				}
			}
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Record>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String name = ((FileSplit) context.getInputSplit()).getPath().getName();
			Record record = new Record();
			String[] split = value.toString().split("\t");
			// 设置标志
			if (name.contains("reimburse.txt")) {
				if (keySet.toString().contains(split[0])) {
					record.setIndex(2);
				}

			} else if (name.contains("record.txt")) {
				if (keySet.toString().contains(split[0])) {
					record.setIndex(1);
				}
			}

			record.setRecordId(split[0]);
			StringBuilder builder = new StringBuilder();
			for (int i = 1, len = split.length; i < len; i++) {
				builder.append(split[i]);
				builder.append(" ");
			}
			kText.set(split[0]);
			builder.deleteCharAt(builder.length() - 1);
			record.setValue(builder.toString());
			context.write(kText, record);
		}
	}

	public static class R extends Reducer<Text, Record, Text, Text> {
		List<String> list1 = new ArrayList<String>();
		List<String> list2 = new ArrayList<String>();
		Text te = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Record> arg1, Reducer<Text, Record, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			list1.clear();
			list2.clear();
			for (Record record : arg1) {
				if (record.getIndex() == 1) {
					list1.add(record.getValue());
				}
				if (record.getIndex() == 2) {
					list2.add(record.getValue());
				}
			}
			for (String record1 : list1) {
				for (String ss : list2) {
					te.set(record1 + " " + ss);
					arg2.write(arg0, te);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(SemiJoin.class);

		job.addCacheFile(new Path("/thumbs/record.txt").toUri());
		job.setMapperClass(M.class);
		job.setReducerClass(R.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/SemiJoin");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileInputFormat.addInputPath(job, new Path("/thumbs/"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
