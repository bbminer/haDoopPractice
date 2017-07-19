package com.min.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//左连接
public class MapJoin {
	public static class M extends Mapper<Object, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();
		Map<String, String> info = new HashMap<String, String>();

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 从缓存中拿到所有文件的路径
			URI[] disPath = context.getCacheFiles();
			for (URI uri : disPath) {
				// 找到小表
				if (uri.toString().contains("record.txt")) {
					FileSystem hdFileSystem = FileSystem.get(context.getConfiguration());
					// 获取流
					FSDataInputStream open = hdFileSystem.open(new Path(uri.toString()));
					InputStreamReader iReader = new InputStreamReader(open, "UTF-8");
					// 读取缓存，放进内存
					BufferedReader brReader = new BufferedReader(iReader);
					String line = null;
					while ((line = brReader.readLine()) != null) {
						// 分割
						String[] split = line.split("\t");
						if (split.length == 10) {
							StringBuilder builder = new StringBuilder();
							for (int i = 1, len = split.length; i < len; i++) {
								builder.append(split[i]);
								builder.append(" ");
							}
							builder.deleteCharAt(builder.length() - 1);
							// 放进map
							info.put(split[0], builder.toString());
						}
					}
				}
			}
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split("\t");
			if (split.length == 3) {
				k.set(split[0]);
				String string = info.get(split[0]);
				v.set(split[1] + "\t" + split[2] + "\t" + string);
				context.write(k, v);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		//只能传入文件的路径
		job.addCacheFile(new Path("/thumbs/record.txt").toUri());
		job.setJarByClass(MapJoin.class);

		job.setMapperClass(M.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/outjoin");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileInputFormat.addInputPath(job, new Path("/thumbs/reimburse.txt"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
