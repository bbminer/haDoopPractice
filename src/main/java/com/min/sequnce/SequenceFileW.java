package com.min.sequnce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

public class SequenceFileW {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		FileSystem system = FileSystem.get(c);
		// 输出路径
		Path outP = new Path("/sequenceW");
		// 指定文本内容<k,v>
		IntWritable key = new IntWritable();
		Text value = new Text();
		Option file = SequenceFile.Writer.file(outP);
		Option keyClass = SequenceFile.Writer.keyClass(IntWritable.class);
		Option valueClass = SequenceFile.Writer.valueClass(Text.class);
		SequenceFile.Writer writer = SequenceFile.createWriter(c, file, keyClass, valueClass);
		for (int i = 0; i < 10; i++) {
			key.set(i);
			value.set("l:" + i);
			writer.append(key, value);
		}
		system.close();
	}
}
