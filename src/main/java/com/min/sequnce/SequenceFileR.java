package com.min.sequnce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileR {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		FileSystem system = FileSystem.get(c);
		Option file = SequenceFile.Reader.file(new Path("/sequenceW"));
		SequenceFile.Reader reader = new SequenceFile.Reader(c, file);
		// 通过头信息获取key和value
		// newInstance 创建key、value的类型
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), c);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), c);
		while (reader.next(key, value)) {
			System.out.println(key + ":" + value);
		}
		reader.close();
		system.close();
	}
}
