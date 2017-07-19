package com.min.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//读
public class SmallFileR {
	private static final String FIELD_FILENAME = "filename";
	private static final String FIELD_CONTENTS = "filecontent";

	public static void readFromAvro(InputStream iStream) throws IOException {
		DataFileStream<Object> reader = new DataFileStream<Object>(iStream, new GenericDatumReader<Object>());
		// 获取读取内容
		for (Object object : reader) {
			GenericRecord record = (GenericRecord) object;
			// 通过key获取对应内容
			System.out.println(record.get(FIELD_FILENAME));
			ByteBuffer buffer = ((ByteBuffer) record.get(FIELD_CONTENTS));
			System.out.println(new String(buffer.array()));
			System.out.println(record.get(FIELD_CONTENTS));
		}
		IOUtils.cleanup(null, reader);
		IOUtils.cleanup(null, iStream);
	}

	public static void main(String[] args) throws IOException {
		FileSystem system = FileSystem.get(new Configuration());
		FSDataInputStream iStream = system.open(new Path("/avro"));
		readFromAvro(iStream);
		system.close();
	}
}
