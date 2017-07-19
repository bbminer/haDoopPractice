package com.min.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//小文件写入
public class SmallFileW {
	private static final String FIELD_FILENAME = "filename";
	private static final String FIELD_CONTENTS = "filecontent";
	// 定义avre
	// 模式定义
	private static final String SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"SmallFW\",\"fields\":[{\"name\":\""
			+ FIELD_FILENAME + "\",\"type\":\"string\"},{\"name\":\"" + FIELD_CONTENTS
			+ "\",\"type\":\"bytes\"}]}";
	// 创建模式
	private static Schema.Parser parser = new Schema.Parser();
	// 解析模式
	private static final Schema SCHEMA = parser.parse(SCHEMA_JSON);

	// 合并小文件
	public static void writeToAvro(File filePath, OutputStream oStream) throws IOException {
		// 以DataFileWriter方式写入
		DataFileWriter<Object> writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
		// 设置文件写入编码格式
		writer.setCodec(CodecFactory.snappyCodec());
		// 加载schema
		writer.create(SCHEMA, oStream);
		// 遍历给定文件夹下的所有小文件
		for (Object object : FileUtils.listFiles(filePath, null, false)) {
			File file = (File) object;
			String name = file.getAbsolutePath();
			// 获取文件内容
			byte[] bs = FileUtils.readFileToByteArray(file);
			// 创建record记录，赋值
			GenericRecord record = new GenericData.Record(SCHEMA);
			record.put(FIELD_FILENAME, name);
			// 写入record
			record.put(FIELD_CONTENTS, ByteBuffer.wrap(bs));
			writer.append(record);
		}
		writer.close();
	}

	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();
		FileSystem system = FileSystem.get(configuration);
		Path path = new Path("/avro");
		if (system.exists(path)) {
			system.delete(path, true);
		}
		OutputStream stream = system.create(path);
		// file是从本地的的文件
		writeToAvro(new File("E:\\2"), stream);
	}
}
