package com.min.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	// 关联
	public final static Configuration con = new Configuration();
	// 文件系统
	public FileSystem hSystem;

	public HdfsTest() {
		// TODO Auto-generated constructor stub
		con.setBoolean("dfs.support.append", true);
		con.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		con.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		try {
			hSystem = FileSystem.get(con);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void creatFile(String path) {
		Path p = new Path(path);
		try {
			FSDataOutputStream outputStream = hSystem.create(p);
			outputStream.write("hellow".getBytes());
			outputStream.writeUTF("world");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void atppend(String path) {
		Path path2 = new Path(path);
		try {
			FSDataOutputStream outputStream = hSystem.append(path2);
			outputStream.writeUTF("热死热了");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void read(String path) {
		Path path2 = new Path(path);
		try {
			FSDataInputStream inputStream = hSystem.open(path2);
			InputStreamReader reader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line = bufferedReader.readLine();
			while (line != null) {
				System.out.println(line);
				line = bufferedReader.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void listHdfsDir(String path) {
		Path path2 = new Path(path);
		try {
			if (hSystem.exists(path2)) {
				if (hSystem.isDirectory(path2)) {
					System.out.println(path2);
					FileStatus[] status = hSystem.listStatus(path2);
					for (FileStatus fileStatus : status) {
						// 如果是根路径，递归
						if ("/".equals(path)) {
							listHdfsDir(path + fileStatus.getPath().getName());
						} else {
							listHdfsDir(path + "/" + fileStatus.getPath().getName());
						}
					}
				} else {
					System.out.println(path2.toString());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void delete(String path) {
		try {
			hSystem.delete(new Path(path), true);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void downLoad(String des, String src) {
		try {
			hSystem.copyToLocalFile(new Path(src), new Path(des));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void upLoad(String des, String src) {
		try {
			hSystem.copyFromLocalFile(new Path(src), new Path(des));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HdfsTest hTest = new HdfsTest();
		String path = "/hellow";
		// hTest.creatFile(path);
		// hTest.atppend(path);
		// hTest.read(path);
		// hTest.listHdfsDir("/");
		// hTest.delete(path);
		//hTest.downLoad("D:\\receive","/hellows");
		hTest.upLoad("/","D:\\receive\\opo");
		try {
			hTest.hSystem.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}