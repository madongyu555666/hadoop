package org.hadoop.hdfs.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 上传目录
 * 
 * @author
 */
public class CopyFromLocalFile {

	public static void main(String[] args) throws IOException, InterruptedException {
		String hdfsPath = "hdfs://47.107.182.164:9000/user/root/input";
		String localPath = "D:\\input";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf, "root");
		Path hdfs_path = new Path(hdfsPath);
		Path local_path = new Path(localPath);
		fs.copyFromLocalFile(local_path, hdfs_path);
		fs.close();
	}
}
