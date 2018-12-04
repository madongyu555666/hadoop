package org.hadoop.hdfs.api;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 删除文件夹
 * 
 * @author
 *
 */
public class DeleteFile {

	public static void main(String[] args) throws IOException, InterruptedException {
		String uri = "hdfs://47.107.182.164:9000/tmp";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf, "root"); 
		// 参数true表示递归删除文件夹及文件夹下的文件 boolean b =	
		 boolean b = fs.delete(new Path(uri), true);																
		 System.out.println(b);
		 fs.close();
	}
}
