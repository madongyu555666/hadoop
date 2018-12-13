package org.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;

public class API {
	
	
	/**
	 * 初始化
	 */
	public static Connection init()throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
		// 设置Zookeeper,直接设置IP地址
        conf.set("hbase.zookeeper.quorum","47.107.182.164,120.78.181.181");
        //建立连接
        Connection  connection = ConnectionFactory.createConnection(conf);
        return connection;
	}
	
	/**
	 * 创建表的方法
	 * @throws IOException 
	 */
	public static  void create(Connection connection,String name,String columnFamily) throws IOException {
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf(name);
		if(admin.tableExists(tableName)) {
			 System.out.println("Table exists!");                    
	         System.exit(0);
		}else {
		   //定义表结构
           HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(name));
           //添加列族
           tableDesc.addFamily(new HColumnDescriptor(columnFamily));
           //创建表
           admin.createTable(tableDesc);
           System.out.println("create table success!");
		}
		 admin.close();
	     connection.close();
		
	}
	/**
	 * 插入数据
	 * @param connection
	 * @param name
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws IOException
	 */
     public static void add(Connection connection,String name,String family,String qualifier,String value) throws IOException{
    	 Table  table = connection.getTable(TableName.valueOf(name));
    	//通过rowKey实例化Put
    	 Put put = new Put(Bytes.toBytes("001"));
    	 //指定列族名、列名和值
    	 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
         //执行put
    	 table.put(put);
    	 //关闭表和连接
         table.close();
         connection.close();
         System.out.println("ok!");
     }
	
     
     /**
      * 读取
     * @throws IOException 
      */
     public static void read(Connection connection,String name,String family,String qualifier) throws IOException {
    	 Table  table = connection.getTable(TableName.valueOf(name));
    	 //通过rowKey实例化Get
         Get get = new Get(Bytes.toBytes("001"));
         get.addColumn(family.getBytes(), qualifier.getBytes());
       //执行Get，返回结果
         Result result=table.get(get);
         String value=Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes()));
         System.out.println("value="+value);
         //关闭表和连接
         table.close();
         connection.close();
     }
	
	/**
	   *扫描 
	 * @throws IOException 
	 */
	public static void scan(Connection connection,String name,String family,String qualifier) throws IOException {
		 Table  table = connection.getTable(TableName.valueOf(name));
		 //初始化SCan
		 Scan scan =new Scan (); 
		//指定开始的rowKey
		// scan.setStartRow("001".getBytes());
		//指定结束的rowKey
		 //scan.setStopRow("005".getBytes());
		 scan.addColumn(family.getBytes(), qualifier.getBytes());
		 //执行scan返回结果
	     ResultScanner result=table.getScanner(scan);
	     String value;
	     for (Result r : result) {
	    	 value=Bytes.toString(r.getValue(family.getBytes(), qualifier.getBytes()));
	    	 System.out.println("value="+value);
		}
	    //关闭表和连接
	    table.close();
	    connection.close();
	}
	
	/**
	 * 删除数据
	 * @throws IOException 
	 */
	public  static void delete(Connection connection,String name,String family,String qualifier,String number) throws IOException {
		Table  table = connection.getTable(TableName.valueOf(name));
		 //通过rowKey实例化Delete
        Delete delete=new Delete(Bytes.toBytes(number));
        delete.addColumn(family.getBytes(), qualifier.getBytes());
        table.delete(delete);
        table.close();
        connection.close();
        System.out.println("ok!");
	}
     
	
	/**
	 * 删除表
	 * @throws IOException 
	 */
	public static  void drop(Connection connection,String name) throws IOException {
		//表管理类
        Admin admin = connection.getAdmin();
        //定义表名
        TableName table = TableName.valueOf("test1");
		//现禁用
        admin.disableTable(table);
        //再删除
        admin.deleteTable(table);
        //关闭
        admin.close();
        connection.close();
        System.out.println("Successfully deleted data table！");
	}
	
	
	public static void main(String[] args) throws Exception {
		/*new API().create(connection,"test1","info");*/
		/*add(connection,"test1","info","name","hadron");*/
		Connection connection=init();
		//read(connection,"test1","info","name");
		//scan(connection,"test1","info","name");
		//delete(connection,"test1","info","name","001");
	}
	
}
