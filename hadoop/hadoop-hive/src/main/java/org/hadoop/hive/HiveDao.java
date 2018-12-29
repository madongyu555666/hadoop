package org.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDao {

	// hive的jdbc驱动类
	private static String dirverName = "org.apache.hive.jdbc.HiveDriver";
	// 连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive
	private static String url = "jdbc:hive2://120.78.181.181:10000/default";
	// 登录linux的用户名 一般会给权限大一点的用户，否则无法进行事务形操作
	private static String user = "root";
	// 登录linux的密码
	private static String password = "123456";
	private Connection connection;
	private PreparedStatement ps;
	private Statement st;
	private ResultSet rs;

	public void getConnection() {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			connection = DriverManager.getConnection("jdbc:hive2://120.78.181.181:10000/", user, password);
			System.out.println(connection);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void getConnection(String ip) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			connection = DriverManager.getConnection("jdbc:hive2://" + ip + ":10000/", user, password);
			System.out.println(connection);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void getConnection(String ip, String db) {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("jdbc:hive2://").append(ip).append(":10000/").append(db);
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			connection = DriverManager.getConnection(sb.toString(), user, password);
			System.out.println(connection);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createDatabase(String dbName) {
		try {
			String sql = "create database if not exists " + dbName;
			ps = connection.prepareStatement(sql);
			// true if the first result is a ResultSet object;
			// false if the first result is an update count or there is no result
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String sql) {
		try {
			ps = connection.prepareStatement(sql);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void dropTable(String tableName) {
		String sql = "drop table if exists " + tableName;
		try {
			st = connection.createStatement();
			st.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 添加数据
	public boolean load(String loadData) {
		try {
			ps = connection.prepareStatement(loadData);
			return ps.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block e.printStackTrace();
			return false;
		}
	}

	public ResultSet query(String sql) {
		try {
			ps = connection.prepareStatement(sql);
			return ps.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void descTables(String tableName) {
		String sql = "desc " + tableName;
		try {
			st = connection.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// 关闭连接
	public void close() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HiveDao dao = new HiveDao();
		dao.getConnection("120.78.181.181");
		//dao.createDatabase("mydb");
		//dao.dropTable("mydb.goods");
		//dao.createTable("create table mydb.goods(id int,name string) row format delimited fields terminated by '\t'");
		//dao.load("load data inpath 'input/goods.txt' into table mydb.goods");
		//dao.descTables("mydb.goods");
		ResultSet rs = dao.query("select * from mydb.goods");
		try {
			while (rs.next()) {
				System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		dao.close();

	}

}
