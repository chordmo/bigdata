package com.example.hive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;

@SpringBootApplication
public class DemoApplication {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {


		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		//replace "hive" here with the name of the user the queries should run as
		Connection con = DriverManager.getConnection("jdbc:hive2://node1:10000/default", "bigdata", "passw0rd");
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (key int, value string)");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line



//		echo -e '1\x01foo' > /tmp/a.txt;echo -e '2\x01bar' >> /tmp/a.txt
//
		String filepath = "/tmp/a.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}

		// regular hive query
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}



		SpringApplication.run(DemoApplication.class, args);

	}

}
