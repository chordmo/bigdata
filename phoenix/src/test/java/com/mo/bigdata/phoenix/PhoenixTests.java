package com.mo.bigdata.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

import java.sql.SQLException;

import org.junit.Test;

public class PhoenixTests {
	/**
	 * 
	 * Download jar for:
	 * http://mirrors.shu.edu.cn/apache/phoenix/apache-phoenix-5.0.0-HBase-2.0/bin/apache-phoenix-5.0.0-HBase-2.0-bin.tar.gz
	 * 
	 * Add External JARs:
	 * phoenix-core-5.0.0-HBase-2.0-sources.jar
	 * phoenix-5.0.0-HBase-2.0-client.jar
	 */
	@Test
	public void run() {
		try {
			PhoenixHelloWorld.run();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void phoenix() {

		Statement stmt = null;
		ResultSet rset = null;

		Connection con;
		try {
			con = DriverManager.getConnection("jdbc:phoenix:node1");
			stmt = con.createStatement();
			
			
			
//			stmt.executeUpdate("create table testtest (mykey integer not null primary key, mycolumn varchar)");
			stmt.executeUpdate("upsert into testtest values (1,'Hello')");
			stmt.executeUpdate("upsert into testtest values (2,'World!')");
			con.commit();

			PreparedStatement statement = con.prepareStatement("select * from testtest");
			rset = statement.executeQuery();
			while (rset.next()) {
				System.out.println(rset.getString("mycolumn"));
			}
			
		
			
			statement.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
