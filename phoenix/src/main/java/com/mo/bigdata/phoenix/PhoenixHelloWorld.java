package com.mo.bigdata.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

public class PhoenixHelloWorld {
	public static void run() throws SQLException, ClassNotFoundException {
		// Create variables
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			// Connect to the database
			connection = DriverManager.getConnection("jdbc:phoenix:node1");

			// Create a JDBC statement
			statement = connection.createStatement();

			// Execute our statements
			statement.executeUpdate("create table javatest (mykey integer not null primary key, mycolumn varchar)");
			statement.executeUpdate("upsert into javatest values (1,'Hello')");
			statement.executeUpdate("upsert into javatest values (2,'Java Application')");
			connection.commit();

			// Query for table
			ps = connection.prepareStatement("select * from javatest");
			rs = ps.executeQuery();
			System.out.println("Table Values");
			while (rs.next()) {
				Integer myKey = rs.getInt(1);
				String myColumn = rs.getString(2);
				System.out.println("\tRow: " + myKey + " = " + myColumn);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		}
	}

	public void select() {
		// Create variables
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		// Connect to the database
		try {
			connection = DriverManager.getConnection("jdbc:phoenix:node1");
			// Create a JDBC statement
			statement = connection.createStatement();
			// Query for table
			ps = connection.prepareStatement("select * from javatest");
			rs = ps.executeQuery();
			System.out.println("Table Values");
			while (rs.next()) {
				Integer myKey = rs.getInt(1);
				String myColumn = rs.getString(2);
				System.out.println("\tRow: " + myKey + " = " + myColumn);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
}
