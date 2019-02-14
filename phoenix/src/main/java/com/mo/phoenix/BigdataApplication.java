package com.mo.phoenix;

import java.sql.SQLException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.mo.bigdata.phoenix.PhoenixHelloWorld;

@SpringBootApplication
public class BigdataApplication {

	public static void main(String[] args) throws SQLException {
		SpringApplication.run(BigdataApplication.class, args);
		
	}

}

