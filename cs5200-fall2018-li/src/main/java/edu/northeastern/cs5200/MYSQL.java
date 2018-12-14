package edu.northeastern.cs5200;

import java.sql.DriverManager;
import java.sql.SQLException;

public class MYSQL {
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String ADDRESS = "cs5200-fall2018-li.cr9kumsixkka.us-east-2.rds.amazonaws.com";
	private static final String PORT = "3306";
	public static final String DEFAULT_SCHEMA = "extra_assignment";
	private static final String USER = "li";	
	private static final String PASSWORD = "cs5200lmq";
	
	public static final String JDBC_ADDRESS = String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s", ADDRESS, PORT, DEFAULT_SCHEMA, USER, PASSWORD);

	private static final String URL = "jdbc:mysql://cs5200-fall2018-li.cr9kumsixkka.us-east-2.rds.amazonaws.com/extra_assignment";

	public static java.sql.Connection getConnection() throws ClassNotFoundException, SQLException { Class.forName(DRIVER);
	return DriverManager.getConnection(URL, USER, PASSWORD);
	}
}
