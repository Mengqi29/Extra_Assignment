package edu.northeastern.cs5200.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.northeastern.cs5200.MYSQL;
import edu.northeastern.cs5200.models.Data;

public class DataDao implements Dao<Data> {
	private static DataDao instance = null;

	public static DataDao getInstance() {
		if (instance == null) {
			instance = new DataDao();
		}
		return instance;
	}

	@Override
	public JSONArray getAll(String tableName) throws SQLException {
		// TODO Auto-generated method stub

//		Connection conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);

		// select the inserted row data from table
		Statement selectTableDataStmt;
		try {
			selectTableDataStmt = MYSQL.getConnection().createStatement();
		String selectTableDataSQL = String.format("SELECT * FROM %s", tableName);
		System.out.println(selectTableDataSQL);
		ResultSet selectTableDataRS = selectTableDataStmt.executeQuery(selectTableDataSQL);

		// table data JSONArray String
		String tableDataJSONString = "[";

		while (selectTableDataRS.next()) {
			ResultSetMetaData md = selectTableDataRS.getMetaData();
			int columnCount = md.getColumnCount();

			// row JSON string
			String rowJSONString = "{";

			for (int i = 1; i <= columnCount; i++) {
				String columnName = md.getColumnName(i);
				String columnValue = selectTableDataRS.getString(columnName);
				rowJSONString += columnName + ": " + columnValue;
				if (i != columnCount) {	
					rowJSONString += ",";
				}
			}

			rowJSONString += "}";

			System.out.println(rowJSONString);

			tableDataJSONString += rowJSONString;
			if (!selectTableDataRS.isLast()) {
				tableDataJSONString += ",";
			}
		}

		tableDataJSONString += "]";

		System.out.println(tableDataJSONString);

		selectTableDataRS.close();
		selectTableDataStmt.close();
		

		return new JSONArray(tableDataJSONString);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

	
	@Override
	public JSONObject save(String tableName, Data t) throws Exception {
		// TODO Auto-generated method stub

		Connection conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);

		Statement createTableStmt = conn.createStatement();

		// check whether the table exists, if not, create the table
		String createTableStatementSQL = String
				.format("CREATE TABLE IF NOT EXISTS %s(" + "id int PRIMARY KEY NOT NULL AUTO_INCREMENT);", tableName);
		System.out.println(createTableStatementSQL);
		createTableStmt.executeUpdate(createTableStatementSQL);

		// we declare two variables(insertColumns, insertValues) here to concatenate the
		// INSERT SQL statement
		// e.g. INSERT INTO table(insertColumns) VALUES(insertValues)
		String insertColumns = ""; // columns for insert
		String insertValues = ""; // values for insert

		// traverse through JSON keys which are table columns
		Iterator<String> it = t.json.keys();
		while (it.hasNext()) {
			String column = it.next();

			// check if column exists
			Statement checkColumnExistsStmt = conn.createStatement();
			String checkColumnExistsSQL = String.format(
					"SELECT count(1) as RESULT FROM INFORMATION_SCHEMA.COLUMNS " + "WHERE TABLE_SCHEMA=\"%s\" "
							+ "AND TABLE_NAME=\"%s\" " + "AND COLUMN_NAME=\"%s\";",
					MYSQL.DEFAULT_SCHEMA, tableName, column);
			System.out.println(checkColumnExistsSQL);
			ResultSet checkColumnExistsRS = checkColumnExistsStmt.executeQuery(checkColumnExistsSQL);

			while (checkColumnExistsRS.next()) {
				int columnCount = checkColumnExistsRS.getInt("Result");
				// if column does not exist, alter table to add the column
				if (columnCount == 0) {
					String addColumnSQL = String.format("ALTER TABLE %s " + "ADD COLUMN %s VARCHAR(100)", tableName,
							column);
					System.out.println(addColumnSQL);
					Statement addColumnStmt = conn.createStatement();
					addColumnStmt.execute(addColumnSQL);
					addColumnStmt.close();
				}
			}

			// memo the column and value
			insertColumns += column;
			insertValues += "\"" + t.json.getString(column) + "\"";
			if (it.hasNext()) {
				insertColumns += ",";
				insertValues += ",";
			}

			checkColumnExistsStmt.close();
			checkColumnExistsRS.close();
		}

		// Insert data
		String generatedColumns[] = { "ID" };
		String insertDataSQL = String.format("INSERT INTO %s(%s)" + "VALUES (%s);", tableName, insertColumns,
				insertValues);
		System.out.println(insertDataSQL);
		PreparedStatement insertDataStmt = conn.prepareStatement(insertDataSQL, generatedColumns);
		insertDataStmt.executeUpdate(insertDataSQL, Statement.RETURN_GENERATED_KEYS);
		

		// get the inserted row ID so that we can retrieve record using SELECT later
		ResultSet insertDataRS = insertDataStmt.getGeneratedKeys();
		String insertedRowPrimaryKey = null;
		if (insertDataRS.next()) {
			insertedRowPrimaryKey = insertDataRS.getString(1);
			insertDataRS.close();
			insertDataStmt.close();
		} else {
			insertDataRS.close();
			insertDataStmt.close();
			throw new Exception("Cannot get inserted row Primary Key, please try again later or contact DBA");
		}

		// select the inserted row data from table
		Statement selectInsertedRowDataStmt = conn.createStatement();
		String selectInsertedRowDataSQL = String.format("SELECT * FROM %s WHERE id=%s", tableName,
				insertedRowPrimaryKey);
		System.out.println(selectInsertedRowDataSQL);
		ResultSet selectInsertedRowDataRS = selectInsertedRowDataStmt.executeQuery(selectInsertedRowDataSQL);

		if (selectInsertedRowDataRS.next()) {
			ResultSetMetaData md = selectInsertedRowDataRS.getMetaData();
			int columnCount = md.getColumnCount();

			// generate result JSON
			String returnJSONString = "{";

			for (int i = 1; i <= columnCount; i++) {
				String columnName = md.getColumnName(i);
				String columnValue = selectInsertedRowDataRS.getString(columnName);
				returnJSONString += columnName + ": " + columnValue;
				if (i != columnCount) {
					returnJSONString += ",";
				}
			}

			returnJSONString += "}";

			selectInsertedRowDataRS.close();
			selectInsertedRowDataStmt.close();

			return new JSONObject(returnJSONString);
		} else {
			selectInsertedRowDataRS.close();
			selectInsertedRowDataStmt.close();

			throw new Exception("Cannot retrieve inserted row data from table. Please contact DBA.");
		}

	}


	@Override
	public JSONObject findByID(String tableName, String id) throws SQLException {
			
			Connection conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);
			Statement findByIdStmt = conn.createStatement();
			
			String findByIdSQL = String.format("SELECT * FROM %s where id = %s", tableName, id);
			
			ResultSet findByIdSQLRS = findByIdStmt.executeQuery(findByIdSQL);
			
			
			if(findByIdSQLRS.next()) { 
				ResultSetMetaData md = findByIdSQLRS.getMetaData();
				int columnCount = md.getColumnCount();

				// row JSON string
				String returnJSONString = "{";

				for (int i = 1; i <= columnCount; i++) {
					String columnName = md.getColumnName(i);
					String columnValue = findByIdSQLRS.getString(columnName);
					returnJSONString += columnName + ": " + columnValue;
					if (i != columnCount) {
						returnJSONString += ",";
					}
				}

				returnJSONString += "}";

				System.out.println(returnJSONString);

			return new JSONObject(returnJSONString);
			}else {
				findByIdSQLRS.close();
				findByIdStmt.close();
				
				throw new SQLException("Cannot retrieve data from table. Please contact DBA.");
			}
			
			
	}
	
	
	@Override
	public JSONObject get(String tableName, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void update(String tableName, String id, String[] params) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public JSONObject updateRecord(String tableName, String id, Data t) throws Exception{
		
		Connection conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);
		
		String insertColumns = ""; // columns for insert
		String insertValues = ""; // values for insert

		// traverse through JSON keys which are table columns
		Iterator<String> it = t.json.keys();
		while (it.hasNext()) {
			String column = it.next();

			// check if column exists
			Statement checkColumnExistsStmt = conn.createStatement();
			String checkColumnExistsSQL = String.format(
					"SELECT count(1) as RESULT FROM INFORMATION_SCHEMA.COLUMNS " + "WHERE TABLE_SCHEMA=\"%s\" "
							+ "AND TABLE_NAME=\"%s\" " + "AND COLUMN_NAME=\"%s\";",
					MYSQL.DEFAULT_SCHEMA, tableName, column);
			System.out.println(checkColumnExistsSQL);
			ResultSet checkColumnExistsRS = checkColumnExistsStmt.executeQuery(checkColumnExistsSQL);

			while (checkColumnExistsRS.next()) {
				int columnCount = checkColumnExistsRS.getInt("Result");
				// if column does not exist, alter table to add the column
				if (columnCount == 0) {
					String addColumnSQL = String.format("ALTER TABLE %s " + "ADD COLUMN %s VARCHAR(100)", tableName,
							column);
					System.out.println(addColumnSQL);
					Statement addColumnStmt = conn.createStatement();
					addColumnStmt.execute(addColumnSQL);
					addColumnStmt.close();
				}
			}

			// memo the column and value
			insertColumns += column;
			insertValues += "\"" + t.json.getString(column) + "\"";
			if (it.hasNext()) {
				insertColumns += ",";
				insertValues += ",";
			}

			checkColumnExistsStmt.close();
			checkColumnExistsRS.close();
		}

		// Insert data
		String updateDataSQL = String.format("UPDATE %s SET %s = %s WHERE id = %s ", tableName, insertColumns,
				insertValues,id);
		System.out.println(updateDataSQL);
//		PreparedStatement insertDataStmt = conn.prepareStatement(insertDataSQL, generatedColumns);
		PreparedStatement upidateDataStmt = conn.prepareStatement(updateDataSQL);
		upidateDataStmt.executeUpdate(updateDataSQL);
		

		
		// select the inserted row data from table
		Statement selectInsertedRowDataStmt = conn.createStatement();
		String selectInsertedRowDataSQL = String.format("SELECT * FROM %s WHERE id=%s", tableName, id);
		ResultSet selectInsertedRowDataRS = selectInsertedRowDataStmt.executeQuery(selectInsertedRowDataSQL);

		if (selectInsertedRowDataRS.next()) {
			ResultSetMetaData md = selectInsertedRowDataRS.getMetaData();
			int columnCount = md.getColumnCount();

			// generate result JSON
			String returnJSONString = "{";

			for (int i = 1; i <= columnCount; i++) {
				String columnName = md.getColumnName(i);
				String columnValue = selectInsertedRowDataRS.getString(columnName);
				returnJSONString += columnName + ": " + columnValue;
				if (i != columnCount) {
					returnJSONString += ",";
				}
			}

			returnJSONString += "}";

			selectInsertedRowDataRS.close();
			selectInsertedRowDataStmt.close();

			return new JSONObject(returnJSONString);
		} else {
			selectInsertedRowDataRS.close();
			selectInsertedRowDataStmt.close();

			throw new Exception("Cannot retrieve inserted row data from table. Please contact DBA.");
		}

	}

	@Override
	public void deleteRow(String tableName, String id){
		
		Connection conn;
		try {
			conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);
			Statement deleteByIdStmt = conn.createStatement();
		
			String deleteByIdSQL = String.format("DELETE FROM %s WHERE id = %s ", tableName, id);
		
			@SuppressWarnings("unused")
			int deleteByIdRS = deleteByIdStmt.executeUpdate(deleteByIdSQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void deleteTable(String tableName) {
		Connection conn;
		try {
			conn = DriverManager.getConnection(MYSQL.JDBC_ADDRESS);
			Statement deleteTableStmt = conn.createStatement();
		
			String deleteTableSQL = String.format("DROP TABLE %s ", tableName);
		
			@SuppressWarnings("unused")
			int deleteTable = deleteTableStmt.executeUpdate(deleteTableSQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
