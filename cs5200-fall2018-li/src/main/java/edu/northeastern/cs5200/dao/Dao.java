package edu.northeastern.cs5200.dao;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONObject;

public interface Dao<T> {
	JSONObject get(String tableName, String id);

	JSONArray getAll(String tableName) throws SQLException;

	JSONObject save(String tableName, T t) throws Exception;

	void update(String tableName, String id, String[] params);

	void deleteRow(String tableName, String id);

	JSONObject findByID(String tableName, String id) throws SQLException;

	void deleteTable(String tableName);

	JSONObject updateRecord(String tableName, String id, T t) throws Exception;

}
