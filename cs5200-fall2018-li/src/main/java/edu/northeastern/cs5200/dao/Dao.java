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

	String createMappingTable(String table1, String table2, String id1, String id2) throws Exception;

	JSONArray findMovieByActor(String table1, String table2, String id1) throws SQLException;

	JSONArray findActorByMovie(String table1, String table2, String id2) throws SQLException;

	void deleteMappingTableRow(String table1, String table2, String id1, String id2);

	void deleteMappingTableRowByActor(String table1, String table2, String id1);

}
