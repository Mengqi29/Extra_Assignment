package edu.northeastern.cs5200.services;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import edu.northeastern.cs5200.dao.Dao;
import edu.northeastern.cs5200.dao.DataDao;
import edu.northeastern.cs5200.models.Data;

@RestController
public class APIController {
	
	Dao<Data> dataDao = DataDao.getInstance();
	
	//Get Table
	@RequestMapping(value = "/api/{table}", 
			method = RequestMethod.GET,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String getData(@PathVariable String table) {
		try {
			JSONArray json = dataDao.getAll(table);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}", e.toString())).toString();
		}
	}
	
	
	//Create Table
	@RequestMapping(value = "/api/{table}", 
			method = RequestMethod.POST, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String postData(@RequestBody String bodyJSON, @PathVariable String table) { 
		
		try {
			Data d = new Data(bodyJSON);
			JSONObject json = dataDao.save(table, d);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}",e.toString())).toString();
		}
		
	}
	
	//Get record by Id
	@RequestMapping(value = "/api/{table}/{id}", 
			method = RequestMethod.GET,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String getFindById(@PathVariable String table, @PathVariable String id) {
		try {
			JSONObject json = dataDao.findByID(table, id);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}", e.toString())).toString();
		}
	}
	
	//Update Record
	@RequestMapping(value = "/api/{table}/{id}", 
			method = RequestMethod.PUT, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String updateRecord(@RequestBody String bodyJSON, @PathVariable String table,@PathVariable String id) { 
		
		try {
			Data d = new Data(bodyJSON);
			JSONObject json = dataDao.updateRecord(table, id, d);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}",e.toString())).toString();
		}
		
	}
	
	//Delete Record 
	@RequestMapping(value = "/api/{table}/{id}", 
			method = RequestMethod.DELETE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void deleteRecord(@PathVariable String table, @PathVariable String id) {
		try {
			dataDao.deleteRow(table, id);
			System.out.println("OK");
		} catch (Exception e) {
			System.out.println("null");
		}
	}
	
	
	//Delete Table
	@RequestMapping(value = "/api/{table}", 
			method = RequestMethod.DELETE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void deleteTable(@PathVariable String table) {
		try {
			dataDao.deleteTable(table);
			System.out.println("OK");
		} catch (Exception e) {
			System.out.println("null");
		}
	}
	
	//Create Mapping Table
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}/{id2}", 
			method = RequestMethod.POST, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String createMappingTable(
			@PathVariable String table1,
			@PathVariable String id1,
			@PathVariable String table2,
			@PathVariable String id2) {
		try {
			dataDao.createMappingTable(table1, table2, id1, id2);
		} catch (Exception e) {
			return String.format("{errorCode:-1003, message: %s}", e.toString());
		}
		return new JSONObject(String.format("{table1:%s, id1:%s, table2:%s, id2:%s}", table1, id1, table2, id2)).toString();
	}
	
	
	//Get record from MappingTable
	@RequestMapping(value = "/api/{table1}/{id}/{table2}", 
			method = RequestMethod.GET, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String getMovieByActor(
			@PathVariable String table1,
			@PathVariable String id,
			@PathVariable String table2) {
		try {
			JSONArray json = dataDao.findMovieByActor(table1, table2, id);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}", e.toString())).toString();
		}
	}
	
	//Get record from MappingTable by getActorByMovie
	@RequestMapping(value = "/api/getActorByMovie/{table2}/{id}/{table1}", 
			method = RequestMethod.GET,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String getData(@PathVariable String table2, @PathVariable String id, @PathVariable String table1) {
		try {
			JSONArray json = dataDao.findActorByMovie(table1, table2, id);
			return json.toString();
		} catch (Exception e) {
			return new JSONObject(String.format("{errorCode: -1001, error:\"%s\"}", e.toString())).toString();
		}
	}
	
	//Delete MappingTable Row by actor_id and movie_id
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}/{id2}", 
			method = RequestMethod.DELETE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void deleteMappingTableRow(@PathVariable String table1, @PathVariable String table2,@PathVariable String id1, @PathVariable String id2) {
		try {
			dataDao.deleteMappingTableRow(table1, table2, id1, id2);
		} catch (Exception e) {
		}
	}
	
	//Delete MappingTable Row by actor_id
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}", 
			method = RequestMethod.DELETE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public void deleteMappingTableRowByActor(@PathVariable String table1, @PathVariable String table2,@PathVariable String id1) {
		try {
			dataDao.deleteMappingTableRowByActor(table1, table2, id1);
		} catch (Exception e) {
		}
	}

	
}
