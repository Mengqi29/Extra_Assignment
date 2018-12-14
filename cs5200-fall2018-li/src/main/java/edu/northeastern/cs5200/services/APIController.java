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
	
	
	
	
}
