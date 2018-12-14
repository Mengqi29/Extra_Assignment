package edu.northeastern.cs5200.models;

import org.json.JSONObject;

public class Data {
public JSONObject json;
	
	public Data(String jsonString) {
		json = new JSONObject(jsonString);
	}
}
