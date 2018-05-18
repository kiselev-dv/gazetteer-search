package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import org.json.JSONObject;

public class Prefix implements ESQueryPart {
	
	private static final String QUERY_NAME = "prefix";

	private String prefix;
	private String field;

	private String name;
	
	public Prefix(String prefix, String field) {
		this.prefix = prefix;
		this.field = field;
	}
	
	@Override
	public JSONObject getPart() {
		JSONObject obj = new JSONObject();
		
		obj.put(QUERY_NAME, new JSONObject());
		
		obj.getJSONObject(QUERY_NAME)
			.put(this.field, new JSONObject());
		
		obj.getJSONObject(QUERY_NAME)
			.getJSONObject(this.field)
			.put("value", this.prefix);
		
		if (name != null) {
			obj.getJSONObject(QUERY_NAME)
			.getJSONObject(this.field)
			.put("_name", this.name);
		}
		
		return obj;
	}

	public void setName(String name) {
		this.name = name; 
	}

}
