package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

public class BooleanPart implements ESQueryPart {
	
	private static final String QUERY_NAME = "bool";

	private JSONArray must;
	private JSONArray should;
	
	private JSONObject query;
	
	public BooleanPart() {
		this.query = new JSONObject();
		query.put(QUERY_NAME, new JSONObject());

		query.getJSONObject(QUERY_NAME).put("must", new ArrayList<>());
		query.getJSONObject(QUERY_NAME).put("should", new ArrayList<>());
		
		this.must = query.getJSONObject(QUERY_NAME).getJSONArray("must");
		this.should = query.getJSONObject(QUERY_NAME).getJSONArray("should");
	}
	
	@Override
	public JSONObject getPart() {
		return query;
	}
	
	public void addMust(ESQueryPart part) {
		must.put(part.getPart());
	}
	
	public void addMust(JSONObject part) {
		must.put(part);
	}
	
	public void addShould(ESQueryPart part) {
		should.put(part.getPart());
	}
	
	public void addShould(JSONObject part) {
		should.put(part);
	}

}

