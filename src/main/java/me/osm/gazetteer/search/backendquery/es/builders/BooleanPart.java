package me.osm.gazetteer.search.backendquery.es.builders;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

public class BooleanPart implements ESQueryPart {
	
	private static final String QUERY_NAME = "bool";

	private JSONArray must;
	private JSONArray filter;
	private JSONArray should;
	private JSONArray mustNot;
	
	private JSONObject query;

	public BooleanPart() {
		this.query = new JSONObject();
		query.put(QUERY_NAME, new JSONObject());

		this.must = new JSONArray();
		this.filter = new JSONArray();
		this.should = new JSONArray();
		this.mustNot = new JSONArray();
		
	}
	
	@Override
	public JSONObject getPart() {
		if (must.length() > 0) {
			query.getJSONObject(QUERY_NAME).put("must", must);
		}
		
		if (mustNot.length() > 0) {
			query.getJSONObject(QUERY_NAME).put("must_not", mustNot);
		}
		
		if (should.length() > 0) {
			query.getJSONObject(QUERY_NAME).put("should", should);
		}
		
		if (filter.length() > 0) {
			query.getJSONObject(QUERY_NAME).put("filter", filter);
		}
		
		return query;
	}
	
	public void addMust(ESQueryPart part) {
		must.put(part.getPart());
	}
	
	public void addMust(JSONObject part) {
		must.put(part);
	}
	
	public void addFilter(ESQueryPart part) {
		filter.put(part.getPart());
	}
	
	public void addFilter(JSONObject part) {
		filter.put(part);
	}
	
	public void addMustNot(ESQueryPart part) {
		mustNot.put(part.getPart());
	}
	
	public void addMustNot(JSONObject part) {
		mustNot.put(part);
	}
	
	public void addShould(ESQueryPart part) {
		should.put(part.getPart());
	}
	
	public void addShould(JSONObject part) {
		should.put(part);
	}

	public void setName(String name) {
		query.getJSONObject(QUERY_NAME).put("_name", name);
	}

	public void setBoost(double boost) {
		query.getJSONObject(QUERY_NAME).put("boost", boost);
	}
	
	public void setMinimumShouldMatch(int min) {
		query.getJSONObject(QUERY_NAME).put("minimum_should_match", min);
	}

}

