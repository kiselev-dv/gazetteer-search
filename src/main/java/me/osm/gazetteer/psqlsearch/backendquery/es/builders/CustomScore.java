package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import org.json.JSONObject;

public class CustomScore implements ESQueryPart {

	private ESQueryPart query;
	private String script;

	public CustomScore (ESQueryPart query, String script) {
		
		this.query = query;
		this.script = script;
		
	}

	@Override
	public JSONObject getPart() {
		JSONObject o = new JSONObject();
		
		o.put("function_score", new JSONObject()
				.put("query", query.getPart())
				.put("script_score", new JSONObject()
						.put("script", script)));

		return o;
	}
}
