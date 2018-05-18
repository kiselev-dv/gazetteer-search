package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.Map;

import org.json.JSONObject;

public class CustomScore implements ESQueryPart {

	private JSONObject query;
	private String script;
	private Map<String, Object> parameters;

	public CustomScore (ESQueryPart query, String script, Map<String, Object> parameters) {
		this.query = query.getPart();
		this.script = script;
		this.parameters = parameters;
	}

	public CustomScore(JSONObject query, String script, Map<String, Object> parameters) {
		this.query = query;
		this.script = script;
		this.parameters = parameters;
	}

	@Override
	public JSONObject getPart() {
		JSONObject o = new JSONObject();
		
		o.put("function_score", new JSONObject()
				.put("query", query)
				.put("script_score", new JSONObject()
						.put("script", new JSONObject().put("source", script))));
		
		if (this.parameters != null) {
			o.getJSONObject("function_score")
				.getJSONObject("script_score")
					.getJSONObject("script")
						.put("params", parameters);
		}

		return o;
	}
}
