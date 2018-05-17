package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import org.json.JSONObject;

public class FunctionScorePart implements ESQueryPart {
	
	private static final String QUERY_NAME = "function_score";

	private ESQueryPart subj;
	private String scoreF;
	
	public FunctionScorePart(ESQueryPart subj, String scoreF) {
		this.subj = subj;
		this.scoreF = scoreF;
	}
	
	@Override
	public JSONObject getPart() {
		JSONObject obj = new JSONObject();
		
		obj.put(QUERY_NAME, new JSONObject());
		
		obj.getJSONObject(QUERY_NAME)
			.put("query", this.subj.getPart());
		
		obj.getJSONObject(QUERY_NAME)
			.put("script_score", new JSONObject());
		obj.getJSONObject(QUERY_NAME)
			.getJSONObject("script_score")
			.put("script", new JSONObject());
		obj.getJSONObject(QUERY_NAME)
			.getJSONObject("script_score")
			.getJSONObject("script")
			.put("source", scoreF);
		
		return obj;
	}

}