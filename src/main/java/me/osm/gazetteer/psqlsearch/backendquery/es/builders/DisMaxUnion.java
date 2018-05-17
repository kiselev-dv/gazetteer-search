package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

public class DisMaxUnion implements ESQueryPart {
	
	private static final String QUERY_NAME = "dis_max";

	private JSONArray queries;
	private JSONObject query;
	
	public DisMaxUnion() {

		this.query = new JSONObject();
		query.put(QUERY_NAME, new JSONObject());
		query.getJSONObject(QUERY_NAME).put("queries", new ArrayList<>());
		
		this.queries = query.getJSONObject(QUERY_NAME).getJSONArray("queries");
	}

	@Override
	public JSONObject getPart() {
		return query;
	}
	
	public void addSubquery(ESQueryPart part, double weight) {
		ESQueryPart scored = wrapWithFunctionScoreQuery(part, weight);
		queries.put(scored.getPart());
	}
	
	public void addSubquery(ESQueryPart part) {
		queries.put(part.getPart());
	}
	
	public void addSubquery(JSONObject part) {
		queries.put(part);
	}

	private ESQueryPart wrapWithFunctionScoreQuery(ESQueryPart part, double weight) {
		return new FunctionScorePart(part, "_score * doc['base_score'].value * " + weight);
	}
	
}
