package me.osm.gazetteer.search.backendquery.es.builders;

import org.json.JSONObject;

public class TermsPart implements ESQueryPart {
	
	private String field;
	private Iterable<String> terms;

	public TermsPart(String field, Iterable<String> terms) {
		this.field = field;
		this.terms = terms;
	}

	@Override
	public JSONObject getPart() {
		return new JSONObject().put("terms", 
				new JSONObject().put(
						this.field, 
						this.terms));
	}

}
