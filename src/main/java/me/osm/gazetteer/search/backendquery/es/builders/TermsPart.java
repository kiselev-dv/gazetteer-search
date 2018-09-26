package me.osm.gazetteer.search.backendquery.es.builders;

import java.util.ArrayList;
import java.util.Collection;

import org.json.JSONObject;

public class TermsPart implements ESQueryPart {
	
	private String field;
	private Collection<String> terms;

	public TermsPart(String field, Collection<String> terms) {
		this.field = field;
		this.terms = new ArrayList<String>(terms);
	}

	@Override
	public JSONObject getPart() {
		return new JSONObject().put("terms", 
				new JSONObject().put(
						this.field, 
						this.terms));
	}

	public void addValue(String string) {
		this.terms.add(string);
	}

}
