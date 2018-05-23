package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

public class MatchPart implements ESQueryPart {

	private Iterable<String> terms;
	private String field;
	private Double boost;
	private String fuzziness;
	private String name;
	private int minimumShouldMatch;


	public MatchPart(String field, Iterable<String> terms) {
		this.field = field;
		this.terms = terms;
	}
	
	public MatchPart(String field, Iterable<String> terms, double boost) {
		this.field = field;
		this.terms = terms;
		this.boost = boost;
	}
	
	public MatchPart setBoost(Double boost) {
		this.boost = boost;
		return this;
	}

	public MatchPart setFuzziness(String fuzziness) {
		this.fuzziness = fuzziness;
		return this;
	}

	@Override
	public JSONObject getPart() {
		JSONObject obj = new JSONObject();
		obj.put("match", new JSONObject());
		obj.getJSONObject("match").put(field, new JSONObject());
		JSONObject q = obj.getJSONObject("match").getJSONObject(field);
		q.put("query", StringUtils.join(terms, ' '));
		
		if (this.boost != null) {
			q.put("boost", boost);
		}
		
		if (this.fuzziness != null) {
			q.put("fuzziness", fuzziness);
		}
		
		if (this.name != null) {
			q.put("_name", name);
		}
		
		if (minimumShouldMatch > 0) {
			q.put("minimum_should_match", minimumShouldMatch);
		}
		
		return obj;
	}

	public MatchPart setName(String name) {
		this.name = name;
		return this;
	}

	public MatchPart setMinimumShouldMatch(int i) {
		this.minimumShouldMatch = i;
		return this;
	}

}
