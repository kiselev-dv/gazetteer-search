package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

public class MatchPart implements ESQueryPart {

	private List<String> terms;
	private String field;
	private Double boost;
	private String fuzziness;
	private String name;


	public MatchPart(String field, List<String> terms) {
		this.field = field;
		this.terms = terms;
	}
	
	public MatchPart(String field, List<String> terms, double boost) {
		this.field = field;
		this.terms = terms;
		this.boost = boost;
	}
	
	public void setBoost(Double boost) {
		this.boost = boost;
	}

	public void setFuzziness(String fuzziness) {
		this.fuzziness = fuzziness;
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
		
		return obj;
	}

	public void setName(String name) {
		this.name = name;
	}

}
