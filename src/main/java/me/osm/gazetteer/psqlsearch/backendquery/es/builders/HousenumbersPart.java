package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class HousenumbersPart implements ESQueryPart {

	private String exact;
	private List<String> variants;
	private Integer numberPart;
	private boolean useRange;

	public HousenumbersPart(String exact, List<String> variants, Integer numberPart, boolean useRange) {
		this.exact = exact;
		if(variants == null) {
			variants = new ArrayList<>();
		}
		this.variants = variants;
		this.numberPart = numberPart;
		this.useRange = useRange;
	}
	
	@Override
	public JSONObject getPart() {
		JSONObject termsq = new JSONObject();
		termsq.put("terms", new JSONObject());
		
		termsq.getJSONObject("terms").put("_name", "house_number_array:" + exact);
		termsq.getJSONObject("terms").put("housenumber_array", this.variants);
		termsq.getJSONObject("terms").getJSONArray("housenumber_array").put(this.exact);
		termsq.getJSONObject("terms").put("boost", 1.5);
		
		if (this.numberPart != null && useRange) {
			JSONObject range = getRangeQuery();
			JSONObject rangeButNotMain = new JSONObject();
			
			rangeButNotMain.put("bool",	new JSONObject()
					.put("must", range)
					.put("must_not", termsq)
					.put("boost", 0.5f)
					.put("_name", "range_house_numbers:" + exact));
			
			return new JSONObject().put("dis_max", new JSONObject()
					.put("queries", new JSONArray().put(termsq).put(rangeButNotMain)));
		}
		
		return termsq;
	}

	private JSONObject getRangeQuery() {
		return new JSONObject().put("range", 
				new JSONObject().put("housenumber_number", 
						new JSONObject()
							.put("gte", Math.max(1, this.numberPart - 16))
							.put("lte", this.numberPart + 16)));
	}

}
