package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class HousenumbersPart implements ESQueryPart {

	private String exact;
	private Set<String> variants;
	private Integer numberPart;
	private boolean useRange;
	
	private double hnArrayQBoost = 1.5;
	private double rangeHNQBoost = 0.5;

	public HousenumbersPart(String exact, Collection<String> variants, Integer numberPart, boolean useRange) {
		this.exact = exact;
		if(variants == null) {
			variants = new ArrayList<>();
		}
		this.variants = new HashSet<>(variants);
		this.numberPart = numberPart;
		this.useRange = useRange;
	}
	
	public void setHnArrayQBoost(double hnArrayQBoost) {
		this.hnArrayQBoost = hnArrayQBoost;
	}

	public void setRangeHNQBoost(double rangeHNQBoost) {
		this.rangeHNQBoost = rangeHNQBoost;
	}

	@Override
	public JSONObject getPart() {
//		JSONObject termsq = new JSONObject();
//		termsq.put("terms", new JSONObject());
//		
//		termsq.getJSONObject("terms").put("_name", "house_number_array:" + exact);
//		termsq.getJSONObject("terms").put("housenumber_array", this.variants);
//		termsq.getJSONObject("terms").getJSONArray("housenumber_array").put(this.exact);
//		termsq.getJSONObject("terms").put("boost", hnArrayQBoost);
//		
//		if (variants.size() > 1) {
//			Map<String, Object> params = new HashMap<>();
//			params.put("terms", this.variants);
//			String script = "params.terms.findAll(t -> doc['housenumber_array'].value.indexOf(t) >= 0).size()";
//			termsq = new CustomScore(termsq, script, params).getPart();
//		}

		BooleanPart hnArray = new BooleanPart();
		for (String t : this.variants) {
			hnArray.addShould(new JSONObject().put("term", new JSONObject().put("housenumber_array", t)));
		}
		JSONObject termsq = hnArray.getPart();
		termsq.getJSONObject("bool").put("boost", hnArrayQBoost);
		termsq.getJSONObject("bool").put("_name", "house_number_array:" + exact);
		
		
		if (this.numberPart != null && useRange) {
			JSONObject range = getRangeQuery();
			JSONObject rangeButNotMain = new JSONObject();
			
			rangeButNotMain.put("bool",	new JSONObject()
					.put("must", range)
					.put("must_not", termsq)
					.put("boost", rangeHNQBoost)
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
