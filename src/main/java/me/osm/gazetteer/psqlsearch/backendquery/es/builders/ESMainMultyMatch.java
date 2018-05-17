package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

public class ESMainMultyMatch implements ESQueryPart {
	
	private List<String> terms;
	private List<String> variants;
	private List<String> numbers;

	/*
	{
		"multi_match": {
			"query": "екатеринбург тимирязева 13",
			"operator": "OR",
			"minimum_should_match": "2",
			"fields": [
				"locality",
				"street",
				"housenumber_exact"
			],
			"type": "cross_fields"
		}
	}
	 */

	/**
	 * @param terms - terms
	 * @param variants - terms variants
	 * */
	public ESMainMultyMatch(List<String> terms, List<String> variants, List<String> numbers) {
		this.terms = terms;
		this.variants = variants;
		this.numbers = numbers;
	}
	
	@Override
	public JSONObject getPart() {
		JSONObject obj = new JSONObject();
		obj.put("multi_match", new JSONObject());
		JSONObject query = obj.getJSONObject("multi_match");
		query.put("_name", "locality_street_exact");
		
		List<String> all = new ArrayList<>();
		all.addAll(terms);
		all.addAll(variants);
		all.addAll(numbers);
		
		query.put("query", StringUtils.join(all, " "));
		query.put("operator", "OR");
		query.put("minimum_should_match", terms.size());
		query.put("type", "cross_fields");
		
		query.put("fields", Arrays.asList("locality", "street"));
		
		// TODO зделать так же как с номером дома
		// вначале пытаемся сматчить без цифр, потом с цифрами
		return obj;
		
	}
	

}
