package me.osm.gazetteer.psqlsearch.backendquery.es.builders;

import org.json.JSONObject;

public class DistinctNameFilter implements ESQueryPart {

	@Override
	public JSONObject getPart() {
		return new JSONObject().put("term", new JSONObject().put("by_name_agg_index", 0));
	}

}
