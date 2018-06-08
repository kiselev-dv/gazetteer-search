package me.osm.gazetteer.search.backendquery.es.builders;

import org.json.JSONObject;

public class StreetHasLocationFilter implements ESQueryPart{

	@Override
	public JSONObject getPart() {
		return new JSONObject().put("term", new JSONObject().put("street_has_loc", true));
	}

}
