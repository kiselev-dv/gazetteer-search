package me.osm.gazetteer.search.api.search;

import java.util.Collection;

import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;

public class POIOptions {
	
	private Collection<String> classes;
	
	private Collection<String> matchedTerms;
	
	private boolean matchPrefix;

	private BooleanPart poiQuery;
	
	public POIOptions(Collection<String> classes, 
			BooleanPart poiQuery, Collection<String> matchedTerms, 
			boolean matchPrefix) {
		this.classes = classes;
		this.poiQuery = poiQuery;
		this.matchedTerms = matchedTerms;
		this.matchPrefix = matchPrefix;
	}

	public Collection<String> getClasses() {
		return classes;
	}

	public Collection<String> getMatchedTerms() {
		return matchedTerms;
	}

	public boolean isMatchPrefix() {
		return matchPrefix;
	}

	public BooleanPart getPoiQuery() {
		return poiQuery;
	}
	
}
