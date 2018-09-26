package me.osm.gazetteer.search.api.search;

import java.util.Collection;

public class POIClassesQueryResults {
	
	private Collection<String> classes;
	
	private Collection<String> matchedTerms;
	
	private boolean matchPrefix;
	
	public POIClassesQueryResults(Collection<String> classes, Collection<String> matchedTerms, boolean matchPrefix) {
		this.classes = classes;
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
	
}
