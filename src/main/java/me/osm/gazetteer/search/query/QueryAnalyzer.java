package me.osm.gazetteer.search.query;


public interface QueryAnalyzer {

	public Query getQuery(String q);

}