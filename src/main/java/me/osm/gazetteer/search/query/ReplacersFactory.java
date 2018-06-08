package me.osm.gazetteer.search.query;

public interface ReplacersFactory {
	public Replacer createReplacer(String pattern, String template);
}
