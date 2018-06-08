package me.osm.gazetteer.search.query;

import java.util.List;
import java.util.Map;

public class QueryAnalizerCfg {

	private Map<String, String> charReplaces;
	private String tokenSeparators;
	private String removeChars;
	private Map<String, List<String>> synonims;

	public Map<String, String> getCharReplaces() {
		return charReplaces;
	}

	public void setCharReplaces(Map<String, String> charReplaces) {
		this.charReplaces = charReplaces;
	}

	public String getTokenSeparators() {
		return tokenSeparators;
	}

	public void setTokenSeparators(String tokenSeparators) {
		this.tokenSeparators = tokenSeparators;
	}

	public String getRemoveChars() {
		return removeChars;
	}

	public void setRemoveChars(String removeChars) {
		this.removeChars = removeChars;
	}

	public Map<String, List<String>> getSynonims() {
		return synonims;
	}

	public void setSynonims(Map<String, List<String>> synonims) {
		this.synonims = synonims;
	}
	
}
