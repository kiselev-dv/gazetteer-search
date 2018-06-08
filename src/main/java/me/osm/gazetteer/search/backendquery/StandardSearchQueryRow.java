package me.osm.gazetteer.search.backendquery;

public final class StandardSearchQueryRow {
	
	private double rank;
	private String fullText;
	private String json;
	private String osmId;
	
	public double getRank() {
		return rank;
	}
	
	public String getFullText() {
		return fullText;
	}
	
	public String getJson() {
		return json;
	}

	public String getOsmId() {
		return osmId;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	public void setFullText(String fullText) {
		this.fullText = fullText;
	}

	public void setJson(String json) {
		this.json = json;
	}

	public void setOsmId(String osmId) {
		this.osmId = osmId;
	}
	
}