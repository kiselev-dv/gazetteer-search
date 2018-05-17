package me.osm.gazetteer.psqlsearch.api;

import java.util.ArrayList;
import java.util.List;

public class ResultsWrapper {
	
	private int page;
	private int pageSize;
	private String query;
	private String error;
	private String parsed_query;
	private long total_hits;
	private String debug_query;
	
	private List<SearchResultRow> rows = new ArrayList<>();
	
	public static final class SearchResultRow {
		public double rank;
		public String full_text;
		public String osm_id;
		public double base_score;
		public String[] matched_queries;
	}
	
	public ResultsWrapper() {
		
	}
	
	public ResultsWrapper(String queryString, int page, int pageSize) {
		this.query = queryString;
		this.page = page;
		this.pageSize = pageSize;
	}

	public void addResultsRow(double rank, double baseScore, String fullText, String osmId, String[] matchedQueries) {
		SearchResultRow row = new SearchResultRow();
		
		row.rank = rank;
		row.full_text = fullText;
		row.osm_id = osmId;
		row.base_score = baseScore;
		row.matched_queries = matchedQueries;
				
		rows.add(row);
	}

	public void setPage(int page) {
		this.page = page;
	}
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public void setErrorMessage(String message) {
		this.error = message;
	}

	public void setParsedQuery(String q) {
		this.parsed_query = q;
	}

	public void setTotalHits(long totalHits) {
		this.total_hits = totalHits;
	}

	public void setDebugQuery(String string) {
		this.debug_query = string;
	}
	
}
