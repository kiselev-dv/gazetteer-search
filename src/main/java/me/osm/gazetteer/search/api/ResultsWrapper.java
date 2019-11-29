package me.osm.gazetteer.search.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.geo.GeoPoint;

public class ResultsWrapper {
	
	private int page;
	private int pageSize;
	private String query;
	private String error;
	private String parsed_query;
	private long total_hits;
	private String debug_query;
	
	private List<SearchResultRow> rows = new ArrayList<>();
	private int trim;
	private long answer_time;
	private long query_time;
	private String mark;
	private Collection<String> matched_poi_classes;
	
	public static final class SearchResultRow {
		public double rank;
		public String full_text;
		public String osm_id;
		public double base_score;
		public String[] matched_queries;
		public GeoPoint centroid;
		public Map address;
		public String name;
		public Map refs;
		public Collection<String> poi_class;
		public String id;
		public Map geometry;
	}
	
	public ResultsWrapper() {
		
	}
	
	public ResultsWrapper(String queryString, int page, int pageSize) {
		this.query = queryString;
		this.page = page;
		this.pageSize = pageSize;
	}

	public void addResultsRow(double rank, double baseScore, 
			String name, Map addressAsMap, String fullText, String id, 
			String osmId, GeoPoint geoPoint, String[] matchedQueries, Map refs, 
			Collection<String> poiClasses, Map geometry) {
		
		SearchResultRow row = new SearchResultRow();
		
		row.name = name;
		
		row.rank = rank;
		row.full_text = fullText;
		row.id = id;
		row.osm_id = osmId;
		row.base_score = baseScore;
		row.matched_queries = matchedQueries;
		row.centroid = geoPoint;
		
		row.address = addressAsMap;
		row.refs = refs;
		row.poi_class = poiClasses;
		row.geometry = geometry;
				
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

	public void setTrim(int trim) {
		this.trim = trim;
	}

	public void setAnswerTime(long l) {
		this.answer_time = l;
	}

	public void setQueryTime(long queryTime) {
		this.query_time = queryTime;
	}

	public void setMark(String mark) {
		this.mark = mark;
	}

	public void setMatchedPoiClasses(Collection<String> poiClasses) {
		this.matched_poi_classes = poiClasses;
	}

	
	public long getTotalHits() {
		return total_hits;
	}

	public String getDebugQuery() {
		return debug_query;
	}

	public List<SearchResultRow> getRows() {
		return rows;
	}

	public int getTrim() {
		return trim;
	}

	public long getAnswerTime() {
		return answer_time;
	}

	public long getQueryTime() {
		return query_time;
	}
	
	public String getParsedQuery() {
		return parsed_query;
	}
	
}
