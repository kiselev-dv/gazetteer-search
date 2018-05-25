package me.osm.gazetteer.psqlsearch.api.search;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;

import me.osm.gazetteer.psqlsearch.esclient.AddressesIndexHolder;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;

public class ESCoalesce {
	
	private List<JSONObject> queries;
	private JSONObject lastQ;
	private String[] fetchSourceInclude;
	private long queryTime = 0;
	private Double lat;
	private Double lon;

	public ESCoalesce(List<JSONObject> queries, String[] fetchSourceInclude) {
		this.queries = queries;
		this.fetchSourceInclude = fetchSourceInclude;
	}

	public SearchResponse execute(int from, int size) {
		SearchResponse response = null;
		for (JSONObject q : queries) {
			this.lastQ = q;
			
			SearchRequestBuilder searchRequestBuilder = ESServer.getInstance().client()
					.prepareSearch(AddressesIndexHolder.INDEX_NAME)
					.setTypes(AddressesIndexHolder.ADDR_ROW_TYPE)
					.setFetchSource(fetchSourceInclude, new String[] {"json.address.parts.names"})
					.setQuery(QueryBuilders.wrapperQuery(q.toString()))
					.setFrom(from).setSize(size);
			
			searchRequestBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
			if (lat != null && lon != null) {
				searchRequestBuilder.addSort(SortBuilders.geoDistanceSort("centroid", lat, lon));
			}
			
			response = searchRequestBuilder.get();
			
			this.queryTime += response.getTook().getMillis();
			
			if (accepted(response)) {
				return response;
			}
		}
		
		return response;
		
	}

	private boolean accepted(SearchResponse response) {
		return response.getHits().totalHits > 0;
	}

	public JSONObject getExecutedQuery() {
		return lastQ;
	}
	
	public long getQueryTime() {
		return queryTime;
	}

	public void setDistanceSort(Double lat, Double lon) {
		this.lat = lat;
		this.lon = lon;
	}

}
