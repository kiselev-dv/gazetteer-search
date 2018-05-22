package me.osm.gazetteer.psqlsearch.api.search;

import java.util.List;

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
	private int from;
	private int size;

	public ESCoalesce(List<JSONObject> queries, int from, int size) {
		this.queries = queries;
		this.from = from;
		this.size = size;
	}

	public SearchResponse execute() {
		SearchResponse response = null;
		
		for (JSONObject q : queries) {
			this.lastQ = q;
			response = ESServer.getInstance().client()
					.prepareSearch(AddressesIndexHolder.INDEX_NAME)
					.addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
					.setTypes(AddressesIndexHolder.ADDR_ROW_TYPE)
					.setQuery(QueryBuilders.wrapperQuery(q.toString()))
					.setFrom(from).setSize(size)
					.get();
			
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

}
