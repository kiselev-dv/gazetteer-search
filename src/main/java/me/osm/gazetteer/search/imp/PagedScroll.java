package me.osm.gazetteer.search.imp;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.json.JSONObject;

import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;

public class PagedScroll {
	
	public static interface PageHandler {
		public void page(SearchResponse response);
	}
	
	private static final TransportClient client = ESServer.getInstance().client();
	
	private String type;
	private String[] fetchSource;

	private int pageSize;

	private String region = null;

	public PagedScroll(int pageSize, String type, String[] fetchSource) {
		this.type = type;
		this.fetchSource = fetchSource;
		this.pageSize = pageSize;
	}
	
	public void setRegion(String region) {
		this.region = region;
	}
	
	private SearchRequestBuilder prepareQuery() {
		
		JSONObject typeQ = new JSONObject().put("term", new JSONObject().put("type", type));
		JSONObject query = typeQ;
		
		if (region != null) {
			BooleanPart booleanPart = new BooleanPart();

			JSONObject regionQ = new JSONObject().put("term", new JSONObject().put("import.region", region));
			
			booleanPart.addMust(typeQ);
			booleanPart.addMust(regionQ);
			
			query = booleanPart.getPart();
		}
		
		return client.prepareSearch(IndexHolder.ADDRESSES_INDEX)
			.setTypes(IndexHolder.ADDR_ROW_TYPE)
			.setFetchSource(fetchSource, new String[] {})
			.addSort(SortBuilders.fieldSort("id"))
			.setQuery(QueryBuilders.wrapperQuery(query.toString()));
	}

	public void scroll(PageHandler pager) {
		SearchRequestBuilder query = prepareQuery().setFrom(0).setSize(0);
		
		
		long total = query.get().getHits().getTotalHits();

		String esid = null;
		for(int page = 0; page <= total / pageSize; page++) {
			
			SearchRequestBuilder q = prepareQuery()
					.setSize(pageSize);
			
			if (esid != null) {
				q.searchAfter(new Object[] {esid});
			}
			
			SearchResponse response = q.get();
			
			SearchHit[] hits = response.getHits().getHits();
			if(hits.length > 0) {
				esid = hits[hits.length - 1].getId();
				pager.page(response);
			}
		}
	}
}
	
