package me.osm.gazetteer.search.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;

public class SendQAPI {
	
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(SendQAPI.class);
	
	public Object create(Request request, Response res) throws IOException {
		String body = IOUtils.toString(request.getBodyAsStream());
		JSONObject qery = new JSONObject(body);
		
		//log.info(qery.toString(2));
		
		SearchRequestBuilder searchRequestBuilder = ESServer.getInstance().client()
				.prepareSearch(IndexHolder.ADDRESSES_INDEX)
				.setTypes(IndexHolder.ADDR_ROW_TYPE)
				.setQuery(QueryBuilders.wrapperQuery(qery.toString()));
		
		List hits = new ArrayList<>();
		
		for (SearchHit hit : searchRequestBuilder.get().getHits()) {
			hits.add(writeHit(hit));
		}
		
		Map result = new HashMap<>();
		result.put("hits", hits);
		return result;
	}

	private Map writeHit(SearchHit hit) {
		Map h = new HashMap();
		h.put("score", hit.getScore());
		Map<String, Object> source = hit.getSourceAsMap();
		
		h.put("type", source.get("type"));
		h.put("name", source.get("name"));
		h.put("full_text", source.get("full_text"));
		
		return h;
	}
}
