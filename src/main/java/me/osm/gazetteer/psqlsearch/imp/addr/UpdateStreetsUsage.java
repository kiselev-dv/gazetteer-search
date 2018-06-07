package me.osm.gazetteer.psqlsearch.imp.addr;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.json.JSONObject;

import me.osm.gazetteer.psqlsearch.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;
import me.osm.gazetteer.psqlsearch.esclient.IndexHolder;
import me.osm.gazetteer.psqlsearch.util.TimePeriodFormatter;

public class UpdateStreetsUsage {
	private static final int pageSize = 1000;
	
	private TransportClient client = ESServer.getInstance().client();
	
	public static void main(String[] args) {
		new UpdateStreetsUsage().run();
	}

	@SuppressWarnings("unchecked")
	private void run() {
		
		SearchRequestBuilder query = buildHighwayRequest()
			.setFrom(0).setSize(0);
		
		long start = new Date().getTime();
		long totalHighways = query.get().getHits().getTotalHits();
		long counter = 0;
		
		
		String esid = null;
		for(int page = 0; page <= totalHighways / pageSize; page++) {
			
			SearchRequestBuilder q = buildHighwayRequest()
					.setSize(pageSize);
			
			if (esid != null) {
				q.searchAfter(new Object[] {esid});
			}
			
			SearchResponse higwaysResponse = q.get();
			
			BulkRequestBuilder bulk = client.prepareBulk();
			MultiSearchRequestBuilder multySearch = client.prepareMultiSearch();

			for(SearchHit hit : higwaysResponse.getHits()) {
				counter++;

				esid = hit.getId();
				
				Collection<String> refs = (Collection<String>)((Map<String, ?>)hit.getSourceAsMap().get("refs")).get("street");
				if (refs != null) {
					List<String> hghways = refs.stream().filter(s -> s.startsWith("hghway")).collect(Collectors.toList());
					
					BooleanPart subQ = new BooleanPart();
					subQ.addMust(new JSONObject().put("term", new JSONObject().put("type", "adrpnt")));
					subQ.addMust(new JSONObject().put("terms", new JSONObject().put("refs.street", hghways)));
					
					multySearch.add(client.prepareSearch(IndexHolder.ADDRESSES_INDEX)
							.setTypes(IndexHolder.ADDR_ROW_TYPE)
							.setSize(0)
							.setQuery(QueryBuilders.wrapperQuery(subQ.getPart().toString())));
					
				}
			}
			
			Item[] responses = multySearch.get().getResponses();
			
			int hitIndex = 0;
			for(SearchHit hit : higwaysResponse.getHits()) {
				
				Iterable<String> localityTokens = (Iterable<String>)hit.getSourceAsMap().get("locality");
				Iterable<String> streetTokens = (Iterable<String>)hit.getSourceAsMap().get("street");
				
				Collection<String> refs = (Collection<String>)((Map<String, ?>)hit.getSourceAsMap().get("refs")).get("street");
				Map<String, Object> doc = new HashMap<>();
				if (refs != null) {
					Item item = responses[hitIndex++];
					SearchResponse related = item.getResponse();
					long count = related.getHits().getTotalHits();
					
					doc.put("ref_count", (Long) count);
				}
				
				doc.put("street_has_loc", isStreetContainsLocation(streetTokens, localityTokens));
				
				bulk.add(client.prepareUpdate(
						IndexHolder.ADDRESSES_INDEX, 
						IndexHolder.ADDR_ROW_TYPE, 
						hit.getId()).setDoc(doc));
			}
			
			bulk.get();
			bulk = client.prepareBulk();
			
			long time = new Date().getTime() - start;
			double perLine = time / (double)counter;
			long eta = new Double((totalHighways - counter) * perLine).longValue();
			
			System.out.println(String.format("Lines %d, %.3f ms per line, ETA %s", counter, perLine, TimePeriodFormatter.printDuration(eta)));
		}
		
	}

	private Boolean isStreetContainsLocation(Iterable<String> streetTokens, Iterable<String> localityTokens) {
		for (String l : localityTokens) {
			for (String s : streetTokens) {
				if(StringUtils.contains(s, l) || StringUtils.contains(l, s)) {
					return true;
				}
			}
		}
		
		return false;
	}

	private SearchRequestBuilder buildHighwayRequest() {
		JSONObject q = new JSONObject().put("term", new JSONObject().put("type", "hghnet"));
		
		return client.prepareSearch(IndexHolder.ADDRESSES_INDEX)
			.setTypes(IndexHolder.ADDR_ROW_TYPE)
			.setFetchSource(new String[] {"id", "refs", "locality", "street"}, new String[] {})
			.addSort(SortBuilders.fieldSort("_id"))
			.setQuery(QueryBuilders.wrapperQuery(q.toString()));
	}

}
