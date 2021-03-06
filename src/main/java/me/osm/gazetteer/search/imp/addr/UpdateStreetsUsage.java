package me.osm.gazetteer.search.imp.addr;

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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.gazetteer.search.imp.PagedScroll;
import me.osm.gazetteer.search.util.TimePeriodFormatter;

public class UpdateStreetsUsage {
	private static final Logger log = LoggerFactory.getLogger(UpdateStreetsUsage.class);
	
	private static final int pageSize = 1000;
	
	private TransportClient client = ESServer.getInstance().client();
	private long counter;

	private String region;
	
	public UpdateStreetsUsage(String region) {
		this.region = region;
	}

	public static void main(String[] args) {
		new UpdateStreetsUsage(null).run();
	}

	@SuppressWarnings("unchecked")
	public void run() {
		
		long start = new Date().getTime();
		counter = 0;
		
		log.info("Update number of streets references for {}", region);
		
		PagedScroll pageScroll = new PagedScroll(pageSize, "hghnet", 
				new String[] {"id", "refs", "locality", "street"});
		
		pageScroll.setRegion(region);
		
		pageScroll.scroll(higwaysResponse -> {
			
			long totalHighways = higwaysResponse.getHits().getTotalHits();
			
			BulkRequestBuilder bulk = client.prepareBulk();
			MultiSearchRequestBuilder multySearch = client.prepareMultiSearch();

			for(SearchHit hit : higwaysResponse.getHits()) {
				
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
				
				counter++;
			}
			
			bulk.get();
			bulk = client.prepareBulk();
			
			long time = new Date().getTime() - start;
			double perLine = time / (double)counter;
			long eta = new Double((totalHighways - counter) * perLine).longValue();
			
			String etaString = "N/A";
			try {
				etaString = TimePeriodFormatter.printDuration(eta);
			}
			catch (ArithmeticException e) {
				// Probabbly too long
			}
			
			log.info(String.format("Lines %d of %d, %.3f ms per line, ETA %s", counter, totalHighways, perLine, etaString));
		});
		
		log.info("Done street references count update for {} in ", 
				region, TimePeriodFormatter.printDuration(new Date().getTime() - start));
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

}
