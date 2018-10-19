package me.osm.gazetteer.search.api.osmdoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restexpress.Request;
import org.restexpress.Response;

import me.osm.gazetteer.search.api.RequestUtil;
import me.osm.gazetteer.search.api.SearchAPIAdapter;
import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.search.backendquery.es.builders.TermsPart;
import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.osmdoc.localization.L10n;
import me.osm.osmdoc.model.v2.Feature;
import me.osm.osmdoc.read.OSMDocFacade;

public class TagStatisticsAPI {
	
	private final OSMDocFacade facade;
	
	public TagStatisticsAPI(OSMDocFacade facade) {
		this.facade = facade;
	}
	
	public Object read(Request request, Response response) {
		
		Set<String> classes = RequestUtil.getSet(request, SearchAPIAdapter.POICLASS);
		Set<String> refs = RequestUtil.getSet(request, SearchAPIAdapter.REFERENCES);
		
		Locale locale = getLocale(request);
		
		boolean doc4Found = RequestUtil.getBoolean(request, "doc4found", true);
		
		List<Feature> features = listClasses(classes, facade);
		
		if(features.isEmpty()) {
			response.setResponseCode(404);
			return null;
		}
		
		BooleanPart filters = new BooleanPart();
		
		filters.addMust(new JSONObject().put("term", new JSONObject().put("type", "poipnt")));
		filters.addMust(new TermsPart("poi_class", classes));
		
		addRefsFilter(refs, filters);
		
		SearchRequestBuilder searchQ = ESServer.getInstance().client()
				.prepareSearch(IndexHolder.ADDRESSES_INDEX)
				.setTypes(IndexHolder.ADDR_ROW_TYPE)
				.setSize(0)
				.setQuery(QueryBuilders.wrapperQuery(filters.getPart().toString()));
		
		JSONObject tagOptions = facade.collectCommonTagsWithTraitsJSON(facade.getFeature(classes), locale);
		Set<String> allTagKeys = getTagKeys(tagOptions);
		
		removeNotGrouppingTags(allTagKeys);

		for(String tagKey : allTagKeys) {
			searchQ.addAggregation(AggregationBuilders.terms(tagKey)
					.field("more_tags." + tagKey).minDocCount(10));
		}
		searchQ.addAggregation(AggregationBuilders.terms("name").field("name.exact")
				.minDocCount(10).size(25));
		searchQ.addAggregation(AggregationBuilders.terms("brand").field("more_tags.brand")
				.minDocCount(10).size(25));
		
		searchQ.setSearchType(SearchType.DEFAULT);
		
		SearchResponse esResponse = searchQ.execute().actionGet();
		
		Aggregations aggregations = esResponse.getAggregations();
		
		JSONObject result = new JSONObject();
		result.put("poi_class", new JSONArray(classes));
		result.put("total_count", esResponse.getHits().getTotalHits());
		result.put("tag_options", tagOptions);
		
		// Order tags by key
		JSONObject statistic = new JSONObject();
		result.put("tagValuesStatistic", statistic);
		
		for(Aggregation agg : aggregations.asList()) {
			if(agg instanceof Terms) {
				Terms termsAgg = (Terms) agg;

				JSONObject values = new JSONObject();
				for(Terms.Bucket bucket : termsAgg.getBuckets()) {
					values.put(bucket.getKey().toString(), bucket.getDocCount()); 
				}
				
				if("name".equals(agg.getName())) {
					result.put("names", values);
				}
				else if("type".equals(agg.getName())) {
					result.put("types", values);
				}
				else if(values.length() > 0) {
					statistic.put(agg.getName(), values);
				}
			}
		}
		
		if(doc4Found) {
			Set<String> foundedKeys = statistic.keySet();
			Set<String> notFound = new HashSet<>(allTagKeys);
			notFound.removeAll(foundedKeys);
			
			JSONObject groupedTags = tagOptions.getJSONObject("groupedTags");
			JSONArray options = tagOptions.getJSONArray("commonTagOptions");
			
			for(String notFoundKey : notFound) {
				groupedTags.remove(notFoundKey);
			}

			TreeSet<Integer> remove = new TreeSet<>();
			for(int i = 0; i < options.length(); i++) {
				JSONObject filter = options.getJSONObject(i);
				if(notFound.contains(filter.getString("key"))) {
					remove.add(i);
				}
				else if(filter.getString("key").startsWith("trait_")) {
					JSONArray group = filter.optJSONArray("options");
					
					TreeSet<Integer> gropRemove = new TreeSet<>();
					for(int j = 0; j < group.length(); j++) {
						if(notFound.contains(group.getJSONObject(j).getString("valueKey"))) {
							gropRemove.add(j);
						}
					}
					
					for(Iterator<Integer> gri = gropRemove.descendingIterator(); gri.hasNext();) {
						group.remove(gri.next());
					}
					
					if(group.length() == 0) {
						remove.add(i);
					}
				}
			}
			
			for(Iterator<Integer> ri = remove.descendingIterator(); ri.hasNext();) {
				options.remove(ri.next());
			}
			
		}
		
		return result.toMap();
	}

	private void removeNotGrouppingTags(Set<String> allTagKeys) {
		//allTagKeys.removeAll(GazetteerWeb.osmdocProperties().getIgnoreTagsGrouping());
	}

	private void addRefsFilter(Set<String> refs, BooleanPart filters) {
		if(refs != null && !refs.isEmpty()) {
			filters.addFilter(new JSONObject().put("query_string", 
					new JSONObject()
					.put("fields", Arrays.asList("refs*"))
					.put("query", StringUtils.join(refs, "OR"))));
		}
	}

	private List<Feature> listClasses(Set<String> classes, OSMDocFacade osmdoc) {
		List<Feature> features = new ArrayList<>();
		for(String clazz : classes) {
			Feature feature = osmdoc.getFeature(clazz);
			if(feature != null) {
				features.add(feature);
			}
		}
		return features;
	}

	private Locale getLocale(Request request) {
		Locale locale = null;
		if(L10n.supported.contains(request.getHeader("lang"))) {
			locale = Locale.forLanguageTag(request.getHeader("lang"));
		}
		return locale;
	}

	private Set<String> getTagKeys(JSONObject tagOptions) {

		Set<String> result = new HashSet<>();
		
		JSONArray tagOptionsJSON = tagOptions.optJSONArray("commonTagOptions");
		for(int i = 0; i < tagOptionsJSON.length(); i++) {
			JSONObject jsonObject = tagOptionsJSON.getJSONObject(i);
			if(!jsonObject.getString("type").equals("GROUP_TRAIT")) {
				result.add(jsonObject.getString("key"));
			}
		}
		
		result.addAll(tagOptions.getJSONObject("groupedTags").keySet());
		
		return result;
	}

	
}
