package me.osm.gazetteer.search.api.search;

import static me.osm.gazetteer.search.api.search.MainAddressQueryBuilder.MATCH_LOCALITY_QUERY_NAME;
import static me.osm.gazetteer.search.api.search.MainAddressQueryBuilder.MATCH_STREET_QUERY_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.api.ResultsWrapper;
import me.osm.gazetteer.search.api.search.MainAddressQueryBuilder.ParsedTokens;
import me.osm.gazetteer.search.api.search.MainAddressQueryBuilder.QueryBuilderFlags;
import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.search.backendquery.es.builders.TermsPart;
import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.gazetteer.search.esclient.POIClassIndexHolder;
import me.osm.gazetteer.search.query.QToken;
import me.osm.gazetteer.search.query.Query;
import me.osm.gazetteer.search.query.QueryAnalyzer;
import me.osm.gazetteer.search.query.QueryAnalyzerImpl;;

public class ESDefaultSearch implements Search {
	
	private static final Logger log = LoggerFactory.getLogger(ESDefaultSearch.class);
	
	private static final String[] SOURCE_FIELDS_BASE = new String[] {
			"full_text", "osm_id", "json.name", "base_score", "refs",
			"type", "centroid", "id", "json.address.text", "poi_class"};

	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();
	private MainAddressQueryBuilder addrQueryBuilder = new MainAddressQueryBuilder();

	private static final boolean POI_IMPORTED = new POIClassIndexHolder().exists();
	
	@Override
	public ResultsWrapper search(String queryString, int page, int pageSize, SearchOptions options) {
		
		long startms = new Date().getTime();
		
		Query query = analyzer.getQuery(queryString);
		List<QToken> tokens = query.listToken();
		
		if (tokens.isEmpty()) {
			return new ResultsWrapper(queryString, page, pageSize);
		}

		QToken prefixT = null;
		if (options.isWithPrefix() && !queryString.endsWith(" ")) {
			prefixT = query.findPrefix();
		}

		List<QToken> numberTokens = new ArrayList<>();
		List<QToken> optionalTokens = new ArrayList<>();
		
		List<QToken> requiredTokens = new ArrayList<>();
		List<String> requiredVariants = new ArrayList<>();
		List<String> allRequiredTokenStrings = new ArrayList<>();
		
		tokens.stream().filter(t -> t.isHasNumbers()).forEach(numberTokens::add);
		tokens.stream().filter(t -> t.isOptional() && !t.isHasNumbers()).forEach(optionalTokens::add);
		
		tokens.stream().filter(t -> !t.isOptional() && !t.isHasNumbers()).forEach(t -> {
			requiredTokens.add(t);
			allRequiredTokenStrings.add(t.toString());

			if(t.isFuzzied()) {
				requiredVariants.addAll(t.getVariants());
				allRequiredTokenStrings.addAll(t.getVariants());
			}
		});
		
		String[] sourceFields = getSourceFields(options);
		POIClassesQueryResults pois = null;
		
		if (POI_IMPORTED && !options.isNoPoi()) {
			pois = queryPois(prefixT, allRequiredTokenStrings, sourceFields);

			if (pois.isMatchPrefix()) {
				log.info("Prefix matched with POI class");
				prefixT = null;
			}
			
			clearPOITerms(tokens, optionalTokens, 
					requiredTokens, requiredVariants, 
					allRequiredTokenStrings, pois);
		}

		List<JSONObject> coallesceQueries = new ArrayList<>();
		
		Collection<String> references = options.getReferences();
		
		ParsedTokens parsedTokens = new ParsedTokens(
				numberTokens, 
				optionalTokens, 
				requiredTokens, 
				allRequiredTokenStrings, 
				prefixT);

		if (pois != null) {
			coallesceQueries.add(addReferencesFilter(pois.getPoiQuery(), references).getPart());
		}
		
		coallesceQueries.add(addReferencesFilter(addrQueryBuilder.buildQuery(
				query, parsedTokens, pois, 
					QueryBuilderFlags.getFlags(QueryBuilderFlags.ONLY_ADDR_POINTS, QueryBuilderFlags.FUZZY)), 
				references).getPart());
		
		coallesceQueries.add(addReferencesFilter(addrQueryBuilder.buildQuery(
				query, parsedTokens, pois,
					QueryBuilderFlags.getFlags(QueryBuilderFlags.STREETS_WITH_NUMBERS, QueryBuilderFlags.FUZZY)), 
				references).getPart());
		
		coallesceQueries.add(addReferencesFilter(addrQueryBuilder.buildQuery(
				query, parsedTokens, pois,
					QueryBuilderFlags.getFlags(QueryBuilderFlags.FUZZY, QueryBuilderFlags.STREET_OR_LOCALITY)), 
				references).getPart());

		ESCoalesce coalesce = new ESCoalesce(coallesceQueries, sourceFields);
		
		if (options.getLat() != null && options.getLon() != null) {
			coalesce.setDistanceSort(options.getLat(), options.getLon());
		}
		
		ResultsWrapper results = new ResultsWrapper(queryString, page, pageSize);
		results.setParsedQuery(query.print());
		if (pois != null) {
			results.setMatchedPoiClasses(pois.getClasses());
		}
		
		try {
			SearchResponse response = coalesce.execute(0, 20);
			int trim = trimResponse(response);
			
			results.setDebugQuery(coalesce.getExecutedQuery().toString(2));
			results.setTotalHits(response.getHits().totalHits);
			results.setTrim(trim);
			results.setQueryTime(coalesce.getQueryTime());
			
			writeHits(results, response);
			
		}
		catch (Exception e) {
			results.setErrorMessage(e.getMessage());
			e.printStackTrace();
		}
		
		results.setAnswerTime(new Date().getTime() - startms);
		
		return results;
	}

	private String[] getSourceFields(SearchOptions options) {
		String[] sourceFields = SOURCE_FIELDS_BASE;
		if (options.isVerboseAddress()) {
			sourceFields = ArrayUtils.add(SOURCE_FIELDS_BASE, "json.address");
		}
		return sourceFields;
	}

	private void clearPOITerms(List<QToken> tokens, List<QToken> optionalTokens, List<QToken> requiredTokens,
			List<String> requiredVariants, List<String> allRequiredTokenStrings, POIClassesQueryResults poiClasses) {
		requiredTokens.clear();
		requiredVariants.clear();
		allRequiredTokenStrings.clear();
		
		for(QToken t : tokens) {
			if(!t.isOptional() && !t.isHasNumbers()) {
				if (poiClasses.getMatchedTerms().contains(t.toString())) {
					optionalTokens.add(t);
				}
				else {
					requiredTokens.add(t);
					allRequiredTokenStrings.add(t.toString());
					
					if(t.isFuzzied()) {
						requiredVariants.addAll(t.getVariants());
						allRequiredTokenStrings.addAll(t.getVariants());
					}
				}
			}
		}
	}

	private BooleanPart addReferencesFilter(BooleanPart q, Collection<String> references) {
		if (references != null && !references.isEmpty()) {
			q.addFilter(new JSONObject().put("query_string", 
					new JSONObject()
						.put("fields", Arrays.asList("refs*"))
						.put("query", StringUtils.join(references, "OR"))));
		}
		return q;
	}

	
	
	/*
	 * For POI we are looking for 
	 * 
	 * Екатеринбург автозапчасти
	 * Екатеринбург Макдональдс
	 * 
	 * Макдональдс
	 * Автозапчасти
	 * 
	 * at least one of the following should match (
	 * 		poi class
	 * 		(poi name, poi brand, poi operator)
	 * )
	 * and optional boost for
	 * 
	 * city match
	 * street match
	 * 
	 */
	
	
	private POIClassesQueryResults queryPois(QToken prefixT, 
			List<String> allRequiredTokenStrings, String[] sourceFields) {
		
		Collection<String> poiClasses = new HashSet<>();
		Collection<String> termsMatchingTypes = new HashSet<>();
		boolean prefixMatchType = false;
		
		String poiTypeQ = getPoiTypeQuery(prefixT, allRequiredTokenStrings).toString();
		
		SearchRequestBuilder poiQueryRequestBuilder = ESServer.getInstance().client()
				.prepareSearch(IndexHolder.POI_CLASS_INDEX)
				.setTypes(IndexHolder.POI_CLASS_TYPE)
				.setFetchSource(new String[] {"name"}, new String[] {"json.address.parts.names"})
				.setQuery(QueryBuilders.wrapperQuery(poiTypeQ));
		
		SearchResponse searchResponse = poiQueryRequestBuilder.get();
		
		for(SearchHit hit : searchResponse.getHits()) {
			HashSet<String> matched = new HashSet<>(Arrays.asList(hit.getMatchedQueries()));
			if (!prefixMatchType) {
				prefixMatchType = matched.remove("_prefix");
				termsMatchingTypes.addAll(matched);
			}
			poiClasses.add(hit.getSourceAsMap().get("name").toString()); 
		}
		
		// ---- poi classes part ^^ 
		//TODO below is pois query itself
		//TODO move pois query to coallesce queries generation ---
		
		BooleanPart bool = new BooleanPart();
		bool.setName("matchByTags");
		bool.addFilter(new TermsPart("type", Arrays.asList("poipnt")));
		
		BooleanPart prefixBoolean = null;
		if (prefixT != null) {
			prefixBoolean = buildPrefixOverMultipleFields(prefixT, 
					Arrays.asList("name", "more_tags.brand", "more_tags.operator"));
		}
		
		for (String termForType : termsMatchingTypes) {
			allRequiredTokenStrings.remove(termForType);
		}
		
		JSONObject requiredTerms = buildPOIRequiredTerms(allRequiredTokenStrings);

		if (prefixBoolean != null && !prefixMatchType) {
			bool.addMust(prefixBoolean);
		}
		bool.addMust(requiredTerms);
		
		if (!poiClasses.isEmpty()) {
			bool.addFilter(new TermsPart("poi_class", poiClasses));
		}
		
		return new POIClassesQueryResults(poiClasses, bool, termsMatchingTypes, prefixMatchType);
	}

	private JSONObject buildPOIRequiredTerms(List<String> allRequiredTokenStrings) {
		JSONObject multiMatch = new JSONObject();
		multiMatch.put("multi_match", new JSONObject()
				.put("type", "cross_fields")
				.put("query", StringUtils.join(allRequiredTokenStrings, " "))
				.put("minimum_should_match", allRequiredTokenStrings.size())
				.put("fields", Arrays.asList("name^5", "more_tags.brand", "more_tags.operator", "street", "locality"))
		);
		return multiMatch;
	}

	private BooleanPart buildPrefixOverMultipleFields(QToken prefixT, List<String> fields) {
		BooleanPart result = new BooleanPart();
		
		for(String field : fields) {
			result.addShould(new JSONObject().put("prefix", 
					new JSONObject().put(field, new JSONObject()
						.put("value", prefixT.toString())
						.put("_name", "_prefix:" + field)
			)));
			result.setMinimumShouldMatch(1);
		}
		
		return result;
	}

	private JSONObject getPoiTypeQuery(QToken prefixT, List<String> terms) {
		BooleanPart bool = new BooleanPart();
		
		for(String term : terms) {
			bool.addShould(new JSONObject().put("term", 
					new JSONObject().put("title", 
							new JSONObject()
								.put("term", term)
								.put("_name", term))));
		}
		
		if (prefixT != null) {
			
			bool.addShould(new JSONObject()
					.put("prefix", new JSONObject()
						.put("title", new JSONObject()
								.put("value", prefixT.toString())
								.put("_name", "_prefix")
						)
					));
		}
		return bool.getPart(); 
	}

	private int trimResponse(SearchResponse response) {
		
		int index = 0;
		for (SearchHit hit : response.getHits()) {
			Set<String> matchedQueries = new HashSet<>(Arrays.asList(hit.getMatchedQueries()));
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			String type = sourceAsMap.get("type").toString();
			
			boolean localityMatched = matchedQueries.contains(MATCH_LOCALITY_QUERY_NAME);
			localityMatched = localityMatched || matchedQueries.stream().filter(s -> s.startsWith("locality_prefix")).findAny().isPresent();

			boolean streetMatched = matchedQueries.contains(MATCH_STREET_QUERY_NAME);
			streetMatched = streetMatched || matchedQueries.stream().filter(s -> s.startsWith("street_prefix")).findAny().isPresent();
			
			boolean isHighway = "hghway".equals(type) || "hghnet".equals(type);
			boolean isAddress = "adrpnt".equals(type);

			boolean houseumberMatched = matchedQueries.contains("housenumber_match");
			
			if (localityMatched && !streetMatched && isHighway) {
				return index;
			}
			
			if (localityMatched && streetMatched && !houseumberMatched && isAddress) {
				return index;
			}
			
			index ++;
		}
		
		return index;
	}

	private void writeHits(ResultsWrapper results, SearchResponse response) {
		for (SearchHit hit : response.getHits()) {
			writeHit(results, hit);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void writeHit(ResultsWrapper results, SearchHit hit) {
		Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		Double baseScore = Double.valueOf(sourceAsMap.get("base_score").toString());
		
		String resultFullText = sourceAsMap.get("full_text").toString();
		Map<?,?> centoidfield = (Map<?, ?>) sourceAsMap.get("centroid");
		
		Map<String, ?> jsonAsMap = (Map<String, ?>)sourceAsMap.get("json");
		Collection<String> poiClasses = (Collection<String>) sourceAsMap.get("poi_class");
		Map addressAsMap = (Map) jsonAsMap.get("address");
		String name = (String) jsonAsMap.get("name");
		
		results.addResultsRow(
				hit.getScore(),
				baseScore,
				name,
				addressAsMap,
				resultFullText, 
				sourceAsMap.get("osm_id").toString(),
				new GeoPoint(asDouble(centoidfield.get("lat")), asDouble(centoidfield.get("lon"))),
				hit.getMatchedQueries(),
				(Map)sourceAsMap.get("refs"),
				poiClasses);
	}
	
	private static double asDouble(Object v) {
		if (v instanceof Double) {
			return (Double) v;
		}
		if (v instanceof Integer) {
			return ((Integer) v).doubleValue();
		}
		return 0.0;
	}

}
