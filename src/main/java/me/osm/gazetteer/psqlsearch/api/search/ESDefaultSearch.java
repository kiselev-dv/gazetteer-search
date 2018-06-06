package me.osm.gazetteer.psqlsearch.api.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import me.osm.gazetteer.psqlsearch.api.ResultsWrapper;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.CustomScore;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.DistinctNameFilter;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.ESQueryPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.HousenumbersPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.MatchPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.Prefix;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.TermsPart;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;
import me.osm.gazetteer.psqlsearch.esclient.IndexHolder;
import me.osm.gazetteer.psqlsearch.query.QToken;
import me.osm.gazetteer.psqlsearch.query.Query;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzer;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzerImpl;

public class ESDefaultSearch implements Search {

	private static final String[] SOURCE_FIELDS_BASE = new String[] {
			"full_text", "osm_id", "json.name", "base_score", 
			"type", "centroid", "id", "json.address.text"};

	private static final String MATCH_STREET_QUERY_NAME = "match_street";

	private static final String MATCH_LOCALITY_QUERY_NAME = "match_locality";

	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();

	private static double ovarallHNQueryBoost = 1.0;
	private static double hnArrayQBoost = 50.0;
	private static double rangeHNQBoost = 0.01;
	
	private static double mainMatchAdrpntBoost = 0.8;
	private static double mainMatchHghnetBoost = 100.0;
	private static double mainMatchPlcpntBoost = 100.0;
	
	private static double prefixScoreScale = 100.0;
	private static double prefixEmptyNameLength = 5.0;
	private static double prefixPlcpntBoost = 5.0;
	private static double prefixRefBoost = 0.005;

	private static final String prefixScoreScript = 
			  "params.scale * "
			+ "doc['base_score'].value / "
			+ "(doc['name_length'].value == 0.0 ? params.empty_name_length : doc['name_length'].value) * "
			+ "(doc['type'].value == 'plcpnt' ? params.plcpnt_boost : 1.0)"
			+ " * (doc['ref'].value != null ? params.ref_boost : 1.0)";
	
	private static final Map<String, Object> prefixScoreScriptParams = new HashMap<>();
	
	static {
		prefixScoreScriptParams.put("scale", prefixScoreScale);
		prefixScoreScriptParams.put("empty_name_length", prefixEmptyNameLength);
		prefixScoreScriptParams.put("plcpnt_boost", prefixPlcpntBoost);
		prefixScoreScriptParams.put("ref_boost", prefixRefBoost);
	}
	
	private static class QueryBuilderFlags {
		
		public static final String FUZZY = "fuzzy";
		public static final String ONLY_ADDR_POINTS = "onlyAddrPoints";
		public static final String STREETS_WITH_NUMBERS = "streetsWithNumbers";
		public static final String HOUSNUMBERS_RANGE = "housenumbersRange";
		public static final String STREET_OR_LOCALITY = "streetOrLocality";

		public boolean onlyAddrPoints = false;
		public boolean fuzzy = false;
		public boolean streetsWithNumbers = false;
		public boolean housenumbersRange = false;
		public boolean streetOrLocality = false;
		
		public static QueryBuilderFlags getFlags(String ... flags ) {
			HashSet<String> set = new HashSet<>(Arrays.asList(flags));
			QueryBuilderFlags f = new QueryBuilderFlags();
			
			f.fuzzy = set.contains(FUZZY);
			f.onlyAddrPoints = set.contains(ONLY_ADDR_POINTS);
			f.streetsWithNumbers = set.contains(STREETS_WITH_NUMBERS);
			f.housenumbersRange = set.contains(HOUSNUMBERS_RANGE);
			f.streetOrLocality = set.contains(STREET_OR_LOCALITY);
			
			return f;
		}
	}
	
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
		
		Collection<String> poiClasses = Collections.emptyList();
		if (!options.isNoPoi()) {
			poiClasses = queryPoiClasses(prefixT, allRequiredTokenStrings);
		}

		List<JSONObject> coallesceQueries = new ArrayList<>();
		
		coallesceQueries.add(buildQuery(
				query, numberTokens, 
				optionalTokens, requiredTokens,
				allRequiredTokenStrings, prefixT, 
				QueryBuilderFlags.getFlags(QueryBuilderFlags.ONLY_ADDR_POINTS, QueryBuilderFlags.FUZZY)).getPart());
		
		coallesceQueries.add(buildQuery(
				query, numberTokens, 
				optionalTokens, requiredTokens,
				allRequiredTokenStrings, prefixT, 
				QueryBuilderFlags.getFlags(QueryBuilderFlags.STREETS_WITH_NUMBERS, QueryBuilderFlags.FUZZY)).getPart());
		
		coallesceQueries.add(buildQuery(query, numberTokens, optionalTokens, requiredTokens,
				allRequiredTokenStrings, prefixT, QueryBuilderFlags.getFlags(
						QueryBuilderFlags.FUZZY, QueryBuilderFlags.STREET_OR_LOCALITY)).getPart());
		
		String[] sourceFields = SOURCE_FIELDS_BASE;
		if (options.isVerboseAddress()) {
			sourceFields = ArrayUtils.add(SOURCE_FIELDS_BASE, "json.address");
		}
		ESCoalesce coalesce = new ESCoalesce(coallesceQueries, sourceFields);
		
		if (options.getLat() != null && options.getLon() != null) {
			coalesce.setDistanceSort(options.getLat(), options.getLon());
		}
		
		ResultsWrapper results = new ResultsWrapper(queryString, page, pageSize);
		results.setParsedQuery(query.print());
		results.setMatchedPoiClasses(poiClasses);
		
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

	private Collection<String> queryPoiClasses(QToken prefixT, List<String> allRequiredTokenStrings) {
		Collection<String> poiClasses = new HashSet<>();
		
		String poiTypeQ = getPoiTypeQuery(prefixT, allRequiredTokenStrings).toString();
		SearchRequestBuilder poiQueryRequestBuilder = ESServer.getInstance().client()
				.prepareSearch(IndexHolder.POI_CLASS_INDEX)
				.setTypes(IndexHolder.POI_CLASS_TYPE)
				.setFetchSource(new String[] {"name"}, new String[] {"json.address.parts.names"})
				.setQuery(QueryBuilders.wrapperQuery(poiTypeQ));
		
		SearchResponse searchResponse = poiQueryRequestBuilder.get();
		
		for(SearchHit hit : searchResponse.getHits()) {
			poiClasses.add(hit.getSourceAsMap().get("name").toString()); 
		}
		
		return poiClasses;
	}

	private JSONObject getPoiTypeQuery(QToken prefixT, List<String> terms) {
		BooleanPart bool = new BooleanPart();
		bool.addShould(new MatchPart("title", terms));
		if (prefixT != null) {
			bool.addShould(new JSONObject().put("prefix", 
					new JSONObject().put("title", prefixT.toString())));
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

	private BooleanPart buildQuery(Query query, List<QToken> numberTokens,
			List<QToken> optionalTokens, List<QToken> requiredTokens, List<String> allRequired, 
			QToken prefixT, QueryBuilderFlags flags) {
		
		ESQueryPart prefixPart = null;
		if (prefixT != null) {
			
			if (query.countTokens() == 0) {
				prefixPart = new Prefix(prefixT.toString(), "name");
			}
			else {
				prefixPart = new Prefix(prefixT.toString(), "full_text");
			}
			
			prefixPart = new CustomScore(prefixPart, prefixScoreScript, prefixScoreScriptParams);
		}
		
		
		JSONObject housenumber = buildHousenumberQ(flags.housenumbersRange, numberTokens);
		
		BooleanPart mainBooleanPart = new BooleanPart();
		
		JSONObject localityAndStreet = buildMultyMatchQuery(
				requiredTokens, prefixT, numberTokens, 
				flags.fuzzy, flags.streetsWithNumbers, !flags.streetOrLocality);
		
		mainBooleanPart.addMust(localityAndStreet);
			
		if (housenumber != null) {
			if(flags.onlyAddrPoints) {
				mainBooleanPart.addMust(housenumber);
			}
			else {
				mainBooleanPart.addShould(housenumber);
			}
		}
		
		if (prefixPart != null) {
			mainBooleanPart.addShould(prefixPart);
		}
		
		if (requiredTokens.isEmpty()) {
			mainBooleanPart.addMust(
					new TermsPart("type", Arrays.asList("plcpnt", "plcbnd", "hghnet")));
			
			// Get distinct by name values
			mainBooleanPart.addMust(new DistinctNameFilter());
		}
		else if (flags.onlyAddrPoints && housenumber != null) {
			mainBooleanPart.addMust(
					new TermsPart("type", Arrays.asList("adrpnt")));
		}
		else if (housenumber == null) {
			mainBooleanPart.addMust(
					new TermsPart("type", Arrays.asList("plcpnt", "plcbnd", "hghnet")));
		}
		else {
			mainBooleanPart.addMust(
					new TermsPart("type", Arrays.asList("plcpnt", "plcbnd", "hghnet", "adrpnt")));
		}
		
		if (!optionalTokens.isEmpty()) {
			mainBooleanPart.addShould(new MatchPart("street", tokensAsStringList(optionalTokens)));
			mainBooleanPart.addShould(new MatchPart("locality", tokensAsStringList(optionalTokens)));
			MatchPart namePart = new MatchPart("name", tokensAsStringList(optionalTokens));
			namePart.setBoost(2.0);
			mainBooleanPart.addShould(namePart);
		}
		
		mainBooleanPart.addShould(new MatchPart("admin0", allRequired).setName("admin0").setBoost(1.2));
		mainBooleanPart.addShould(new MatchPart("admin1", allRequired).setName("admin1"));
		mainBooleanPart.addShould(new MatchPart("admin2", allRequired).setName("admin2"));
		mainBooleanPart.addShould(new MatchPart("local_admin", allRequired).setName("local_admin"));
		
		return mainBooleanPart;
	}

	private JSONObject buildMultyMatchQuery(List<QToken> requiredTokens, QToken prefixT, 
			List<QToken> numberTokens, boolean fuzzy, boolean addNumberTokensToStreets, 
			boolean allMustMatch) {
		
		allMustMatch = allMustMatch && !requiredTokens.isEmpty();
		
		BooleanPart multimatch = new BooleanPart();
		multimatch.setName("required_terms");
		
		Set<String> tokenStrings = new HashSet<>();
		requiredTokens.stream().forEach(t -> {
			tokenStrings.add(t.toString());
			if (t.isFuzzied()) {
				tokenStrings.addAll(t.getVariants());
			}
		});
		
		ESQueryPart streetMatch = new MatchPart("street", tokenStrings);
		((MatchPart) streetMatch).setName(MATCH_STREET_QUERY_NAME);
		if (fuzzy) {
			((MatchPart) streetMatch).setFuzziness("1");
		}
		if (prefixT != null) {
			Prefix streetPrefix = new Prefix(prefixT.toString(), "street");
			streetPrefix.setName("street_prefix:" + prefixT.toString());
			
			BooleanPart or = new BooleanPart();
			or.addShould(streetMatch);
			or.addShould(streetPrefix);
			
			if (tokenStrings.isEmpty()) {
				streetMatch = streetPrefix;
			}
			else {
				streetMatch = or;
			}
		}
		if (allMustMatch) {
			multimatch.addMust(streetMatch);
		}
		else {
			multimatch.addShould(streetMatch);
		}
		
		for (QToken qtoken : numberTokens) {
			if (qtoken.isStreetMatched() || addNumberTokensToStreets) {
				multimatch.addMust(new MatchPart("street", qtoken.getVariants()).setName(MATCH_STREET_QUERY_NAME));
			}
		}
		
		ESQueryPart localityMatch = new MatchPart("locality", tokenStrings);
		((MatchPart) localityMatch).setName(MATCH_LOCALITY_QUERY_NAME);
		if (fuzzy) {
			((MatchPart) localityMatch).setFuzziness("1");
		}
		if (prefixT != null) {
			Prefix localityPrefix = new Prefix(prefixT.toString(), "locality");
			localityPrefix.setName("locality_prefix:" + prefixT.toString());
			
			BooleanPart or = new BooleanPart();
			or.addShould(localityMatch);
			or.addShould(localityPrefix);
			
			if (tokenStrings.isEmpty()) {
				localityMatch = localityPrefix;
			}
			else {
				localityMatch = or;
			}
		}
		if (allMustMatch) {
			multimatch.addMust(localityMatch);
		}
		else {
			multimatch.addShould(localityMatch);
		}

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("adrpnt_boost", mainMatchAdrpntBoost);
		parameters.put("hghnet_boost", mainMatchHghnetBoost * 10.0);
		parameters.put("plcpnt_boost", mainMatchPlcpntBoost);
		parameters.put("ref_boost", 0.0001);
		
		String script = "_score * " 
				+ "(doc['type'].value == 'adrpnt' ? params.adrpnt_boost : 1.0) * "
				+ "(doc['type'].value == 'hghnet' && doc['ref'].value == null ? params.hghnet_boost : 1.0) * "
				+ "(doc['type'].value == 'plcpnt' ? params.plcpnt_boost : 1.0)";
		
		return new CustomScore(multimatch, script, parameters).getPart();
		
	}

	private JSONObject buildHousenumberQ(boolean rangeHouseNumbers, List<QToken> numberTokens) {
		JSONObject housenumber = null;
		
		for (QToken t : numberTokens) {
			if (t.isStreetMatched()) {
				continue;
			}

			HousenumbersPart hn = new HousenumbersPart(
					t.toString(), 
					t.getVariants(), 
					getNumberPart(t.toString()), 
					rangeHouseNumbers);
			
			hn.setHnArrayQBoost(hnArrayQBoost);
			hn.setRangeHNQBoost(rangeHNQBoost);
			
			if (numberTokens.size() == 1) {
				housenumber = hn.getPart();
			}
			else if (housenumber == null) {
				housenumber = new JSONObject().put("dis_max", 
						new JSONObject().put("boost", ovarallHNQueryBoost).put("queries", 
								new JSONArray().put(hn.getPart())));
			}
			else {
				housenumber.getJSONObject("dis_max")
				.getJSONArray("queries").put(hn.getPart());
			}
		}
		
		if (housenumber != null) {
			housenumber.getJSONObject(housenumber.keys().next()).put("_name", "housenumber_match"); 
		}
		
		return housenumber;
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
		Map addressAsMap = (Map) jsonAsMap.get("address");
		String name = (String) jsonAsMap.get("name");
		
		results.addResultsRow(
				hit.getScore(),
				baseScore,
				name,
				addressAsMap,
				resultFullText, 
				sourceAsMap.get("osm_id").toString(),
				new GeoPoint(asDouble(centoidfield.get("lat")), (Double)centoidfield.get("lon")),
				hit.getMatchedQueries());
	}
	
	private double asDouble(Object v) {
		if (v instanceof Double) {
			return (Double) v;
		}
		if (v instanceof Integer) {
			return ((Integer) v).doubleValue();
		}
		return 0.0;
	}

	private List<String> tokensAsStringList(List<QToken> requiredTokens) {
		List<String> result = new ArrayList<>();
		for (QToken t : requiredTokens) {
			result.add(t.toString());
		}
		return result;
	}

	private Integer getNumberPart(String hnPrimary) {
		try {
			return Integer.valueOf(hnPrimary);
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

}
