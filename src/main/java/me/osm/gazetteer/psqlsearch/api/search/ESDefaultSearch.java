package me.osm.gazetteer.psqlsearch.api.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import me.osm.gazetteer.psqlsearch.api.ResultsWrapper;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.CustomScore;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.ESQueryPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.HousenumbersPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.MatchPart;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.Prefix;
import me.osm.gazetteer.psqlsearch.backendquery.es.builders.TermsPart;
import me.osm.gazetteer.psqlsearch.imp.es.DistinctNameFilter;
import me.osm.gazetteer.psqlsearch.query.QToken;
import me.osm.gazetteer.psqlsearch.query.Query;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzer;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzerImpl;

public class ESDefaultSearch implements Search {

	private static final String MATCH_STREET_QUERY_NAME = "match_street";

	private static final String MATCH_LOCALITY_QUERY_NAME = "match_locality";

	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();

	private static double ovarallHNQueryBoost = 1.0;
	private static double hnArrayQBoost = 50.0;
	private static double rangeHNQBoost = 0.01;
	
	private static double mainMatchAdrpntBoost = 0.8;
	
	private static double prefixScoreScale = 100.0;
	private static double prefixEmptyNameLength = 5.0;
	private static double prefixPlcpntBoost = 5.0;
	private static double prefixRefBoost = 0.005;

	private static double mainMatchFuzzyBoost = 0.75;
	
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
		

		List<JSONObject> coallesceQueries = new ArrayList<>();
		
		BooleanPart q1 = buildQuery(
				query, numberTokens, 
				optionalTokens, requiredTokens,
				allRequiredTokenStrings, prefixT, 
				QueryBuilderFlags.getFlags(QueryBuilderFlags.ONLY_ADDR_POINTS, QueryBuilderFlags.FUZZY));
		q1.setName("fuzzy_only_addrpnt");
		coallesceQueries.add(q1.getPart());
		
		BooleanPart q2 = buildQuery(query, numberTokens, optionalTokens, requiredTokens,
				allRequiredTokenStrings, prefixT, QueryBuilderFlags.getFlags(
						QueryBuilderFlags.FUZZY, QueryBuilderFlags.STREET_OR_LOCALITY));
		q2.setName("fuzzy_street_or_locality");
		coallesceQueries.add(q2.getPart());
		
		ESCoalesce coalesce = new ESCoalesce(coallesceQueries, new String[] {"full_text", "osm_id", "name", "base_score", "type"});
		
		ResultsWrapper results = new ResultsWrapper(queryString, page, pageSize);
		results.setParsedQuery(query.print());
		
		try {
			SearchResponse response = coalesce.execute(0, 100);
			int trim = trimResponse(response);
			
			results.setDebugQuery(coalesce.getExecutedQuery().toString(2));
			results.setTotalHits(response.getHits().totalHits);
			results.setTrim(trim);
			results.setQueryTime(coalesce.getQueryTime());
			
			writeHits(results, response, trim);
			
		}
		catch (Exception e) {
			results.setErrorMessage(e.getMessage());
			e.printStackTrace();
		}
		
		results.setAnswerTime(new Date().getTime() - startms);
		
		return results;
	}

	private int trimResponse(SearchResponse response) {
		
		int index = 0;
		for (SearchHit hit : response.getHits()) {
			Set<String> matchedQueries = new HashSet<>(Arrays.asList(hit.getMatchedQueries()));
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			String type = sourceAsMap.get("type").toString();
			boolean isHighway = "hghway".equals(type) || "hghnet".equals(type);
			if (matchedQueries.contains(MATCH_LOCALITY_QUERY_NAME) && !matchedQueries.contains(MATCH_STREET_QUERY_NAME) && isHighway) {
				return index - 1;
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
		else if (flags.onlyAddrPoints) {
			mainBooleanPart.addMust(
					new TermsPart("type", Arrays.asList("adrpnt")));
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
		
		if (addNumberTokensToStreets) {
			for (QToken qtoken : numberTokens) {
				if (qtoken.isFuzzied() && !qtoken.isNumbersOnly()) {
					multimatch.addMust(new MatchPart("street", qtoken.getVariants()));
				}
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
		parameters.put("plcpnt_boost", 100.0);
		
		String script = "_score * (doc['type'].value == 'adrpnt' ? params.adrpnt_boost : 1.0) * (doc['type'].value == 'plcpnt' ? params.plcpnt_boost : 1.0)";
		
		return new CustomScore(multimatch, script, parameters).getPart();
		
	}

	private JSONObject buildHousenumberQ(boolean rangeHouseNumbers, List<QToken> numberTokens) {
		JSONObject housenumber = null;
		
		for (QToken t : numberTokens) {
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
		
		return housenumber;
	}

	private void writeHits(ResultsWrapper results, SearchResponse response, int trim) {
		for (int i = 0; i < trim && i < response.getHits().totalHits; i++) {
			writeHit(results, response.getHits().getAt(i));
		}
	}

	private void writeHit(ResultsWrapper results, SearchHit hit) {
		Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		Double baseScore = Double.valueOf(sourceAsMap.get("base_score").toString());
		
		String resultFullText = sourceAsMap.get("full_text").toString();
		String resultName = sourceAsMap.get("name").toString();
		
		results.addResultsRow(
				hit.getScore(),
				baseScore,
				resultName + " (" + resultFullText + ")", 
				sourceAsMap.get("osm_id").toString(),
				hit.getMatchedQueries());
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
