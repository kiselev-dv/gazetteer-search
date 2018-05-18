package me.osm.gazetteer.psqlsearch.api.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
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
import me.osm.gazetteer.psqlsearch.esclient.AddressesIndexHolder;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;
import me.osm.gazetteer.psqlsearch.imp.es.DistinctNameFilter;
import me.osm.gazetteer.psqlsearch.query.QToken;
import me.osm.gazetteer.psqlsearch.query.Query;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzer;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzerImpl;

public class ESDefaultSearch implements Search {

	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();

	private double ovarallHNQueryBoost = 1.0;
	private double hnArrayQBoost = 20.0;
	private double rangeHNQBoost = 0.01;
	
	private double mainMatchAdrpntBoost = 0.8;
	
	private double prefixScoreScale = 100.0;
	private double prefixEmptyNameLength = 5.0;
	private double prefixPlcpntBoost = 5.0;
	private double prefixRefBoost = 0.005;
	
	@Override
	public ResultsWrapper search(String queryString, int page, int pageSize, SearchOptions options) {

		Query query = analyzer.getQuery(queryString);
		List<QToken> tokens = query.listToken();
		
		ResultsWrapper results = new ResultsWrapper(queryString, page, pageSize);
		
		if (tokens.isEmpty()) {
			return results;
		}
		
		ESQueryPart prefixPart = null;
		QToken prefixT = null;
		
		if (options.isWithPrefix() && !queryString.endsWith(" ")) {
			prefixT = query.findPrefix();
		}

		if (prefixT != null) {
			
			if (query.countTokens() == 0) {
				prefixPart = new Prefix(prefixT.toString(), "name");
			}
			else {
				prefixPart = new Prefix(prefixT.toString(), "full_text");
			}
			
			Map<String, Object> params = new HashMap<>();
			
			params.put("scale", prefixScoreScale);
			params.put("empty_name_length", prefixEmptyNameLength);
			params.put("plcpnt_boost", prefixPlcpntBoost);
			params.put("ref_boost", prefixRefBoost);
			
			String prefixScoreScript = 
					  "params.scale * "
					+ "doc['base_score'].value / "
					+ "(doc['name_length'].value == 0.0 ? params.empty_name_length : doc['name_length'].value) * "
					+ "(doc['type'].value == 'plcpnt' ? params.plcpnt_boost : 1.0)"
					+ " * (doc['ref'].value != null ? params.ref_boost : 1.0)";
			
			prefixPart = new CustomScore(prefixPart, prefixScoreScript, params);
		}
		
		List<QToken> numberTokens = new ArrayList<>();
		List<QToken> optionalTokens = new ArrayList<>();
		List<QToken> requiredTokens = new ArrayList<>();
		List<String> requiredVariants = new ArrayList<>();
		
		tokens.stream().filter(t -> t.isHasNumbers()).forEach(numberTokens::add);
		tokens.stream().filter(t -> t.isOptional() && !t.isHasNumbers()).forEach(optionalTokens::add);
		tokens.stream().filter(t -> !t.isOptional() && !t.isHasNumbers()).forEach(t -> {
			requiredTokens.add(t);

			if(t.isFuzzied()) {
				requiredVariants.addAll(t.getVariants());
			}
		});
		
		
		JSONObject housenumber = buildHousenumberQ(options.isRangeHouseNumbers(), numberTokens);
		
		BooleanPart mainBooleanPart = new BooleanPart();
		
		if (!requiredTokens.isEmpty()) {
			JSONObject multimatch = buildMultyMatchQuery(
					requiredTokens, prefixT, numberTokens, options.isFuzzy());
			mainBooleanPart.addMust(multimatch);
		}
		
		if (housenumber != null) {
			mainBooleanPart.addShould(housenumber);
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
		
		ESQueryPart finalQ = mainBooleanPart;

		results.setDebugQuery(finalQ.getPart().toString(2));
		results.setParsedQuery(query.print());
		
		try {
			SearchResponse response = ESServer.getInstance().client()
					.prepareSearch(AddressesIndexHolder.INDEX_NAME)
					.addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
					.setTypes(AddressesIndexHolder.ADDR_ROW_TYPE)
					.setQuery(QueryBuilders.wrapperQuery(finalQ.getPart().toString()))  
					.setFrom(0).setSize(10)
					.get();
			
			results.setTotalHits(response.getHits().totalHits);
			
			writeHits(results, response);
			
		}
		catch (Exception e) {
			results.setErrorMessage(e.getMessage());
			e.printStackTrace();
		}
		
		return results;
	}

	private JSONObject buildMultyMatchQuery(List<QToken> requiredTokens, QToken prefixT, List<QToken> numberTokens, boolean fuzzy) {
		
		JSONArray termTopQArray = new JSONArray();
		
		JSONObject multimatch = new JSONObject().put("bool", new JSONObject()
				.put("should", termTopQArray)
				.put("_name", "required_terms"));

		int tokenCounter = 0;
		
		for (QToken qtoken : requiredTokens) {
			addToken(fuzzy, termTopQArray, qtoken);
			tokenCounter++;
		}
		
		if (prefixT != null) {
			
			Prefix localityPrefix = new Prefix(prefixT.toString(), "locality");
			localityPrefix.setName("locality_prefix:" + prefixT.toString());
			Prefix streetPrefix = new Prefix(prefixT.toString(), "street");
			streetPrefix.setName("street_prefix:" + prefixT.toString());
			
			JSONArray termOverFieldsShould = new JSONArray()
					.put(localityPrefix.getPart())
					.put(streetPrefix.getPart());
			
			termTopQArray.put(new JSONObject().put("bool", 
					new JSONObject().put("should", termOverFieldsShould)));
			
			tokenCounter++;
		}
		
		for (QToken qtoken : numberTokens) {
			if (qtoken.isFuzzied() && !qtoken.isNumbersOnly()) {
				addToken(false, termTopQArray, qtoken);
				tokenCounter++;
			}
		}
		
		int shouldMatch = tokenCounter;
		if (tokenCounter > 2) {
			shouldMatch = 2;
		}
		if (tokenCounter > 4) {
			shouldMatch = (int)(1.0 * tokenCounter * 0.6);
		}
		
		multimatch.getJSONObject("bool").put("minimum_should_match", shouldMatch);
		
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("adrpnt_boost", mainMatchAdrpntBoost);
		
		String script = "_score * (doc['type'].value == 'adrpnt' ? params.adrpnt_boost : 1.0)";
		
		return new CustomScore(multimatch, script, parameters).getPart();
		
	}

	private void addToken(boolean fuzzy, JSONArray termTopQArray, QToken qtoken) {
		String token = qtoken.toString();
		
		List<String> tokenValues = new ArrayList<>(Arrays.asList(token));
		if (qtoken.isFuzzied()) {
			tokenValues.addAll(qtoken.getVariants());
		}
				MatchPart matchLocality = new MatchPart("locality", tokenValues);
		matchLocality.setName("locality_token:" + token);
		
		MatchPart matchStreet = new MatchPart("street", tokenValues);
		matchStreet.setName("street_token:" + token);
		
		JSONArray termOverFieldsShould = new JSONArray()
				.put(matchLocality.getPart())
				.put(matchStreet.getPart());
		
		termTopQArray.put(new JSONObject().put("bool", 
				new JSONObject().put("should", termOverFieldsShould)));
		
		if (fuzzy) {
			matchLocality.setFuzziness("1");
			matchLocality.setName("locality_token_fuzzy:" + token);
			matchLocality.setBoost(0.75);
			
			matchStreet.setFuzziness("1");
			matchStreet.setName("street_token_fuzzy:" + token);
			matchStreet.setBoost(0.75);
			
			termOverFieldsShould.put(matchLocality.getPart());
			termOverFieldsShould.put(matchStreet.getPart());
		}
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

	private void writeHits(ResultsWrapper results, SearchResponse response) {
		
		for(SearchHit hit : response.getHits()) {
			writeHit(results, hit);
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
