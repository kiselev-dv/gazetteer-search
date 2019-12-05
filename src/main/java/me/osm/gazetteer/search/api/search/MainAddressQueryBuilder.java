package me.osm.gazetteer.search.api.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import me.osm.gazetteer.search.backendquery.es.builders.BooleanPart;
import me.osm.gazetteer.search.backendquery.es.builders.CustomScore;
import me.osm.gazetteer.search.backendquery.es.builders.DistinctNameFilter;
import me.osm.gazetteer.search.backendquery.es.builders.ESQueryPart;
import me.osm.gazetteer.search.backendquery.es.builders.HousenumbersPart;
import me.osm.gazetteer.search.backendquery.es.builders.MatchPart;
import me.osm.gazetteer.search.backendquery.es.builders.PrefixPart;
import me.osm.gazetteer.search.backendquery.es.builders.StreetHasLocationFilter;
import me.osm.gazetteer.search.backendquery.es.builders.TermsPart;
import me.osm.gazetteer.search.query.QToken;
import me.osm.gazetteer.search.query.Query;

public class MainAddressQueryBuilder {
	
	public static final String MATCH_STREET_QUERY_NAME = "match_street";
	public static final String MATCH_LOCALITY_QUERY_NAME = "match_locality";
	
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
	
	private static double matchedPOIClassBoost = 100000.0;
	
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
	
	public static class QueryBuilderFlags {
		
		public static final String FUZZY = "fuzzy";
		public static final String POIS_FIRST = "pois";
		public static final String ONLY_ADDR_POINTS = "onlyAddrPoints";
		public static final String STREETS_WITH_NUMBERS = "streetsWithNumbers";
		public static final String HOUSNUMBERS_RANGE = "housenumbersRange";
		public static final String STREET_OR_LOCALITY = "streetOrLocality";

		public boolean onlyAddrPoints = false;
		public boolean fuzzy = false;
		public boolean streetsWithNumbers = false;
		public boolean housenumbersRange = false;
		public boolean streetOrLocality = false;
		public boolean pois_first = false;
		
		public static QueryBuilderFlags getFlags(String ... flags ) {
			HashSet<String> set = new HashSet<>(Arrays.asList(flags));
			QueryBuilderFlags f = new QueryBuilderFlags();
			
			f.fuzzy = set.contains(FUZZY);
			f.onlyAddrPoints = set.contains(ONLY_ADDR_POINTS);
			f.streetsWithNumbers = set.contains(STREETS_WITH_NUMBERS);
			f.housenumbersRange = set.contains(HOUSNUMBERS_RANGE);
			f.streetOrLocality = set.contains(STREET_OR_LOCALITY);
			f.pois_first  = set.contains(POIS_FIRST);
			
			return f;
		}
		
	}
	
	public static class ParsedTokens {
		public List<QToken> numberTokens;
		public List<QToken> optionalTokens;
		public List<QToken> requiredTokens;
		public List<String> allRequired; 
		public QToken prefixT;
		
		public ParsedTokens(List<QToken> numberTokens, List<QToken> optionalTokens, 
				List<QToken> requiredTokens, List<String> allRequired, QToken prefixT) {

			this.numberTokens = numberTokens;
			this.optionalTokens = optionalTokens;
			this.requiredTokens = requiredTokens;
			this.allRequired = allRequired;
			this.prefixT = prefixT;
		}

	}
	
	public BooleanPart buildQuery(
			Query query, 
			ParsedTokens tokens, 
			POIClasses pois, 
			QueryBuilderFlags flags) {
		
		ESQueryPart prefixPart = buildPrefixPart(query, tokens, true);
		
		JSONObject housenumber = buildHousenumberQ(flags.housenumbersRange, tokens.numberTokens);
		
		BooleanPart mainBooleanPart = new BooleanPart();
		
		addNumberOfTermsFilter(mainBooleanPart, prefixPart, tokens.allRequired, flags.fuzzy);
		
		JSONObject localityAndStreet = buildMultyMatchQuery(
				tokens.requiredTokens, 
				tokens.prefixT, 
				tokens.numberTokens, 
				flags.fuzzy, flags.streetsWithNumbers, 
				!flags.streetOrLocality, pois);
		
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
		
		
		TermsPart typeFilter = buildTypeFilter(tokens, pois, flags, housenumber, mainBooleanPart);
		
		mainBooleanPart.addMust(typeFilter);
		
		addOptionalTerms(tokens, mainBooleanPart);
		
		// Just for sorting
		MatchPart fullTextMatch = buildFullTextMatch(tokens.allRequired, tokens.prefixT, tokens.numberTokens, flags.fuzzy);
		mainBooleanPart.addShould(fullTextMatch.setBoost(1.2));
		
		mainBooleanPart.addShould(new MatchPart("admin0", tokens.allRequired).setName("admin0").setBoost(1.2));
		mainBooleanPart.addShould(new MatchPart("admin1", tokens.allRequired).setName("admin1"));
		mainBooleanPart.addShould(new MatchPart("admin2", tokens.allRequired).setName("admin2"));
		mainBooleanPart.addShould(new MatchPart("local_admin", tokens.allRequired).setName("local_admin"));
		
		return mainBooleanPart;
	}
	
	private static BooleanPart buildPrefixOverMultipleFields(QToken prefixT, List<String> fields) {
		
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

	private static TermsPart buildTypeFilter(ParsedTokens tokens, POIClasses pois, QueryBuilderFlags flags,
			JSONObject housenumber, BooleanPart mainBooleanPart) {
		TermsPart typeFilter;
		if (tokens.requiredTokens.isEmpty()) {
			typeFilter = new TermsPart("type", 
					Arrays.asList("plcpnt", "plcbnd", "hghnet"));
			
			// Get distinct by name values
			mainBooleanPart.addMust(new DistinctNameFilter());
		}
		else if (flags.onlyAddrPoints && housenumber != null) {
			typeFilter = new TermsPart("type", 
					Arrays.asList("adrpnt"));
		}
		else if (housenumber == null) {
			typeFilter = new TermsPart("type", 
					Arrays.asList("plcpnt", "plcbnd", "hghnet"));
		}
		else {
			typeFilter = new TermsPart("type", 
					Arrays.asList("plcpnt", "plcbnd", "hghnet", "adrpnt"));
		}

		if (pois != null && !flags.onlyAddrPoints) {
			if (flags.pois_first) {
				typeFilter = new TermsPart("type", 
						Arrays.asList("poipnt"));
			}
			else {
				typeFilter.addValue("poipnt");
			}

			TermsPart poiClassTerm = new TermsPart("poi_class", pois.getClasses());
			
			if (flags.pois_first) {
				mainBooleanPart.addMust(poiClassTerm);
			}
			else {
				mainBooleanPart.addShould(new JSONObject().put("constant_score", new JSONObject()
						.put("filter", poiClassTerm.getPart())
						.put("boost", matchedPOIClassBoost) ));
			}
		}
		return typeFilter;
	}

	private static void addOptionalTerms(ParsedTokens tokens, BooleanPart mainBooleanPart) {
		if (!tokens.optionalTokens.isEmpty()) {
			mainBooleanPart.addShould(new MatchPart("street", tokensAsStringList(tokens.optionalTokens)));
			mainBooleanPart.addShould(new MatchPart("locality", tokensAsStringList(tokens.optionalTokens)));
			MatchPart namePart = new MatchPart("name", tokensAsStringList(tokens.optionalTokens));
			namePart.setBoost(2.0);
			mainBooleanPart.addShould(namePart);
		}
	}

	private ESQueryPart buildPrefixPart(Query query, ParsedTokens tokens, boolean pois) {
		ESQueryPart prefixPart = null;

		if (tokens.prefixT != null && !tokens.prefixT.isOptional()) {
			if(!pois) {
				if (query.countTokens() == 0) {
					prefixPart = new PrefixPart(tokens.prefixT.toString(), "name");
				}
				else {
					prefixPart = new PrefixPart(tokens.prefixT.toString(), "full_text");
				}
				((PrefixPart)prefixPart).setName("match_pefix");
			}
			else {
				prefixPart = buildPrefixOverMultipleFields(tokens.prefixT, 
						Arrays.asList("name", "full_text", "more_tags.brand", "more_tags.operator"));
				
				((BooleanPart)prefixPart).addShould(new JSONObject()
						.put("match", new JSONObject().put("name", new JSONObject()
								.put("query", tokens.prefixT.toString())
								.put("fuzziness", "1"))));
			}
			
			prefixPart = new CustomScore(prefixPart, prefixScoreScript, prefixScoreScriptParams);
		}
		
		return prefixPart;
	}

	/**
	 * At least two required terms of the query should match
	 * */
	private void addNumberOfTermsFilter(BooleanPart mainBooleanPart, 
			ESQueryPart prefixPart, List<String> allRequired, boolean fuzzy) {
		
		int total = (prefixPart != null ? 1 : 0) + allRequired.size();
		
		if (total >= 2) {
			BooleanPart termsCounter = new BooleanPart();

			if (prefixPart != null) {
				termsCounter.addShould(prefixPart);
			}
			
			for(String term : allRequired) {
				JSONObject termMatchQueryOptions = new JSONObject()
						.put("query", term)
						.put("_name", "term:" + term);
				
				if (fuzzy) {
					termMatchQueryOptions.put("fuzziness", "1");
				}
				
				termsCounter.addShould(new JSONObject().put("match", new JSONObject()
						.put("full_text", termMatchQueryOptions)));
			}
			
			termsCounter.setMinimumShouldMatch(2);
			termsCounter.setName("number_of_matched_terms_filter");
			
			mainBooleanPart.addMust(termsCounter);
		}
		
		if (allRequired.size() <= 1) {
			mainBooleanPart.addMustNot(new StreetHasLocationFilter());
		}
		
	}

	private JSONObject buildMultyMatchQuery(List<QToken> requiredTokens, QToken prefixT, 
			List<QToken> numberTokens, boolean fuzzy, boolean addNumberTokensToStreets, 
			boolean allMustMatch, POIClasses pois) {
		
		int numOfTokens = requiredTokens.size() + (prefixT == null || !prefixT.isOptional() ? 0 : 1);
		allMustMatch = allMustMatch && !requiredTokens.isEmpty() && numOfTokens > 1;
		
		BooleanPart multimatch = new BooleanPart();
		multimatch.setName("required_terms");
		
		Set<String> requiredTokenString = new HashSet<>();
		requiredTokens.stream().forEach(t -> {
			requiredTokenString.add(t.toString());
			if (t.isFuzzied()) {
				requiredTokenString.addAll(t.getVariants());
			}
		});
		
		ESQueryPart streetMatch = new MatchPart("street", requiredTokenString);
		((MatchPart) streetMatch).setName(MATCH_STREET_QUERY_NAME);
		if (fuzzy) {
			((MatchPart) streetMatch).setFuzziness("1");
		}
		if (prefixT != null && !prefixT.isOptional()) {
			PrefixPart streetPrefix = new PrefixPart(prefixT.toString(), "street");
			streetPrefix.setName("street_prefix:" + prefixT.toString());
			
			// If the prefix is actually a full term
			((MatchPart)streetMatch).addTerm(prefixT.toString());
			
			BooleanPart or = new BooleanPart();
			or.addShould(streetMatch);
			or.addShould(streetPrefix);
			
			if (requiredTokenString.isEmpty()) {
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
		
		ESQueryPart localityMatch = new MatchPart("locality", requiredTokenString);
		((MatchPart) localityMatch).setName(MATCH_LOCALITY_QUERY_NAME);
		if (fuzzy) {
			((MatchPart) localityMatch).setFuzziness("1");
		}
		if (prefixT != null) {
			PrefixPart localityPrefix = new PrefixPart(prefixT.toString(), "locality");
			localityPrefix.setName("locality_prefix:" + prefixT.toString());
			
			// If the prefix is actually a full term
			((MatchPart)localityMatch).addTerm(prefixT.toString());
			
			BooleanPart or = new BooleanPart();
			or.addShould(localityMatch);
			or.addShould(localityPrefix);
			
			if (requiredTokenString.isEmpty()) {
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
		
		if (pois != null){
			
			JSONObject poiTagsMatch = new JSONObject();
			poiTagsMatch.put("multi_match", new JSONObject()
					.put("type", "cross_fields")
					.put("_name", "poi_cross_fields")
					.put("query", StringUtils.join(requiredTokenString, " "))
					.put("minimum_should_match", requiredTokenString.size())
					.put("fields", Arrays.asList("name^5", "more_tags.brand", "more_tags.operator"))
			);
			
			if (prefixT != null) {
				BooleanPart poiPrefix = 
						buildPrefixOverMultipleFields(prefixT, Arrays.asList("name", "more_tags.brand", "more_tags.operator"));
				poiPrefix.setName("poi_prefix:" + prefixT.toString());
				
				BooleanPart or = new BooleanPart();
				or.addShould(poiTagsMatch);
				or.addShould(poiPrefix);
				
				// If prefix is actually full
				or.addShould(new JSONObject().put("match", new JSONObject()
						.put("name", new JSONObject()
								.put("query", prefixT.toString())
								.put("fuzziness", 1) 
							)) 
						);
				
				if (requiredTokenString.isEmpty()) {
					if (allMustMatch) {
						multimatch.addMust(poiPrefix);
					}
					else {
						multimatch.addShould(poiPrefix);
					}
				}
				else {
					if (allMustMatch) {
						multimatch.addMust(or);
					}
					else {
						multimatch.addShould(or);
					}
				}
			}
		}
		
		if (numOfTokens >= 2) {
			if (fuzzy) {
				MatchPart crossFields = buildFullTextMatch(requiredTokenString, prefixT, numberTokens, fuzzy)
						.setBoost(1000.0).setName("cross_field");
				
				multimatch.addShould(crossFields);
				multimatch.setMinimumShouldMatch(0);
			}
			else {
				// cross_fields query doesn't support fuzzyness
				// TODO: left this part for backward compatibility,
				// it might not be in use.
				
				if (prefixT != null && prefixT.isOptional()) {
					requiredTokenString.add(prefixT.toString());
				}

				JSONObject crossFields = new JSONObject().put("multi_match", new JSONObject()
					.put("query", StringUtils.join(requiredTokenString, ' '))
					.put("fields", Arrays.asList("locality", "street", "name"))
					.put("type", "cross_fields")
					.put("_name", "cross_fields")
					.put("boost", 1000.0));
				
				multimatch.addShould(crossFields);
				multimatch.setMinimumShouldMatch(0);
			}
		}
		

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("adrpnt_boost", mainMatchAdrpntBoost);
		parameters.put("hghnet_boost", mainMatchHghnetBoost);
		parameters.put("plcpnt_boost", mainMatchPlcpntBoost * 100.0);
		parameters.put("ref_boost", 0.001);
		
		String script = "_score * " 
				+ "(doc['type'].value == 'adrpnt' ? params.adrpnt_boost : 1.0) * "
				+ "(doc['type'].value == 'hghnet' && doc['ref'].value == null ? params.hghnet_boost : 1.0) * "
				+ "(doc['type'].value == 'plcpnt' ? params.plcpnt_boost : 1.0) * "
				+ "(doc['type'].value == 'hghnet' && doc['ref'].value != null ? params.ref_boost : 1.0)";
		
		return new CustomScore(multimatch, script, parameters).getPart();
		
	}

	private static JSONObject buildHousenumberQ(boolean rangeHouseNumbers, List<QToken> numberTokens) {
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
	
	private static List<String> tokensAsStringList(List<QToken> requiredTokens) {
		List<String> result = new ArrayList<>();
		for (QToken t : requiredTokens) {
			result.add(t.toString());
		}
		return result;
	}

	private static Integer getNumberPart(String hnPrimary) {
		try {
			return Integer.valueOf(hnPrimary);
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Fuzzy by default
	 * */
	public BooleanPart buildFullTextQuery(
			List<String> allRequiredTokenStrings, QToken prefixT, 
			List<QToken> numberTokens, boolean fuzzy) {
		
		MatchPart fullTextMatch = buildFullTextMatch(allRequiredTokenStrings, prefixT, numberTokens, fuzzy);
		
		BooleanPart fuzzyFullText = new BooleanPart()
				.addFilter(new TermsPart("type", numberTokens.isEmpty() ? 
						Arrays.asList("hghnet", "hghway", "plcpnt", "plcbnd") : Arrays.asList("adrpnt") ))
				.addMust(fullTextMatch);
		
		return fuzzyFullText;
	}

	private MatchPart buildFullTextMatch(Collection<String> allRequiredTokenStrings, QToken prefixT,
			List<QToken> numberTokens, boolean fuzzy) {
		List<String> terms = new ArrayList<String>(allRequiredTokenStrings);
		if (prefixT != null && !prefixT.isOptional()) {
			terms.add(prefixT.toString());
		}
		if (numberTokens != null) {
			numberTokens.forEach(t -> terms.add(t.toString()));
		}
		
		MatchPart fullTextMatch = new MatchPart("full_text", terms).setMinimumShouldMatch(terms.size());
		if (fuzzy) {
			fullTextMatch.setFuzziness("1");
		}
		return fullTextMatch;
	}

}
