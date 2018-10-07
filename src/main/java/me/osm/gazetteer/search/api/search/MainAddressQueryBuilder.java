package me.osm.gazetteer.search.api.search;

import java.util.ArrayList;
import java.util.Arrays;
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
import me.osm.gazetteer.search.backendquery.es.builders.Prefix;
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
			POIClassesQueryResults pois, 
			QueryBuilderFlags flags) {
		
		ESQueryPart prefixPart = buildPrefixPart(query, tokens);
		
		
		JSONObject housenumber = buildHousenumberQ(flags.housenumbersRange, tokens.numberTokens);
		
		BooleanPart mainBooleanPart = new BooleanPart();
		
		addNumberOfTermsFilter(mainBooleanPart, prefixPart, tokens.allRequired);
		
		JSONObject localityAndStreet = buildMultyMatchQuery(
				tokens.requiredTokens, 
				tokens.prefixT, 
				tokens.numberTokens, 
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
		
		TermsPart typeFilter = buildTypeFilter(tokens, pois, flags, housenumber, mainBooleanPart);
		
		mainBooleanPart.addMust(typeFilter);
		
		addOptionalTerms(tokens, mainBooleanPart);
		
		// Just for sorting
		mainBooleanPart.addShould(new MatchPart("full_text", tokens.allRequired).setBoost(1.2));
		
		mainBooleanPart.addShould(new MatchPart("admin0", tokens.allRequired).setName("admin0").setBoost(1.2));
		mainBooleanPart.addShould(new MatchPart("admin1", tokens.allRequired).setName("admin1"));
		mainBooleanPart.addShould(new MatchPart("admin2", tokens.allRequired).setName("admin2"));
		mainBooleanPart.addShould(new MatchPart("local_admin", tokens.allRequired).setName("local_admin"));
		
		return mainBooleanPart;
	}

	private static TermsPart buildTypeFilter(ParsedTokens tokens, POIClassesQueryResults pois, QueryBuilderFlags flags,
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

		if (pois != null && !pois.getClasses().isEmpty()) {
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
				mainBooleanPart.addShould(poiClassTerm);
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

	private static ESQueryPart buildPrefixPart(Query query, ParsedTokens tokens) {
		ESQueryPart prefixPart = null;

		if (tokens.prefixT != null) {
			if (query.countTokens() == 0) {
				prefixPart = new Prefix(tokens.prefixT.toString(), "name");
			}
			else {
				prefixPart = new Prefix(tokens.prefixT.toString(), "full_text");
			}
			
			prefixPart = new CustomScore(prefixPart, prefixScoreScript, prefixScoreScriptParams);
		}
		
		return prefixPart;
	}

	private static void addNumberOfTermsFilter(BooleanPart mainBooleanPart, ESQueryPart prefixPart, List<String> allRequired) {
		
		int total = (prefixPart != null ? 1 : 0) + allRequired.size();
		
		if (total >= 2) {
			BooleanPart termsCounter = new BooleanPart();

			if (prefixPart != null) {
				termsCounter.addShould(prefixPart);
			}
			
			for(String term : allRequired) {
				termsCounter.addShould(new JSONObject().put("match", new JSONObject()
						.put("full_text", new JSONObject()
								.put("query", term)
								.put("_name", "term:" + term))));
			}
			
			termsCounter.setMinimumShouldMatch(2);
			
			mainBooleanPart.addMust(termsCounter);
		}
		
		if (allRequired.size() <= 1) {
			mainBooleanPart.addMustNot(new StreetHasLocationFilter());
		}
		
	}

	private static JSONObject buildMultyMatchQuery(List<QToken> requiredTokens, QToken prefixT, 
			List<QToken> numberTokens, boolean fuzzy, boolean addNumberTokensToStreets, 
			boolean allMustMatch) {
		
		int numOfTokens = requiredTokens.size() + (prefixT == null ? 0 : 1);
		allMustMatch = allMustMatch && !requiredTokens.isEmpty() && numOfTokens > 1;
		
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
		
		if (prefixT != null) {
			tokenStrings.add(prefixT.toString());
		}
		
		if (numOfTokens >= 2) {
			JSONObject crossFields = new JSONObject().put("multi_match", new JSONObject()
					.put("query", StringUtils.join(tokenStrings, ' '))
					.put("fields", Arrays.asList("locality", "street"))
					.put("type", "cross_fields")
					.put("_name", "cross_fields")
					.put("boost", 1000.0));
			multimatch.addShould(crossFields);
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

}
