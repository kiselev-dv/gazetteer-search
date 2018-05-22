package me.osm.gazetteer.psqlsearch.imp.es;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.psqlsearch.imp.DefaultScoreBuilder;
import me.osm.gazetteer.psqlsearch.imp.ScoreBuilder;
import me.osm.gazetteer.psqlsearch.imp.postgres.Importer.ImportException;
import me.osm.gazetteer.psqlsearch.query.IndexAnalyzer;
import me.osm.gazetteer.psqlsearch.query.IndexAnalyzer.Token;

public class ImportObjectParser {
	
	private static final Logger log = LoggerFactory.getLogger(ImportObjectParser.class);
	
	private IndexAnalyzer indexAnalyzer = new IndexAnalyzer();
	private ScoreBuilder scoreBuilder = new DefaultScoreBuilder();
	
	private Map<Integer, Integer> nameAgg = new HashMap<>();

	public AddrRowWrapper parseAddress(JSONObject jsonObject) throws ImportException {
		String type = jsonObject.getString("type");
			
		try {
			
			String osm_type = jsonObject.optString("osm_type");
			
			if ("mtainf".equals(type) || osm_type == null) {
				return null;
			}
			
			long osm_id = jsonObject.getLong("osm_id");
			
			JSONObject addrObject = jsonObject.optJSONObject("address");
			
			String fullText = getAddrFullText(addrObject);
			String localityName = jsonObject.optString("locality_name");
			List<Token> localityTokens = indexAnalyzer.normalizeLocationName(localityName);

			String housenumber = jsonObject.optString("housenumber");
			String streetName = jsonObject.optString("street_name");
			List<Token> streetTokens = indexAnalyzer.normalizeStreetName(streetName);
			
			JSONObject optTags = jsonObject.optJSONObject("tags");
			String name = optTags != null ? optTags.optString("name") : null;
			String ref = optTags != null ? optTags.optString("ref") : null;
			
			List<Token> nameTokens = indexAnalyzer.normalizeName(name);
			List<Token> nameAltTokens = indexAnalyzer.normalizeName(getAltNames(jsonObject));
			
			boolean isPoi = "poipnt".equals(type);
			if (isPoi || notEmpty(jsonObject)) {
				
				AddrRowWrapper subj = new AddrRowWrapper();
				
				fillCommonField(subj, jsonObject, type, osm_type, osm_id);
				
				subj.setFullText(fullText);
				
				subj.setName(nameTokens);
				subj.setNameAlt(nameAltTokens);
				subj.setRef(ref);

				subj.setHN(parseHousenumber(housenumber));
				subj.setHNExact(StringUtils.stripToNull(housenumber));
				subj.setHNVariants(indexAnalyzer.getHNVariants(housenumber));
				
				subj.setStreet(streetTokens);
				subj.setLocality(localityTokens);
				
				List<Token> admin0 = indexAnalyzer.normalizeLocationName(jsonObject.optString("admin0_name"));
				subj.setAdmin0(admin0);
				
				List<Token> admin1 = indexAnalyzer.normalizeLocationName(jsonObject.optString("admin1_name"));
				subj.setAdmin1(admin1);
				
				List<Token> admin2 = indexAnalyzer.normalizeLocationName(jsonObject.optString("admin2_name"));
				
				subj.setAdmin2(admin2);
				
				List<Token> localAdmin = indexAnalyzer.normalizeLocationName(jsonObject.optString("local_admin_name"));
				subj.setLocalAdmin(localAdmin);
				
				subj.setNeighbourhood(null);
				
				subj.setLocalityType(getLocalityType(jsonObject));

				fillLonLat(subj, jsonObject);
				
				
				if (isPoi) {
					setPoiClasses(subj, jsonObject);
					setPoiKeywords(subj, jsonObject);
					//more_tags
					
					// TODO
					subj.setHNMatch("exact");
				}
				else {
					subj.setAddrSchema("regular");
				}
				
				fillNameAggIndex(type, nameTokens, subj);

				/* TODO find is there any addresses connected
				 * to highway, and debuf them, not depends
				 * on ref existance
				 */
				fillRefs(subj, jsonObject);
				
				DateTime dateTimeTimestamp = new DateTime(jsonObject.getString("timestamp"));
				subj.setTimestamp(new Timestamp(dateTimeTimestamp.getMillis()));
				subj.setSource(jsonObject);

				double score = scoreBuilder.getScore(subj);
				if (score == 0.0) {
					return null;
				}
				
				subj.setScoreBase(score);

				return subj;
			}
			
		}
		catch (JSONException je) {
			je.printStackTrace();
		}

		return null;
	}

	private void fillNameAggIndex(String type, List<Token> nameTokens, AddrRowWrapper subj) {
		subj.setNameAggIndex(0);
		if ("hghnet".equals(type)) {
			
			if(nameTokens.stream().filter(t -> !t.optional).count() > 0) {
				int nameHash = nameTokens.stream().filter(t -> !t.optional)
						.mapToInt(t -> t.token.hashCode()).reduce((i1, i2) -> i1 * i2).getAsInt();
				
				if(nameAgg.get(nameHash) == null) {
					nameAgg.put(nameHash, 0);
				}
				
				int i = nameAgg.get(nameHash);
				subj.setNameAggIndex(i);
				
				nameAgg.put(nameHash, i + 1);
			}
		}
	}
	
	private String getLocalityType(JSONObject jsonObject) {
		return null;
	}

	private int parseHousenumber(String housenumber) {
		String string = StringUtils.stripToEmpty(housenumber);
		string = string.replaceAll("[^\\d.]", " ");
		Pattern pattern = Pattern.compile("[\\d]+");
		Matcher matcher = pattern.matcher(string);
		
		// Ok for now, just get the first match
		// it works not in all cases
		while(matcher.find()) {
			String group = matcher.group();
			try {
				return Integer.parseInt(group);
			}
			catch (Exception e) {
				return -1;
			}
		}
		return -1;
	}

	private void setPoiClasses(final AddrRowWrapper subj, JSONObject jsonObject) {
		JSONArray classesJSON = jsonObject.getJSONArray("poi_class");
		List<String> classes= readJSONTextArray(classesJSON);
		subj.setPoiClasses(classes);
	}

	private void setPoiKeywords(final AddrRowWrapper subj, JSONObject jsonObject) {
		JSONArray poiKeywords = jsonObject.getJSONArray("poi_keywords");
		List<String> keywords = readJSONTextArray(poiKeywords);
		subj.setPoiKeywords(keywords);
	}

	private List<String> readJSONTextArray(JSONArray poiKeywords) {
		List<String> keywords = new ArrayList<>();
		for(int i = 0; i < poiKeywords.length(); i++) {
			keywords.add(poiKeywords.getString(i));
		}
		return keywords;
	}

	private void fillLonLat(final AddrRowWrapper subj, JSONObject jsonObject) {
		JSONObject centroid = jsonObject.getJSONObject("center_point");
		subj.setLon(centroid.getDouble("lon"));
		subj.setLat(centroid.getDouble("lat"));
	}

	private void fillRefs(final AddrRowWrapper subj, JSONObject jsonObject) {
		JSONObject optJSONObject = jsonObject.optJSONObject("refs");
		if (optJSONObject != null) {
			JSONObject refs = new JSONObject();
			
			for (String key : optJSONObject.keySet()) {
				JSONArray values = new JSONArray();
				for(String ref : asStringList(optJSONObject, key)) {
					values.put(ref);
					String omsId = getOSMid(ref);
					if (omsId != null) {
						values.put(omsId);
					}
				}
				if (values.length() > 0) {
					refs.put(key, values);
				}
			}
			
			subj.setRefs(refs);
		}
		
	}

	private String getOSMid(String ref) {
		return StringUtils.split(ref, '-')[2];
	}

	private List<String> asStringList(JSONObject obj, String key) {
		List<String> result = new ArrayList<>();
		
		Object val = obj.get(key);
		if (val instanceof JSONArray) {
			((JSONArray)val).forEach(o -> result.add(o.toString()));
		}
		else if (val instanceof String) {
			result.add((String) val);
		}
		
		return result;
	}

	private String getAddrFullText(JSONObject addrObject) {
		return addrObject != null ? addrObject.optString("longText") : null;
	}

	private boolean notEmpty(JSONObject jsonObject) {
		
		JSONObject addrObject = jsonObject.optJSONObject("address");
		String fullText = getAddrFullText(addrObject);
		String localityName = jsonObject.optString("locality_name");
		String housenumber = jsonObject.optString("housenumber");
		String streetName = jsonObject.optString("street_name");
		
		return fullText != null && (housenumber != null || streetName != null || localityName != null);
	}

	private List<Token> requiredTokens(List<Token> tokens) {
		List<Token> result = new ArrayList<>();
		for (Token t : tokens) {
			if(!t.optional) {
				result.add(t);
			}
		}
		return result;
	}
	
	private List<Token> optionalTokens(List<Token> tokens) {
		List<Token> result = new ArrayList<>();
		for (Token t : tokens) {
			if(t.optional) {
				result.add(t);
			}
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private String getAltNames(JSONObject jsonObject) {
		JSONObject tags = jsonObject.optJSONObject("tags");

		if (tags != null) {
			List<String> names = new ArrayList<>();
			
			for (Iterator<String> iterator = tags.keys(); iterator.hasNext();) {
				String key = iterator.next();
				if (!"name".equals(key) && StringUtils.contains(key, "name")) {
					names.add(tags.getString(key));
				}
			}
			
			if(!names.isEmpty()) {
				return StringUtils.join(names, ' ');
			}
		}
		
		return null;
	}

	private void fillCommonField(AddrRowWrapper subj, 
			JSONObject jsonObject, String type, String osm_type, long osm_id) {
		
		subj.setId(jsonObject.getString("id"));
		subj.setType(type);
		subj.setFeatureId(jsonObject.getString("feature_id"));

		subj.setOSMId(osm_id);
		subj.setOSMType(osm_type);

	}

}
