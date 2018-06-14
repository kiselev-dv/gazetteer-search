package me.osm.gazetteer.search.imp.addr;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.imp.DefaultScoreBuilder;
import me.osm.gazetteer.search.imp.ImportException;
import me.osm.gazetteer.search.imp.ImportOptions;
import me.osm.gazetteer.search.imp.POIIgnore;
import me.osm.gazetteer.search.imp.ScoreBuilder;
import me.osm.gazetteer.search.imp.poi.PoiInfo;
import me.osm.gazetteer.search.imp.poi.UpdatePOITagsAndClasses;
import me.osm.gazetteer.search.query.IndexAnalyzer;
import me.osm.gazetteer.search.query.IndexAnalyzer.Token;

public class ImportObjectParser {
	
	private static final Logger log = LoggerFactory.getLogger(ImportObjectParser.class);
	
	private IndexAnalyzer indexAnalyzer = new IndexAnalyzer();
	private ScoreBuilder scoreBuilder = new DefaultScoreBuilder();
	
	private Map<Integer, Integer> nameAggHghnet = new HashMap<>();
	private Map<Integer, Integer> nameAggHghway = new HashMap<>();

	private ImportOptions importOptions;
	private POIIgnore poiIgnore;

	private List<String> languages;

	private Set<String> skip;
	
	private UpdatePOITagsAndClasses poiParser = null;
	
	public ImportObjectParser(ImportOptions options) {
		this.importOptions = options;
		this.languages = importOptions.getLanguages();
		
		skip = new HashSet<>();
		skip.add("mtainf");
		
		if (importOptions.isSkipPoi()) {
			skip.add("poipnt");
		}
		else {
			poiParser = new UpdatePOITagsAndClasses(importOptions.getOSMDoc());
			poiIgnore = importOptions.getPoiCfg();
		}
	}

	public AddrRowWrapper parseAddress(JSONObject jsonObject) throws ImportException {
		String type = jsonObject.getString("type");
			
		try {
			
			
			if ("mtainf".equals(type)) {
				log.info("Import metainf: {}", jsonObject);
				return null;
			}
			
			String osm_type = jsonObject.optString("osm_type");
			if (osm_type == null || skip.contains(type)) {
				return null;
			}
			
			long osm_id = jsonObject.getLong("osm_id");
			
			NamesAccessor accessor = new NamesAccessor(jsonObject, languages);
			
			String fullText = accessor.getAddrFullText();
			
			String localityName = accessor.getLocality();
			List<Token> localityTokens = indexAnalyzer.normalizeLocationName(
					localityName, importOptions.isTranslit());

			String housenumber = jsonObject.optString("housenumber");
			String streetName = accessor.getStreet();
			List<Token> streetTokens = indexAnalyzer.normalizeStreetName(
					streetName, importOptions.isTranslit());
			
			JSONObject optTags = jsonObject.optJSONObject("tags");
			String name = accessor.getName();
			String ref = optTags != null ? optTags.optString("ref") : null;
			
			List<Token> nameTokens = indexAnalyzer.normalizeName(
					name, importOptions.isTranslit());
			List<Token> nameAltTokens = indexAnalyzer.normalizeName(
					accessor.getAltNames(), importOptions.isTranslit());
			
			boolean isPoi = "poipnt".equals(type);
			if (isPoi || !accessor.isEmpty()) {
				
				AddrRowWrapper subj = new AddrRowWrapper();
				
				subj.setImport(new ImportMeta(importOptions.getRegion(), 0, 0));
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
				
				subj.setStreetHasLocalityName(isStreetContainsLoc(streetTokens, localityTokens));
				
				List<Token> admin0 = indexAnalyzer.normalizeLocationName(
						accessor.getAdmin0(), importOptions.isTranslit());
				subj.setAdmin0(admin0);
				
				List<Token> admin1 = indexAnalyzer.normalizeLocationName(
						accessor.getAdmin1(), importOptions.isTranslit());
				subj.setAdmin1(admin1);
				
				List<Token> admin2 = indexAnalyzer.normalizeLocationName(
						accessor.getAdmin2(), importOptions.isTranslit());
				subj.setAdmin2(admin2);
				
				List<Token> localAdmin = indexAnalyzer.normalizeLocationName(
						accessor.getLocalAdminName(), importOptions.isTranslit());
				subj.setLocalAdmin(localAdmin);
				
				subj.setNeighbourhood(null);
				
				subj.setLocalityType(getLocalityType(jsonObject));

				fillLonLat(subj, jsonObject);
				
				if (isPoi) {
					List<String> classes = setPoiClasses(subj, jsonObject);
					subj.setHNMatch(jsonObject.optString("poi_addr_match"));
					if (classes != null) {
						if (poiIgnore != null) {
							boolean keep = poiIgnore.keep(classes, StringUtils.stripToNull(name) != null);
							if (!keep) {
								return null;
							}
						}
						
						if (poiParser != null) {
							PoiInfo poiInfo = poiParser.getInfo(jsonObject);
							subj.setPoiClassTranslated(poiInfo.getTranslatedPoiClasses());
							subj.setMoreTags(poiInfo.getMoreTags());
							subj.setPoiKeywords(poiInfo.getKeywords());
						}
					}
				}
				else {
					String[] split = StringUtils.splitByWholeSeparator(jsonObject.getString("id"), "--");
					subj.setAddrSchema(split.length > 1 ? split[1] : "regular");
				}
				
				fillNameAggIndex(type, nameTokens, subj);

				fillRefs(subj, jsonObject);
				
				DateTime dateTimeTimestamp = new DateTime(jsonObject.getString("timestamp"));
				subj.setTimestamp(new Timestamp(dateTimeTimestamp.getMillis()));
				subj.setSource(jsonObject);

				double score = scoreBuilder.getScore(subj);
				if (score == 0.0) {
					return null;
				}
				
				subj.setScoreBase(score);
				
				accessor.trimNames();
				
				return subj;
			}
			
		}
		catch (JSONException je) {
			je.printStackTrace();
		}

		return null;
	}

	private static boolean isStreetContainsLoc(List<Token> streetTokens, List<Token> localityTokens) {
		
		for (Token l : localityTokens) {
			for (Token s : streetTokens) {
				if(StringUtils.contains(s.token, l.token) || StringUtils.contains(l.token, s.token)) {
					return true;
				}
			}
		}
		
		return false;
	}

	private void fillNameAggIndex(String type, List<Token> nameTokens, AddrRowWrapper subj) {
		subj.setNameAggIndex(0);
		
		
		if ("hghnet".equals(type) || "hghway".equals(type)) {
			
			Map<Integer, Integer> agg = "hghnet".equals(type) ? nameAggHghnet : nameAggHghway;
			
			if(nameTokens.stream().filter(t -> !t.optional).count() > 0) {
				int nameHash = nameTokens.stream().filter(t -> !t.optional)
						.mapToInt(t -> t.token.hashCode()).reduce((i1, i2) -> i1 * i2).getAsInt();
				
				if(agg.get(nameHash) == null) {
					agg.put(nameHash, 0);
				}
				
				int i = agg.get(nameHash);
				subj.setNameAggIndex(i);
				
				agg.put(nameHash, i + 1);
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

	private List<String> setPoiClasses(final AddrRowWrapper subj, JSONObject jsonObject) {
		JSONArray classesJSON = jsonObject.optJSONArray("poi_class");
		if (classesJSON != null) {
			List<String> classes = readJSONTextArray(classesJSON);
			subj.setPoiClasses(classes);
			return classes;
		}
		return null;
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

	private void fillCommonField(AddrRowWrapper subj, 
			JSONObject jsonObject, String type, String osm_type, long osm_id) {
		
		subj.setId(jsonObject.getString("id"));
		subj.setType(type);
		subj.setFeatureId(jsonObject.getString("feature_id"));

		subj.setOSMId(osm_id);
		subj.setOSMType(osm_type);

	}

}
