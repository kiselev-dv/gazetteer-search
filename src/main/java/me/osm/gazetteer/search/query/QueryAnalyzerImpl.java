package me.osm.gazetteer.search.query;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAnalyzerImpl implements QueryAnalyzer {
	
	private static final Logger log = LoggerFactory.getLogger(QueryAnalyzerImpl.class);
	
	// this characters used to brake the string into terms
	public static final String tokenSeparators;
	
	// this characters will be removed
	public static final String removeChars;
	
	// char level replaces like ё->е in russian or ß->ss for german
	public static final List<String[]> charReplaces = new ArrayList<>();
	
	// Optional terms
	public static final Set<String> optionals = new HashSet<String>(); 
	
	// One regexp for all optional regexps
	public static volatile Pattern optRegexp;

	private static final Pattern groupPattern = Pattern.compile("GROUP[0-9]+");
	
	// Terms synonims (for street names mainly)
	public static final Map<String, Set<String>> synonims = new HashMap<>();

	// Regexp synonims expansions for streets
	public static final List<Replacer> streetReplacers = new ArrayList<>();
	
	// Regexp synonims expansions for housenumbers
	public static final List<Replacer> hnReplacers = new ArrayList<>();
	
	// One regexp for all stop words and regexp
	public static volatile Pattern stopRegexp;
	
	static {
		try {
			File cfgFile = new File("config/QueryAnalizer.json");
			JSONObject cfg = new JSONObject(IOUtils.toString(new FileReader(cfgFile)));

			tokenSeparators = cfg.getString("tokenSeparators");
			removeChars = cfg.getString("removeChars");

			JSONObject charReplacesJson = cfg.optJSONObject("charReplaces");
			if (charReplacesJson != null) {
				for (String key : charReplacesJson.keySet()) {
					charReplaces.add(new String[] {key, charReplacesJson.getString(key)});
				}
			}
			
			JSONObject synonimsJson = cfg.optJSONObject("synonims");
			if (synonimsJson != null) {
				for(String key : synonimsJson.keySet()) {
					
					Set <String> synonimValues = new HashSet<>();
					
					synonimValues.add(key.toLowerCase());

					Object val = synonimsJson.get(key);
					
					if (val instanceof String) {
						synonimValues.add(((String) val).toLowerCase());
					}
					
					if (val instanceof JSONArray) {
						JSONArray valArr = (JSONArray) val;
						for (int i = 0; i < valArr.length(); i++) {
							String valS = valArr.getString(i);
							synonimValues.add(((String) valS).toLowerCase());
						}
					}
					
					synonims.put(key.toLowerCase(), synonimValues);
				}
			}
			
		}
		catch (JSONException e) {
			throw new RuntimeException("Can't parse config/QueryAnalizer.json cfg", e);
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException("Can't read cfg file config/QueryAnalizer.json", e);
		} catch (IOException e) {
			throw new RuntimeException("Can't read cfg file config/QueryAnalizer.json", e);
		}
		
		readOptionals();
		readStopWords();

		ReplacersCompiler.compile(streetReplacers, new File("config/replacers/search/requiredSearchReplacers"));
		ReplacersCompiler.compile(hnReplacers, new File("config/replacers/search/hnSearchReplacers"));
	}
	
	@Override
	public Query getQuery(String q) {
		
		if(null == q) {
			return null;
		}
		
		String original = q;
		
		q = q.toLowerCase();
		
		for(String[] r : charReplaces) {
			q = StringUtils.replace(q, r[0], r[1]);
		}
		
		Set<String> removed = new LinkedHashSet<>();
		Matcher stopMatcher = stopRegexp.matcher(q);
		while (stopMatcher.find()) {
			removed.add(stopMatcher.group(0));
		}

		q = stopRegexp.matcher(q).replaceAll("");

		LinkedHashMap<String, Collection<String>> group2variants = new LinkedHashMap<>();
		
		Set<String> streetMatches = new HashSet<>();
		Set<String> hnMatches = new HashSet<>();
		
		for(Replacer r : streetReplacers) {
			Map<String, Collection<String>> replaceGroups = r.replaceGroups(q);
			group2variants.putAll(replaceGroups);
			streetMatches.addAll(replaceGroups.keySet());
		}
		
		for(Replacer r : hnReplacers) {
			Map<String, Collection<String>> replaceGroups = r.replaceGroups(q);
			group2variants.putAll(replaceGroups);
			hnMatches.addAll(replaceGroups.keySet());
		}
		
		HashMap<String, String> groupAliases = new HashMap<>();
		
		int i = 0;
		for(Entry<String, Collection<String>> gk : group2variants.entrySet()) {
			String alias = "GROUP" + i++;
			groupAliases.put(alias, gk.getKey());

			q = StringUtils.replace(q, gk.getKey(), " " + alias);
		}
		
		Set<String> matchedOptTokens = new HashSet<>();

		if(optRegexp != null) {
			Matcher matcher = optRegexp.matcher(q);
			while(matcher.find()) {
				 String group = matcher.group(0);
				 for(String t : StringUtils.split(group, tokenSeparators)) {
					 matchedOptTokens.add(t);
				 }
			}
		}
		
		q = StringUtils.replaceChars(q, removeChars, null);
		
		String[] tokens = StringUtils.split(q, tokenSeparators);
		
		List<QToken> result = new ArrayList<QToken>(tokens.length);

		for(String t : tokens) {
			
			boolean matchedHN = false;
			boolean matchedStreet = false;

			List<String> variants = new ArrayList<>();
			if(StringUtils.startsWith(t, "GROUP")) {
				Matcher matcher = groupPattern.matcher(t);
				if(matcher.find()) {
					String matched = matcher.group();
					String groupKey = groupAliases.get(matched);
					if(groupKey != null) {
						String tail = StringUtils.remove(t, matched);
						t = groupKey + tail;
						variants = new ArrayList<>();
						for(String var : group2variants.get(groupKey)) {
							variants.add(var + tail);
						}
						matchedHN = hnMatches.contains(groupKey);
						matchedStreet = streetMatches.contains(groupKey);
					}
				}
			}
			
			if (synonims.get(t) != null) {
				matchedStreet = true;
				variants.addAll(synonims.get(t));
			}
			
			String withoutNumbers = StringUtils.replaceChars(t, "0123456789", "");
			
			boolean hasNumbers = withoutNumbers.length() != t.length();
			boolean numbersOnly = StringUtils.isBlank(withoutNumbers);
			boolean optional = optionals.contains(StringUtils.lowerCase(t)) 
					|| (!hasNumbers && withoutNumbers.length() < 3)
					|| matchedOptTokens.contains(t);
			
			result.add(new QToken(t, variants, hasNumbers, numbersOnly, optional, matchedHN, matchedStreet));
		}
		
		Query query = new Query(result, original, varyOriginal(original), removed);
		
		log.trace("Query: {}", query.print());
		
		return query;
	}
	
	private static void readStopWords() {
		Set<String> patterns = new HashSet<>();
		File dir = new File("config/stop-terms/");
		
		for(String line : readTermsD(dir)) {
			patterns.add(StringUtils.substringAfter(line, "~"));
		}
		
		if(!patterns.isEmpty()) {
			List<String> t = new ArrayList<>(patterns.size());
			for(String s : patterns) {
				t.add("(" + s + ")");
			}
			
			stopRegexp = Pattern.compile(StringUtils.join(t, "|"), Pattern.CASE_INSENSITIVE);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void readOptionals() {
		Set<String> patterns = new HashSet<>();
		File dir = new File("config/optional-terms/");
		
		for(String line : readTermsD(dir)) {
			if(StringUtils.startsWith(line, "~")) {
				patterns.add(StringUtils.substringAfter(line, "~"));
			}
			else {
				optionals.add(StringUtils.lowerCase(line));
			}
		}
		
		if(!patterns.isEmpty()) {
			List<String> t = new ArrayList<>(patterns.size());
			for(String s : patterns) {
				t.add("(" + s + ")");
			}
			
			optRegexp = Pattern.compile(StringUtils.join(t, "|"), Pattern.CASE_INSENSITIVE);
		}
	}

	private static Set<String> readTermsD(File dir) {
		try {
			LinkedHashSet<String> lines = new LinkedHashSet<>();
			for (File f : dir.listFiles((d, name) -> name.endsWith(".terms"))) {
				for(String option : (List<String>)FileUtils.readLines(f)) {
					if(!StringUtils.startsWith(option, "#") && !StringUtils.isEmpty(option)) {
						lines.add(option);
					}
				}
			}
			return lines;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Collection<String> varyOriginal(String original) {
		Collection<String> result = new ArrayList<>();
		
		result.add(original);
		
		String replaced = original;
		for(String[] r : charReplaces) {
			replaced = StringUtils.replace(replaced, r[0], r[1]);
		}
		result.add(replaced);
		
		replaced = StringUtils.replaceChars(replaced, ".,", "");
		result.add(replaced);

		result.add(StringUtils.capitalize(replaced));
		
		result.add(StringUtils.upperCase(replaced));

		result.add(StringUtils.lowerCase(replaced));
		
		return result;
	}
	
}
