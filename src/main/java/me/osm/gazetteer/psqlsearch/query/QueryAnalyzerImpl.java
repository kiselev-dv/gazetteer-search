package me.osm.gazetteer.psqlsearch.query;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAnalyzerImpl implements QueryAnalyzer {
	
	private static final Logger log = LoggerFactory.getLogger(QueryAnalyzerImpl.class);

	public static final String tokenSeparators = "«»<>, -——–;&:.\"()|[]№#";
	public static final String removeChars = "#?%*№@$'\"";
	
	private static final Pattern groupPattern = Pattern.compile("GROUP[0-9]+");
	
	public static final List<String[]> charReplaces = new ArrayList<>();
	static {
		charReplaces.add(new String[] {
				"ё", "е"
		});
	}
	
	public static final Set<String> optionals = new HashSet<String>(); 
	public static Pattern optRegexp = null;
	static {
		readOptionals();
	}
	
	public static final List<Replacer> streetReplacers = new ArrayList<>();
	public static final List<Replacer> hnReplacers = new ArrayList<>();
	static {
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
			
			String withoutNumbers = StringUtils.replaceChars(t, "0123456789", "");
			
			boolean hasNumbers = withoutNumbers.length() != t.length();
			boolean numbersOnly = StringUtils.isBlank(withoutNumbers);
			boolean optional = optionals.contains(StringUtils.lowerCase(t)) 
					|| (!hasNumbers && withoutNumbers.length() < 3)
					|| matchedOptTokens.contains(t);
			
			result.add(new QToken(t, variants, hasNumbers, numbersOnly, optional, matchedHN, matchedStreet));
		}
		
		Query query = new Query(result, original, varyOriginal(original));
		
		log.trace("Query: {}", query.print());
		
		return query;
	}
	
	@SuppressWarnings("unchecked")
	private static void readOptionals() {
		try {
			Set<String> patterns = new HashSet<>();
			File optCfg = new File("config/optional-terms/default.terms");
			for(String option : (List<String>)FileUtils.readLines(optCfg)) {
				if(!StringUtils.startsWith(option, "#") && !StringUtils.isEmpty(option)) {
					if(StringUtils.startsWith(option, "~")) {
						patterns.add(StringUtils.substringAfter(option, "~"));
					}
					else {
						optionals.add(StringUtils.lowerCase(option));
					}
				}
			}
			
			if(!patterns.isEmpty()) {
				List<String> t = new ArrayList<>(patterns.size());
				for(String s : patterns) {
					t.add("(" + s + ")");
				}
				
				optRegexp = Pattern.compile(StringUtils.join(t, "|"));
			}
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
