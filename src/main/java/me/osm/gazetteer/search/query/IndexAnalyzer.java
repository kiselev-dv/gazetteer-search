package me.osm.gazetteer.search.query;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;

import gcardone.junidecode.Junidecode;

public class IndexAnalyzer {
	
	private static final int MINIMAL_MEANING_TERM_LENGTH = 3;
	private List<Replacer> hnReplacers = new ArrayList<>(); 
	private List<Replacer> streetsReplacers = new ArrayList<>();
	private List<Replacer> localityReplacers = new ArrayList<>();
	
	public IndexAnalyzer() {
		ReplacersCompiler.compile(hnReplacers, new File("config/replacers/index/hnIndexReplasers"));
		ReplacersCompiler.compile(streetsReplacers, new File("config/replacers/index/streetsReplacers"));
		ReplacersCompiler.compile(localityReplacers, new File("config/replacers/index/localityReplacers"));
	} 
	
	public String toASCII(String original) {
		return Junidecode.unidecode(original);
	}
	
	public static final class Token {
		public String token;
		public boolean optional;
		
		public Token(String token, boolean optional) {
			this.token = token;
			this.optional = optional;
		}
	}
	
	public List<Token> normalizeLocationName(String original) {
		return listTokens(original, localityReplacers);
	}
	
	public List<Token> normalizeStreetName(String original) {
		return listTokens(original, streetsReplacers);
	}
	
	public List<Token> normalizeName(String original) {
		return listTokens(original, streetsReplacers);
	}

	private Set<String> findOptionals(Set<String> uniqueTokens) {
		Set<String> matchedOptTokens = new HashSet<>();
		if(QueryAnalyzerImpl.optRegexp != null) {
			Matcher matcher = QueryAnalyzerImpl.optRegexp.matcher(StringUtils.join(uniqueTokens, ' '));
			while(matcher.find()) {
				 String group = matcher.group(0);
				 for(String t : StringUtils.split(group, QueryAnalyzerImpl.tokenSeparators)) {
					 matchedOptTokens.add(t);
				 }
			}
		}
		uniqueTokens.stream().filter(t -> QueryAnalyzerImpl.optionals.contains(t)).forEach(matchedOptTokens::add);
		
		return matchedOptTokens;
	}
	
	private List<Token> listTokens(String original, List<Replacer> replacers) {
		original = StringUtils.stripToEmpty(original);
		
		String replaced = StringUtils.join(transform(original.toLowerCase(), replacers), ' ');
		replaced = original.toLowerCase() + " " + replaced;
		
		String filtered = StringUtils.replaceChars(replaced, QueryAnalyzerImpl.removeChars, null);
		String transformed = filtered;
		
		List<String> optionals = new ArrayList<>();
		// Mark tokens in braces as optionals
		filtered = filterOptionals(filtered, optionals, "(", ")");
		filtered = filterOptionals(filtered, optionals, "[", "]");
		filtered = filterOptionals(filtered, optionals, "<", ">");
		filtered = filterOptionals(filtered, optionals, "{", "}");
		
		String[] tokensArr = StringUtils.split(filtered, QueryAnalyzerImpl.tokenSeparators);
		LinkedHashSet<String> tokens = new LinkedHashSet<>(Arrays.asList(tokensArr)); 
		
		String optionalsAsString = StringUtils.join(optionals, ' ');
		String[] optTokensArr = StringUtils.split(optionalsAsString, QueryAnalyzerImpl.tokenSeparators);
		HashSet<String> optTokens = new HashSet<>(Arrays.asList(optTokensArr));
		
		// Look for optional tokens in the rest of the tokens
		optTokens.addAll(findOptionals(tokens));
		
		List<Token> result = new ArrayList<>();
		for (String token : StringUtils.split(transformed, QueryAnalyzerImpl.tokenSeparators)) {
			String text = StringUtils.stripToNull(token);
			if (text != null) {
				boolean hasNumbers = StringUtils.containsAny(text, "0123456789");
				
				boolean optional = optTokens.contains(text);
				if (!hasNumbers && text.length() < MINIMAL_MEANING_TERM_LENGTH) {
					optional = true;
				}
				
				result.add(new Token(text, optional));
				
			}
		}
		
		return result;
	}

	private String filterOptionals(String filtered, List<String> optionals, String open, String close) {
		String[] opt = StringUtils.substringsBetween(filtered, open, close);
		if (opt != null) {
			for (String s : opt) {
				filtered = StringUtils.remove(filtered, s);
				optionals.add(s);
			}
		}
		return filtered;
	}

	public Collection<String> getHNVariants(String original) {
		Collection<String> variants = transform(original, hnReplacers);
		if (variants.isEmpty()) {
			variants.add(original);
		}
		return variants;
	}
	
	private Collection<String> transform(String optString, Collection<Replacer> replacers) {
		
		for(String [] replacer : QueryAnalyzerImpl.charReplaces) {
			optString = StringUtils.replace(optString, replacer[0], replacer[1]);
		}
		
		Set<String> result = new HashSet<>(); 
		for(Replacer replacer : replacers) {
			try {
				Collection<String> replace = replacer.replace(optString);
				if(replace != null) {
					for(String s : replace) {
						if(StringUtils.isNotBlank(s) && !"null".equals(s)) {
							result.add(s);
						}
					}
				}
			}
			catch (Exception e) {
				
			}
		}
		
		return result;
	}

}
