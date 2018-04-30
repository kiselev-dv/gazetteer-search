package me.osm.gazetteer.psqlsearch.imp;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.osm.gazetteer.psqlsearch.imp.es.AddrRowWrapper;
import me.osm.gazetteer.psqlsearch.query.IndexAnalyzer.Token;

public class DefaultScoreBuilder implements ScoreBuilder {

	@Override
	public double getScore(AddrRowWrapper obj) {
		
		double score = 1.0;
		
		String type = obj.getType();
		
		if ("hghnet".equals(type) || "hghway".equals(type)) {
			if (intersects(obj.getName(), obj.getLocality())) {
				
				// Such highways is a huge pain in the butt
				// I'm thinking to throw it away or at least mark them as garbadge
				score /= 10;
			}
		}
		
		return score;
	}
	
	private boolean intersects(List<Token> nameTokens, List<Token> localityNameTokens) {
		Set<String> localityStringTokens = new HashSet<>();
		for (Token t : localityNameTokens) {
			localityStringTokens.add(t.token);
		}
		
		for (Token t : nameTokens) {
			if (localityStringTokens.contains(t.token)) {
				return true;
			}
		}
		
		return false;
	}

}
