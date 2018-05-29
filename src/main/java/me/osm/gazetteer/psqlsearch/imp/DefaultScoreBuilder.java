package me.osm.gazetteer.psqlsearch.imp;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.psqlsearch.imp.addr.AddrRowWrapper;
import me.osm.gazetteer.psqlsearch.query.IndexAnalyzer.Token;

public class DefaultScoreBuilder implements ScoreBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(DefaultScoreBuilder.class);
	
	private final JSONObject highwayTypeWeights;
	private final JSONObject placeTypeWeights;

	public DefaultScoreBuilder() {
		try {
			highwayTypeWeights = new JSONObject(IOUtils.toString(
					DefaultScoreBuilder.class.getClassLoader().getResourceAsStream("weights/highwayClasses.json")));
			
			placeTypeWeights = new JSONObject(IOUtils.toString(
					DefaultScoreBuilder.class.getClassLoader().getResourceAsStream("weights/placeClasses.json")));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public double getScore(AddrRowWrapper obj) {
		
		double score = 1.0;
		
		JSONObject source = obj.getSource();
		JSONObject tags = source.getJSONObject("tags");
		
		String type = obj.getType();
		
		if (isHighway(type)) {
			
			if (intersects(obj.getName(), obj.getLocality())) {
				
				// Such highways is a huge pain in the butt
				// I'm thinking to throw it away or at least mark them as garbadge
				score /= 10.0;
			}
			
			String highwayType = tags.getString("highway");
			double typeWeight = highwayTypeWeights.optDouble(highwayType, 0.0);
			
			if (!highwayTypeWeights.has(highwayType)) {
				log.warn("Unknown highway type {} for {} {}", 
						highwayType, obj.getOsmType(), obj.getOsmId());
			}
			
			score = score * typeWeight;
		}
		
		String placeType = StringUtils.stripToNull(tags.optString("place"));
		if (placeType != null) {
			double placeWeight = placeTypeWeights.optDouble(placeType, 0.0);
			
			if (!placeTypeWeights.has(placeType)) {
				log.warn("Unknown place type {} for {} {}", 
						placeType, obj.getOsmType(), obj.getOsmId());
			}
			
			score = score * placeWeight;
		}

		long nameOptionalCount = obj.getName().stream().filter(t -> t.optional).count();
		if (nameOptionalCount > 2) {
			score *= 0.1;
		}
		
		if (nameOptionalCount > 4) {
			score = 0.001;
		}
		
		if(Double.isInfinite(score)) {
			score = 0.0;
		}
		
		return score;
	}

	private boolean isHighway(String type) {
		return "hghnet".equals(type) || "hghway".equals(type);
	}
	
	private long countOptional(List<Token> name) {
		return name.stream().filter(t -> {return t.optional;}).count();
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
