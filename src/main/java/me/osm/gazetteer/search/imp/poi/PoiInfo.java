package me.osm.gazetteer.search.imp.poi;

import java.util.LinkedHashSet;
import java.util.List;

import org.json.JSONObject;

public class PoiInfo {

	private List<String> poiTypesTranslated;
	private JSONObject moreTags;
	private LinkedHashSet<String> keywords;

	public void setTranslatedPoiClasses(List<String> poiTypesTranslated) {
		this.poiTypesTranslated = poiTypesTranslated;
	}
	
	public List<String> getTranslatedPoiClasses() {
		return this.poiTypesTranslated;
	}

	public void setMoreTags(JSONObject moreTags) {
		this.moreTags = moreTags;
	}

	public void setKeywords(LinkedHashSet<String> keywords) {
		this.keywords = keywords;
	}

	public JSONObject getMoreTags() {
		return moreTags;
	}

	public LinkedHashSet<String> getKeywords() {
		return keywords;
	}
	
	

}
