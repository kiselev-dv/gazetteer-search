package me.osm.gazetteer.search.api.osmdoc;

import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.restexpress.Request;
import org.restexpress.Response;

import me.osm.osmdoc.read.OSMDocFacade;

public class OSMDocAPI {
	
	private final OSMDocFacade facade;
	
	public OSMDocAPI(OSMDocFacade facade) {
		this.facade = facade;
	}
	
	public Object read(Request request, Response response) {

		String langCode = request.getHeader("lang");
		Locale lang = null;
		if(langCode != null) {
			lang = Locale.forLanguageTag(langCode);
		}

		String handler = (String) request.getParameter("handler");
		
		if(handler.equals("hierarchy")) {
			String hierarchy = request.getHeader("id");
			
			return facade.getHierarchyJSON(StringUtils.stripToNull(hierarchy), lang).toMap();
		}
		else if(handler.equals("poi-class")) {

			JSONObject classes = new JSONObject();
			for(JSONObject f : facade.listTranslatedFeatures(lang)) {
				classes.put(f.getString("name"), f);
			}
			
			return classes.toMap();
		}
		
		return null;
		
	}
}
