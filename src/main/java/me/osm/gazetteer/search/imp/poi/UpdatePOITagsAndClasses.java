package me.osm.gazetteer.search.imp.poi;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.gazetteer.search.imp.PagedScroll;
import me.osm.gazetteer.search.imp.osmdoc.OSMDoc;
import me.osm.osmdoc.localization.L10n;
import me.osm.osmdoc.model.v2.Feature;
import me.osm.osmdoc.model.v2.Tag.Val;
import me.osm.osmdoc.read.tagvalueparsers.LogTagsStatisticCollector;

public class UpdatePOITagsAndClasses {
	
	private static final Logger log = LoggerFactory.getLogger(UpdatePOITagsAndClasses.class); 
	
	private TransportClient client = ESServer.getInstance().client();
	private OSMDoc osmDoc;
	private long counter;

	public UpdatePOITagsAndClasses (OSMDoc osmDoc) {
		this.osmDoc = osmDoc;
	}
	
	public void run() {
		long start = new Date().getTime();
		new PagedScroll(10000, "poipnt", new String[] {"json.tags", "poi_class"}).scroll(page -> {
			
			BulkRequestBuilder bulk = client.prepareBulk();
			
			for (SearchHit hit : page.getHits()) {
				JSONObject jsonObject = new JSONObject(hit.getSourceAsMap()); 
				JSONArray poiClasses = jsonObject.optJSONArray("poi_class");
				if (poiClasses != null && poiClasses.length() > 0) {
					
					fillPoiPoint(jsonObject);
					jsonObject.remove("json");
					
					bulk.add(client.prepareUpdate(
							IndexHolder.ADDRESSES_INDEX, 
							IndexHolder.ADDR_ROW_TYPE, 
							hit.getId()).setDoc(jsonObject.toMap()));
				}

				counter++;
			}
			
			if (bulk.numberOfActions() > 0) {
				bulk.get();
			}
			
			double perRow = (new Date().getTime() - start) / (double) counter;
			long eta = (long) ((page.getHits().getTotalHits() - counter) * perRow);
			log.info("Done {} of {}. ETA: {}", counter, page.getHits().getTotalHits(), 
					DurationFormatUtils.formatDurationHMS(eta));
		});
	}
	
	private void fillPoiPoint(JSONObject jsonObject) {
		if (osmDoc != null) {
			PoiInfo info = getInfo(jsonObject);
			
			jsonObject.put("poi_class_trans", new JSONArray(info.getTranslatedPoiClasses()));
			jsonObject.put("more_tags", info.getMoreTags());
			jsonObject.put("poi_keywords", new JSONArray(info.getKeywords()));
		}
	}
	
	public PoiInfo getInfo(JSONObject jsonObject) {
		PoiInfo inf = new PoiInfo();
		
		inf.setTranslatedPoiClasses(getPoiTypesTranslated(jsonObject));
		
		List<Feature> poiClassess = listPoiClassesOSMDoc(jsonObject);
		Map<String, List<Val>> moreTagsVals = new HashMap<String, List<Val>>();
		JSONObject moreTags = osmDoc.getFacade().parseMoreTags(poiClassess, getTagsJSON(jsonObject), 
				new LogTagsStatisticCollector(), moreTagsVals, true);
		
		inf.setMoreTags(moreTags);
		
		LinkedHashSet<String> keywords = new LinkedHashSet<String>();
		osmDoc.getFacade().collectKeywords(poiClassess, moreTagsVals, keywords, null);
		
		inf.setKeywords(keywords);
		
		return inf;
	}

	private JSONObject getTagsJSON(JSONObject jsonObject) {
		JSONObject tags = jsonObject.optJSONObject("tags");
		if (tags != null) {
			return tags;
		}
		
		return jsonObject.getJSONObject("json").getJSONObject("tags");
	}
	
	private List<String> getPoiTypesTranslated(JSONObject obj) {
		
		List<String> result = new ArrayList<String>(1);
		
		List<Feature> classes = listPoiClassesOSMDoc(obj);
		
		for(Feature f : classes) {
			// This L10n.supported is complete mess
			// TODO get rid of that
			for(String ln : L10n.supported) {
				String translatedTitle = osmDoc.getFacade().getTranslatedTitle(f, Locale.forLanguageTag(ln));
				result.add(translatedTitle);
			}
		}
		
		return result;
	}

	private List<Feature> listPoiClassesOSMDoc(JSONObject obj) {
		JSONArray poiClasses = obj.optJSONArray("poi_class");
		
		List<Feature> classes = new ArrayList<Feature>();
		for(int i = 0; i < poiClasses.length(); i++) {
			String classCode = poiClasses.getString(i);
			Feature poiClass = osmDoc.getFacade().getFeature(classCode);
			if(poiClass != null) {
				classes.add(poiClass);
			}
			else {
				log.warn("Couldn't find poi class for code {}", classCode);
			}
		}
		return classes;
	}
	
	public static void main(String[] args) {
		new UpdatePOITagsAndClasses(OSMDoc.get(args[0])).run();
	}

}
