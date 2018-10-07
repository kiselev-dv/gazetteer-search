package me.osm.gazetteer.search.imp.osmdoc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;

import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.gazetteer.search.esclient.POIClassIndexHolder;
import me.osm.osmdoc.localization.L10n;
import me.osm.osmdoc.model.Feature;
import me.osm.osmdoc.read.OSMDocFacade;

public class OSMDocImport {
	
	private static TransportClient client = ESServer.getInstance().client();
	private static BulkRequestBuilder bulk = client.prepareBulk();
	
	private static final IndexHolder indexHolder = new POIClassIndexHolder();
	private final OSMDocFacade facade;
	
	public OSMDocImport(DocImportOptions options) {
		facade = OSMDoc.get(options.getPath()).getFacade();
	}
	
	public void run() {
		if (indexHolder.exists()) {
			indexHolder.drop();
		}
		
		if (!indexHolder.exists()) {
			indexHolder.create();
		}
		
		List<JSONObject> features = facade.listTranslatedFeatures(null);
		
		for (JSONObject obj : features) {
			String name = obj.getString("name");
			Feature feature = facade.getFeature(name);
			
			JSONArray namesTrans = new JSONArray();
			for(String lt : L10n.supported) {
				String translatedTitle = facade.getTranslatedTitle(feature, Locale.forLanguageTag(lt));
				
				namesTrans.put(translatedTitle);
			}
			
			obj.put("title", namesTrans);
			Set<String> kwds = new HashSet<String>();
			
			facade.collectKeywords(Collections.singleton(feature), null, kwds, null);
			obj.put("keywords", new JSONArray(kwds));
			
			IndexRequestBuilder index = client
					.prepareIndex(indexHolder.getIndex(), indexHolder.getType())
					.setSource(obj.toString(), XContentType.JSON);
			
			bulk.add(index);
		}
		
		try {
			bulk.execute().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new OSMDocImport(new DocImportOptions(args[0])).run();
	}
}
