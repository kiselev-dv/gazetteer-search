package me.osm.gazetteer.search.imp.addr;

import java.util.Collection;

import org.json.JSONObject;

public class ImportMeta {
	
	private String region;
	private long region_counter;
	private long import_counter;
	
	public ImportMeta(String region, long regionCounter, long importCounter) {
		this.region = region;
		this.region_counter = regionCounter;
		this.import_counter = importCounter;
	}
	
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	
	public long getRegionCounter() {
		return region_counter;
	}
	public void setRegionCounter(long regionCounter) {
		this.region_counter = regionCounter;
	}
	
	public long getImportCounter() {
		return import_counter;
	}
	public void setImportCounter(long importCounter) {
		this.import_counter = importCounter;
	}

	public JSONObject getJsonForIndex() {
		JSONObject obj = new JSONObject();
		obj.put("region", region);
		obj.put("region_counter", region_counter);
		obj.put("import_counter", import_counter);
		return obj;
	}
	
}
