package me.osm.gazetteer.search.imp.addr;

public class ImportMeta {
	
	private String region;
	private int region_counter;
	private int import_counter;
	
	public ImportMeta(String region, int regionCounter, int importCounter) {
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
	
	public int getRegionCounter() {
		return region_counter;
	}
	public void setRegionCounter(int regionCounter) {
		this.region_counter = regionCounter;
	}
	
	public int getImportCounter() {
		return import_counter;
	}
	public void setImportCounter(int importCounter) {
		this.import_counter = importCounter;
	}
	
}
