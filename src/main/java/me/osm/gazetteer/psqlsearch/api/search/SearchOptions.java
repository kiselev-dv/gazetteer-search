package me.osm.gazetteer.psqlsearch.api.search;

public class SearchOptions {
	
	private boolean withPrefix = true; 
	private Double lon; 
	private Double lat;
	private boolean addressesOnly;
	
	public Double getLon() {
		return lon;
	}
	public void setLon(Double lon) {
		this.lon = lon;
	}
	
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	
	public boolean isAddressesOnly() {
		return addressesOnly;
	}
	public void setAddressesOnly(boolean addressesOnly) {
		this.addressesOnly = addressesOnly;
	}
	
	public boolean isWithPrefix() {
		return withPrefix;
	}
	public void setWithPrefix(boolean withPrefix) {
		this.withPrefix = withPrefix;
	}
	
}
