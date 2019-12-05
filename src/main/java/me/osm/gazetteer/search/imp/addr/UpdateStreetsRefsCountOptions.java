package me.osm.gazetteer.search.imp.addr;

import com.beust.jcommander.Parameters;

@Parameters(commandDescription="Update ref count for streets")
public class UpdateStreetsRefsCountOptions {
	
	private String region;

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}
	
}
