package me.osm.gazetteer.search.imp.osmdoc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription="Import OSM Doc classes")
public class DocImportOptions {
	
	@Parameter(names= {"--source", "-s"}, description="Path to catalog. Use jar for internal osm-doc copy")
	private String path = "jar";

	public DocImportOptions() {
		
	}
	
	public DocImportOptions(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
