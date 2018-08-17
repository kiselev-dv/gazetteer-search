package me.osm.gazetteer.search.imp;

import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import me.osm.gazetteer.search.imp.osmdoc.OSMDoc;

@Parameters(commandDescription="Import data to ES Index")
public class ImportOptions {
	
	@Parameter(names={"--drop", "-d"}, description="Drop and create the whole index")
	private boolean drop;
	
	@Parameter(names= {"--source", "-s"}, description="Path to file to import. Use - for stdin")
	private String source = "-";
	
	@Parameter(names= {"--osm-doc"}, description="Path to OSMDoc")
	private String osmdocPath = "jar";
	
	@Parameter(names= {"--poi-cfg"}, description="Path to POI import configuration json")
	private String importPoiCfg = "config/poi-import.json";
	
	@Parameter(names={"--skip-poi"}, description="Do not import POI")
	private boolean skipPoi;
	
	@Parameter(names={"--languages", "-l"}, description="Languages to import")
	private List<String> languages;
	
	@Parameter(names={"--region"}, description="Import region")
	private String region;
	
	@Parameter(names={"--translit"}, description="Transliterate names")
	private boolean translit;
	
	@Parameter(names={"--mode"}, description="Import mode")
	private ImportMode importMode = ImportMode.update;

	public boolean isDrop() {
		return drop;
	}

	public void setDrop(boolean drop) {
		this.drop = drop;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public OSMDoc getOSMDoc() {
		return OSMDoc.get(osmdocPath);
	}

	public boolean isSkipPoi() {
		return skipPoi;
	}
	
	public String getRegion() {
		return region;
	}

	public boolean isTranslit() {
		return translit;
	}
	
	public ImportMode getMode() {
		return importMode;
	}
	
	public POIIgnore getPoiCfg() {
		if (skipPoi) {
			return null;
		}
		
		JSONObject cfg;
		try {
			cfg = new JSONObject(IOUtils.toString(new FileReader(new File(importPoiCfg))));
			return new POIIgnore(getOSMDoc(), cfg);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getLanguages() {
		if (languages != null) {
			return Arrays.asList(StringUtils.split(StringUtils.join(languages, ' '), " ,[]\"\'")); 
		}
		return null;
	}

}