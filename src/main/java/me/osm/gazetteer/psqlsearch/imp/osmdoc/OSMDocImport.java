package me.osm.gazetteer.psqlsearch.imp.osmdoc;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import me.osm.osmdoc.read.AbstractReader;
import me.osm.osmdoc.read.DOCFileReader;
import me.osm.osmdoc.read.DOCFolderReader;
import me.osm.osmdoc.read.OSMDocFacade;

public class OSMDocImport {
	
	public static void main(String[] args) {
		String docPath = "/home/dkiselev/osm/osm-doc/catalog/";
		
		AbstractReader reader;
		if(docPath.endsWith(".xml") || docPath.equals("jar")) {
			reader = new DOCFileReader(docPath);
		}
		else {
			reader = new DOCFolderReader(docPath);
		}
		
		ArrayList<String> exclude = new ArrayList<String>();
		OSMDocFacade facade = new OSMDocFacade(reader, exclude);
		List<JSONObject> features = facade.listTranslatedFeatures(null);
		
		for (JSONObject obj : features) {
			System.out.println(obj);
		}
		
	}

}
