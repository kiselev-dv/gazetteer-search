package me.osm.gazetteer.search.imp.osmdoc;

import java.util.ArrayList;

import me.osm.osmdoc.read.DOCFileReader;
import me.osm.osmdoc.read.DOCFolderReader;
import me.osm.osmdoc.read.DOCReader;
import me.osm.osmdoc.read.OSMDocFacade;

public class OSMDoc {
	
	private static volatile String docPath = "osm-doc/catalog/";
	private static volatile OSMDoc instance;
	
	private OSMDocFacade facade;
	private DOCReader reader;
	
	public static OSMDoc get() {
		if (instance != null) {
			return instance;
		}
		
		synchronized (OSMDoc.class) {
			if (instance == null) {
				instance = new OSMDoc();
			}
			
			return instance;
		}
	}
	
	public static OSMDoc get(String path) {
		docPath = path;
		return get();
	}

	private OSMDoc() {
		if(docPath.endsWith(".xml") || docPath.equals("jar")) {
			this.reader = new DOCFileReader(docPath);
		}
		else {
			this.reader = new DOCFolderReader(docPath);
		}
		
		ArrayList<String> exclude = new ArrayList<String>();
		this.facade = new OSMDocFacade(this.reader, exclude);
	}

	public OSMDocFacade getFacade() {
		return facade;
	}

	public DOCReader getReader() {
		return reader;
	}
	
}
