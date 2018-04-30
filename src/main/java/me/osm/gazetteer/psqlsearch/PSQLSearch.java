package me.osm.gazetteer.psqlsearch;

import java.io.FileNotFoundException;
import java.io.IOException;

import me.osm.gazetteer.psqlsearch.imp.postgres.Importer;
import me.osm.gazetteer.psqlsearch.imp.postgres.Importer.ImportException;

public class PSQLSearch {
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		try {
			new Importer(args[0]).run();
		} catch (ImportException e) {
			e.printStackTrace();
		}
	}

}
