package me.osm.gazetteer.search;

import java.util.Arrays;

import com.beust.jcommander.JCommander;

import me.osm.gazetteer.search.imp.ImportOptions;
import me.osm.gazetteer.search.imp.addr.AddressesImporter;
import me.osm.gazetteer.search.server.REServer;

public class GazetteerSearch {
	
	public static void main(String[] args) {
		
		ImportOptions imprt = new ImportOptions();
		ServerOptions serve = new ServerOptions();
		
		JCommander jc = JCommander.newBuilder()
				.programName("gazetteer-search")
				.addCommand("import", imprt)
				.addCommand("serve", serve)
				.build();
		
		if(Arrays.stream(args).anyMatch(a -> "--help".equals(a) || "-h".equals(a))) {
			jc.usage();
			System.exit(0);
		}
		
		try {
			jc.parse(args);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			jc.usage();
			
			System.exit(1);
		}

		String parsedCommand = jc.getParsedCommand();
		if (parsedCommand == "import") {
			new AddressesImporter(imprt).run();
		}
		else {
			REServer.getInstance(serve);
		}
		
	}

}
