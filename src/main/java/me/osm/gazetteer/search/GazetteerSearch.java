package me.osm.gazetteer.search;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import me.osm.gazetteer.search.imp.addr.AddressesImporter;
import me.osm.gazetteer.search.server.REServer;

public class GazetteerSearch {
	
	@Parameters(commandDescription="Import data to ES Index")
	public static class ImportOptions {
		
		@Parameter(names={"--drop", "-d"}, description="Drop and create the whole index")
		private boolean drop;
		
		@Parameter(names= {"--source", "-s"}, description="Path to file to import. Use - for stdin")
		private String source = "-";

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

	}
	
	public static void main(String[] args) {
		
		ImportOptions imprt = new ImportOptions();
		ImportOptions serve = new ImportOptions();
		
		JCommander jc = JCommander.newBuilder()
				.programName("gazetteer-search")
				.addCommand("import", imprt)
				.addCommand("serve", serve)
				.build();
		
		jc.parse(args);
		
		String parsedCommand = jc.getParsedCommand();
		if (parsedCommand == "import") {
			new AddressesImporter(imprt).run();
		}
		else {
			REServer.getInstance();
		}
		
	}

}
