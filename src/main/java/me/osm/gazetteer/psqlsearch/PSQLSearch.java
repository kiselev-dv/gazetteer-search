package me.osm.gazetteer.psqlsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import me.osm.gazetteer.psqlsearch.imp.addr.AddressesImporter;
import me.osm.gazetteer.psqlsearch.server.REServer;

public class PSQLSearch {
	
	@Parameters(commandDescription="Imaport data to ES Index")
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
	
	@Parameters(commandDescription="Start server")
	public static class ServeCLICommand {
		
		@Parameter(names= {"--port", "-p"})
		private int port = 8080;
		
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
