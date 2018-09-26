package me.osm.gazetteer.search.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription="Geocode csv/tsv")
public final class MassGeocodeOptions {
	
	@Parameter(names= {"--source", "-s"}, description="Path to csv/tsv to geocode.")
	private String source = "-";
	
	@Parameter(names= {"--no-header", "-H"}, description="There is no column names in header")
	private boolean noHeader = false;
	
	@Parameter(names= {"--query-column", "-q"}, description="Column with query") 
	public String requestColumn = "query";
	
	@Parameter(names= {"--compare", "-c"}, description="CSV aready has being geocoded, comapre lat/lon") 
	public boolean compare = false;
	
	@Parameter(names= {"--tsv", "-t"}, description="Use Tab separated values format")
	private boolean tsv = false;
	
	@Parameter(names= {"--compare-lon", "-o"}) 
	public String lonHeader = "lon";
	
	@Parameter(names= {"--compare-lat", "-a"}) 
	public String latHeader = "lat";
	
	@Parameter(names= {"--error-report", "-e"}) 
	public String errorReport = "errors.html";

	@Parameter(names= {"--treshold"})
	public double treshold = 250.0;

	int totalLines = -1;

	public InputStream getInputStream() throws IOException {
		if ("-".equals(this.source)) {
			return System.in;
		}
		else {
			countLines();
			return inputStream();
		}
	}

	private void countLines() {
		try (LineNumberReader count = new LineNumberReader(new InputStreamReader(inputStream()));) {
			while (count.skip(Long.MAX_VALUE) > 0){}
			totalLines = count.getLineNumber() + 1;
		}
		catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	private InputStream inputStream() throws IOException {
		InputStream is = new FileInputStream (new File(this.source));
		if (this.source.endsWith(".gz")) {
			return new GZIPInputStream(is);
		}
		return is;
	}
	
	public CSVParser getParser() throws FileNotFoundException, IOException {
		CSVFormat format = null;
		
		if (this.tsv || this.source.contains(".tsv")) {
			format = CSVFormat.TDF;
		}
		else {
			format = CSVFormat.EXCEL;
		}
		
		if (!this.noHeader) {
			format = format.withFirstRecordAsHeader();
		}
		
		return new CSVParser(new InputStreamReader(getInputStream()), format);
	}
}