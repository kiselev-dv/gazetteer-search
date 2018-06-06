package me.osm.gazetteer.psqlsearch.imp.addr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.psqlsearch.PSQLSearch.ImportOptions;
import me.osm.gazetteer.psqlsearch.esclient.AddressesIndexHolder;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;
import me.osm.gazetteer.psqlsearch.esclient.IndexHolder;
import me.osm.gazetteer.psqlsearch.util.TimePeriodFormatter;

public class AddressesImporter {
	
	private static final Logger log = LoggerFactory.getLogger(AddressesImporter.class);
	
	private ImportOptions options;

	private int batchSize = 1000;
	private int total = 0;
	private int skip = 0;

	private int addresses = 0;

	private ImportObjectParser parser = new ImportObjectParser();
	
	private TransportClient client = ESServer.getInstance().client();
	private volatile BulkRequestBuilder bulk = client.prepareBulk();
	private static final IndexHolder indexHolder = new AddressesIndexHolder();
	

	private long started;
	
	
	public static final class ImportException extends RuntimeException {
		public ImportException(Exception se) {
			super(se);
		}

		public ImportException(String msg, Exception cause) {
			super(msg, cause);
		}

		private static final long serialVersionUID = 5207702025718645246L;
	}

	public AddressesImporter(ImportOptions options) {
		this.options = options;
	}
	
	public void run() throws ImportException {
		
		log.info("Read from {}", options.getSource());
		
		if (options.isDrop() && indexHolder.exists()) {
			log.info("Drop index");
			indexHolder.drop();
		}

		if(!indexHolder.exists()) {
			log.info("Create index");
			indexHolder.create();
		}
		
		this.started = new Date().getTime();
		
		try {
			
			BufferedReader reader = getStreamReader();

			try {
				String line = reader.readLine();
				while (line != null) {
					
					try {
						JSONObject obj = new JSONObject(line);
					
						AddrRowWrapper row = parser.parseAddress(obj);
						
						if(row != null) {
							total ++;

							IndexRequestBuilder index = client
									.prepareIndex(indexHolder.getIndex(), indexHolder.getType())
									.setSource(row.getJsonForIndex().toString(), XContentType.JSON);
							
							bulk.add(index);
						}
						else {
							skip++;
						}
						
						submitBatch(bulk.numberOfActions());
					}
					catch (JSONException je) {
						je.printStackTrace();
					}
					
					line = reader.readLine();
				}
				this.submitBulk();
			}
			catch (IOException e) {
				throw new ImportException(e);
			}
			finally {
				IOUtils.closeQuietly(reader);
			}
			
			String duration = TimePeriodFormatter.printDuration(new Date().getTime() - this.started);
			log.info("{} lines skiped", skip);
			log.info("Import done in {}", duration);
		}
		catch (Exception e) {
			throw new ImportException(e);
		}
	}

	private BufferedReader getStreamReader() throws FileNotFoundException, IOException, UnsupportedEncodingException {
		File file = new File(options.getSource());
		InputStream is = new FileInputStream(file);
		if (file.getName().endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf8"));
		return reader;
	}
	
	private void submitBatch(int counter) throws ImportException {
		if (counter > 0 && counter % batchSize == 0) {
			submitBulk();
			bulk = client.prepareBulk();
		}
	}

	public int rowsImported() {
		return addresses;
	}
	
	public int rowsTotal() {
		return total;
	}
	
	protected void submitBulk() {
		
		BulkResponse response = bulk.get();
		if(response.hasFailures()) {
			throw new Error(response.buildFailureMessage());
		}
		
		log.info("{} rows imported", String.format(Locale.US, "%,9d", total));
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		try {
			ImportOptions importOptions = new ImportOptions();
			importOptions.setSource(args[0]);
			
			new AddressesImporter(importOptions).run();
		} catch (ImportException e) {
			e.printStackTrace();
		}
	}

}
