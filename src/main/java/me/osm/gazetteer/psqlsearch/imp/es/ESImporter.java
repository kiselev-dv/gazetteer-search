package me.osm.gazetteer.psqlsearch.imp.es;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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

import me.osm.gazetteer.psqlsearch.esclient.AddressesIndexHolder;
import me.osm.gazetteer.psqlsearch.esclient.ESServer;

public class ESImporter {
	
	private static final Logger log = LoggerFactory.getLogger(ESImporter.class);
	
	private int batchSize = 1000;
	private int total = 0;
	private String source;

	private int addresses = 0;

	private ImportObjectParser parser = new ImportObjectParser();
	
	private TransportClient client = ESServer.getInstance().client();
	private volatile BulkRequestBuilder bulk = client.prepareBulk();

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

	public ESImporter(String source) {
		this.source = source;
	}
	
	public void run() throws ImportException {
		
		log.info("Read from {}", source);
		
		AddressesIndexHolder.drop();
		if(!AddressesIndexHolder.exists()) {
			log.info("Create index");
			AddressesIndexHolder.create();
		}
		
		this.started = new Date().getTime();
		
		try {
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(new File(source)));
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf8"));

			try {
				String line = reader.readLine();
				while (line != null) {
					
					total ++;
					try {
						JSONObject obj = new JSONObject(line);
					
						AddrRowWrapper row = parser.parseAddress(obj);
						
						if(row != null) {
							IndexRequestBuilder index = client
									.prepareIndex(AddressesIndexHolder.INDEX_NAME, AddressesIndexHolder.ADDR_ROW_TYPE)
									.setSource(row.getJsonForIndex().toString(), XContentType.JSON);
							
							bulk.add(index);
						}
						
						submitBatch(total);
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
			
			String duration = printDuration(new Date().getTime() - this.started);
			log.info("Import done in {}", duration);
		}
		catch (Exception e) {
			throw new ImportException(e);
		}
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

	private String printDuration(long time) {
		Duration duration = new Duration(time);
		PeriodFormatter formatter = new PeriodFormatterBuilder()
			     .appendDays()
			     .appendSuffix("d")
			     .appendSeparator(" ")
			     .appendHours()
			     .appendSuffix("h")
			     .appendSeparator(" ")
			     .appendMinutes()
			     .appendSuffix("m")
			     .appendSeparator(" ")
			     .appendSeconds()
			     .appendSuffix("s")
			     .appendSeparator(" ")
			     .appendMillis3Digit()
			     .appendSuffix("ms")
			     .toFormatter();
		
		String durationString = formatter.print(duration.toPeriod());
		return durationString;
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		try {
			new ESImporter(args[0]).run();
		} catch (ImportException e) {
			e.printStackTrace();
		}
	}

}
