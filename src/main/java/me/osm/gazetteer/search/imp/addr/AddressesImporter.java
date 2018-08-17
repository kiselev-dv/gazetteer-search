package me.osm.gazetteer.search.imp.addr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.esclient.AddressesIndexHolder;
import me.osm.gazetteer.search.esclient.ESServer;
import me.osm.gazetteer.search.esclient.IndexHolder;
import me.osm.gazetteer.search.imp.ImportMode;
import me.osm.gazetteer.search.imp.ImportOptions;
import me.osm.gazetteer.search.util.TimePeriodFormatter;

public class AddressesImporter {
	
	private static final Logger log = LoggerFactory.getLogger(AddressesImporter.class);
	
	private ImportOptions options;
	private ImportObjectParser parser;

	private int batchSize = 1000;
	private int total = 0;
	private int skip = 0;

	private int addresses = 0;

	private TransportClient client = ESServer.getInstance().client();
	private volatile BulkRequestBuilder bulk = client.prepareBulk();
	private static final IndexHolder indexHolder = new AddressesIndexHolder();

	private long started;
	
	private Set<String> batchObjectIds = new HashSet<>();
	
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
		this.parser = new ImportObjectParser(this.options);
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
		
		if (options.getMode() == ImportMode.delete) {
			if (options.getRegion() != null) {
				log.info("Drop region {}", options.getRegion());
				
				BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
					.filter(QueryBuilders.matchQuery("import.region", options.getRegion()))
					.source(IndexHolder.ADDRESSES_INDEX).get();
				
				log.info("Deleted {}", bulkByScrollResponse.getDeleted());
			}
		}
		
		this.started = new Date().getTime();
		
		try {
			
			BufferedReader reader = getStreamReader();
			
			ImportMeta imp = createImportMetaObject();
			
			try {
				String line = reader.readLine();
				while (line != null) {
					
					try {
						JSONObject obj = new JSONObject(line);
					
						AddrRowWrapper row = parser.parseAddress(obj);
						
						if(row != null) {
							total ++;

							row.setImport(imp);
							if(options.getMode() == ImportMode.update) {
								batchObjectIds.add(row.getId());
							}

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

				if (options.getMode() == ImportMode.update) {
					BoolQueryBuilder filter = QueryBuilders.boolQuery();
					filter.must(QueryBuilders.termQuery("import.region", getRegion()));
					filter.must(QueryBuilders.rangeQuery("import.region_counter").lt(imp.getRegionCounter()));
					
					DeleteByQueryAction.INSTANCE.newRequestBuilder(client).source(IndexHolder.ADDRESSES_INDEX)
						.filter(filter).get();
				}
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
			
			fileRead();
			
			new UpdateStreetsUsage(this.options.getRegion()).run();
		}
		catch (Exception e) {
			throw new ImportException(e);
		}
	}

	private String getRegion() {
		if (options.getRegion() == null) {
			return null;
		}
		
		return options.getRegion().toLowerCase();
	}

	private ImportMeta createImportMetaObject() throws InterruptedException, ExecutionException {
		TermQueryBuilder byReagionQF = QueryBuilders.termQuery("import.region", getRegion());
		SearchRequestBuilder counters = client.prepareSearch(IndexHolder.ADDRESSES_INDEX)
			.setQuery(QueryBuilders.matchAllQuery())
			.addAggregation(
					AggregationBuilders.filter("region_max", byReagionQF)
						.subAggregation(AggregationBuilders.max("value").field("import.region_counter")))
			.addAggregation(AggregationBuilders.max("import_max").field("import.import_counter"));
		
		SearchResponse countersResponse = counters.execute().get();
		
		JSONObject regionMaxAggregation = new JSONObject(countersResponse.getAggregations().get("region_max").toString());
		long regionImportCounter = regionMaxAggregation.getJSONObject("region_max").getJSONObject("value").optLong("value", 0);

		JSONObject importMaxAggregation = new JSONObject(countersResponse.getAggregations().get("import_max").toString());
		long importCounter = importMaxAggregation.getJSONObject("import_max").optLong("value", 0);
		
		ImportMeta imp = new ImportMeta(getRegion(), regionImportCounter + 1, importCounter + 1);
		return imp;
	}

	private void fileRead() {
		// Do nothing for now
		// Later if we have something like swap
		// whole region we'll have to do some actions here
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
		if (bulk.numberOfActions() > 0) {
			
			if (!batchObjectIds.isEmpty()) {
				DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
					.source(IndexHolder.ADDRESSES_INDEX)
					.filter(QueryBuilders.termsQuery("id", batchObjectIds)).get();
				
				batchObjectIds.clear();
			}
			
			BulkResponse response = bulk.get();
			if(response.hasFailures()) {
				throw new Error(response.buildFailureMessage());
			}
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
