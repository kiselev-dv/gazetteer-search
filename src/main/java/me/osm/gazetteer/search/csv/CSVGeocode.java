package me.osm.gazetteer.search.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.api.ResultsWrapper;
import me.osm.gazetteer.search.api.ResultsWrapper.SearchResultRow;
import me.osm.gazetteer.search.api.search.ESDefaultSearch;
import me.osm.gazetteer.search.api.search.Search;
import me.osm.gazetteer.search.api.search.SearchOptions;
import me.osm.gazetteer.search.util.TimePeriodFormatter;

public class CSVGeocode {
	
	private static final Logger log = LoggerFactory.getLogger(CSVGeocode.class);
	
	private static final Search search = new ESDefaultSearch();
	private static int reportFileCounter = 1;
	
	private static final String reportTemplate;
	static {
		try {
			reportTemplate = IOUtils.toString(CSVGeocode.class.getResourceAsStream("/utility/error-report-template.html"));
		}
		catch(Exception e) {
			throw new Error(e);
		}
	}
	
	public CSVGeocode(MassGeocodeOptions options) {
		try {
			
			SearchOptions searchOptions = new SearchOptions();
			searchOptions.setWithPrefix(false);

			long fails = 0;
			long onPage = 0;
			
			JSONArray errors = new JSONArray();

			search.search("warmup", 0, 1, searchOptions);

			CSVParser parser = options.getParser();

			long started = new Date().getTime();
			long counter = 0;
			
			for(CSVRecord record : parser) {
				
				String q = record.get(0);
				if (record.isMapped(options.requestColumn)) {
					q = record.get(options.requestColumn);
				}
				
				ResultsWrapper searchResults = search.search(q, 0, 10, searchOptions);
				List<SearchResultRow> rows = searchResults.getRows();
				if (!rows.isEmpty()) {
					SearchResultRow firstRow = rows.get(0);
					
					if (options.compare) {
						double refLon = Double.parseDouble(record.get(options.lonHeader)); 
						double refLat = Double.parseDouble(record.get(options.latHeader));
						
						Double distance = distance(refLon, refLat, firstRow.centroid.lon(), firstRow.centroid.lat());
						if (distance > options.treshold) {
							fails++;
							boolean foundOnFirstPage = false;
							
							JSONObject err = new JSONObject();
							errors.put(err);
							
							err.put("d", distance.intValue());
							err.put("q", q);
							err.put("c", searchResults.getTotalHits());
							err.put("t", searchResults.getTrim());
							err.put("pq", searchResults.getParsedQuery());
							
							JSONArray errRows = new JSONArray();
							err.put("rows", errRows);
							for (SearchResultRow row : rows) {
								JSONObject errRow = new JSONObject();
								errRows.put(errRow);
								
								errRow.put("full_text", row.full_text);
								errRow.put("matched_queries", row.matched_queries);
								errRow.put("rank", row.rank);
								errRow.put("osm_id", row.osm_id);
								
								Double rowD = distance(refLon, refLat, row.centroid.lon(), row.centroid.lat());
								errRow.put("distance", rowD.intValue());
								
								if (rowD.intValue() <= options.treshold) {
									foundOnFirstPage = true;
								}
							}
							
							if (foundOnFirstPage) {
								onPage++;
								err.put("on_page", true);
							}
							
							if(errors.length() > 0 && errors.length() % 1000 == 0) {
								writeReport(errors, options);
								errors = new JSONArray();
							} 
						}
					}
					else {
						System.out.println(q + "\t" + firstRow.full_text + "\t" + firstRow.centroid.lon() + "\t" + firstRow.centroid.lat()); 
					}
				}
				
				counter++;
				if (counter % 100 == 0) {
					
					long now = new Date().getTime();
					Double perLine = new Double(now - started) / counter;
					
					if (options.totalLines > 0) {
						log.info("{}/{} lines geocoded", counter, options.totalLines);
						
						long etams = new Double((options.totalLines - counter) * perLine).longValue();
						log.info("{}ms per line, ETA: {}", String.format("%.3f", perLine), TimePeriodFormatter.printDuration(etams));
					}
					else {
						log.info("{} lines geocoded", counter, parser.getRecordNumber());
					}
					
					log.info("Fails {}, Not found {}, On first page: {}, Success: {}", 
							fails, 
							(fails - onPage),
							onPage,
							counter - fails);
					
				}
			}
			
			parser.close();
			
			long ended = new Date().getTime();
			
			log.info("{} lines geocoded in {}", parser.getRecordNumber(), TimePeriodFormatter.printDuration(ended - started));
			log.info(String.format("%.2f ms per line", new Double(ended - started) / parser.getRecordNumber()));
			log.info("Fails {}, Not found {}, On first page: {}, Total: {}", 
					fails, 
					(fails - onPage),
					onPage,
					parser.getRecordNumber());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		
		
	}
	
	private static void writeReport(JSONArray errors, 
			MassGeocodeOptions options) {
		
		try {
			String fileName = options.errorReport;
			String name = StringUtils.substringBeforeLast(fileName, ".");
			String ext = StringUtils.substringAfterLast(fileName, ".");
			
			File file = new File(name + "-" + reportFileCounter + "." + ext);
			PrintWriter writer = new PrintWriter(new FileWriter(file));
			
			writer.print(StringUtils.replace(reportTemplate, "$errors", errors.toString()));
			writer.flush();
			writer.close();
			
			System.out.println("Save report to: " + file.toString());
			
			reportFileCounter++;
		}
		catch (Exception e) {
			throw new Error(e);
		}
	}

	/**
	 * Calculate distance between two points in latitude and longitude taking
	 * into account height difference. If you are not interested in height
	 * difference pass 0.0. Uses Haversine method as its base.
	 * 
	 * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
	 * el2 End altitude in meters
	 * @returns Distance in Meters
	 */
	public static double distance(double lon1, double lat1, double lon2,
	        double lat2) {

	    final int R = 6371; // Radius of the earth

	    double latDistance = Math.toRadians(lat2 - lat1);
	    double lonDistance = Math.toRadians(lon2 - lon1);
	    double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
	            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    return R * c * 1000; // convert to meters
	}

}
