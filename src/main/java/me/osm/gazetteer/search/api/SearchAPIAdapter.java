package me.osm.gazetteer.search.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import me.osm.gazetteer.search.api.meta.QueryParameter;
import me.osm.gazetteer.search.api.search.Search;
import me.osm.gazetteer.search.api.search.SearchOptions;

public class SearchAPIAdapter implements SearchAPI {
	
	private static final Logger log = LoggerFactory.getLogger(SearchAPIAdapter.class);
	
	private static final int DEFAULT_PAGE_SIZE = 20;
	
	@QueryParameter(type="string", description="Search string" )
	public static final String Q_PARAM = "q";
	
	@QueryParameter(type="boolean", description="Use search as you type, default is true" )
	public static final String PREFIX_PARAM = "prefix";
	
	@QueryParameter(type="int", description="Results page" )
	public static final String PAGE_PARAM = "page";
	@QueryParameter(type="int", description="Results page size, default is 20" )
	public static final String PAGE_SIZE = "size";

	@QueryParameter(type="double", description="Sort results relative to given lon lat" )
	public static final String LAT_PARAM = "lat";
	@QueryParameter(type="double", description="Sort results relative to given lon lat" )
	public static final String LON_PARAM = "lon";
	
	@QueryParameter(type="boolean", description="Return verbose address data" )
	public static final String VERBOSE_ADDRESS = "verbose_address";
	@QueryParameter(type="boolean", description="Do not search for POI, default is false" )
	public static final String NO_POI = "no_poi";
	
	@QueryParameter(type="String[]", description="Results should has one of the given references, "
			+ "for instance should be inside one of the given city" )
	public static final String REFERENCES = "references";
	
	@Inject
	private Search search;

	@Override
	public ResultsWrapper read(Request request, Response response) {

		SearchOptions searchOptions = new SearchOptions();
		
		String query = request.getHeader(Q_PARAM);
		boolean prefix = getBoolean(request, PREFIX_PARAM, false);
		searchOptions.setWithPrefix(prefix);
		
		int pageSize = getPageSize(request);
		int page = getPage(request);
		
		searchOptions.setLon(getLon(request));
		searchOptions.setLat(getLat(request));
		
		searchOptions.setVerboseAddress(getBoolean(request, VERBOSE_ADDRESS, false));
		searchOptions.setNoPoi(getBoolean(request, NO_POI, false));
		
		searchOptions.setReferences(getSet(request, REFERENCES));
		
		log.info("search {}", query);
		
		String mark = request.getHeader("mark");
		
		ResultsWrapper res = search.search(query, page, pageSize, searchOptions);
		
		if (res != null) {
			res.setMark(mark);
		}
		
		return res;
	}

	private Set<String> getSet(Request request, String param) {
		String val = request.getHeader(param);
		if (StringUtils.isNoneBlank(val)) {
			String[] split = StringUtils.split(val, "[],;");
			return new HashSet<>(Arrays.asList(split));
		}
		
		return null;
	}

	private boolean getBoolean(Request request, String header, boolean defValue) {
		String val = request.getHeader(header);
		if (val != null && "true".equals(val.toLowerCase())) {
			return true;
		}
		
		return false;
	}

	private Double getLat(Request request) {
		if(request.getHeader(LAT_PARAM) != null) {
			return getDoubleOrNull(request.getHeader(LAT_PARAM));
		}
		return null;
	}
	
	private Double getLon(Request request) {
		if(request.getHeader(LON_PARAM) != null) {
			return getDoubleOrNull(request.getHeader(LON_PARAM));
		}
		return null;
	}

	private Double getDoubleOrNull(String s) {
		try {
			return Double.valueOf(s);
		}
		catch (Exception e) {
			return null;
		}
	}

	private int getPage(Request request) {
		int page = 1;
		if(request.getHeader(PAGE_PARAM) != null) {
			page = Integer.parseInt(request.getHeader(PAGE_PARAM));
			if(page < 1) {
				page = 1;
			}
		}
		return page;
	}

	private int getPageSize(Request request) {
		int pageSize = DEFAULT_PAGE_SIZE;
		if(request.getHeader(PAGE_SIZE) != null) {
			pageSize = Integer.parseInt(request.getHeader(PAGE_SIZE));
		}
		return pageSize;
	}

}
