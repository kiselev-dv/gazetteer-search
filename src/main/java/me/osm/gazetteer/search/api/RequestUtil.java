package me.osm.gazetteer.search.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.restexpress.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestUtil {

	private static final Logger log = LoggerFactory.getLogger(RequestUtil.class);
	
	public static double[] getDoubleArray(Request request, String param) {
		try {
			String val = request.getHeader(param);
			if (StringUtils.isNoneBlank(val)) {
				String[] split = StringUtils.split(val, "[],;");
				double[] result = new double[split.length];
				for(int i = 0; i < split.length; i++) {
					result[i] = Double.valueOf(split[i]);
				}
				return result;
			}
		}
		catch (NumberFormatException e) {
			log.warn("Can't parse value as double for {}", param, e);
		}
		
		return null;
	}

	public static Set<String> getSet(Request request, String param) {
		String val = request.getHeader(param);
		if (StringUtils.isNoneBlank(val)) {
			String[] split = StringUtils.split(val, "[],;");
			return new HashSet<>(Arrays.asList(split));
		}
		
		return null;
	}

	public static boolean getBoolean(Request request, String header, boolean defValue) {
		String val = request.getHeader(header);
		if (val != null && "true".equals(val.toLowerCase())) {
			return true;
		}
		
		return false;
	}
	
	public static Double getDoubleOrNull(String s) {
		try {
			return Double.valueOf(s);
		}
		catch (Exception e) {
			return null;
		}
	}

}
