package me.osm.gazetteer.search.server;

import org.apache.commons.lang3.StringUtils;
import org.restexpress.Flags;
import org.restexpress.Parameters;
import org.restexpress.RestExpress;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpMethod;
import me.osm.gazetteer.search.api.SearchAPI;
import me.osm.gazetteer.search.api.SendQAPI;
import me.osm.gazetteer.search.api.osmdoc.OSMDocAPI;
import me.osm.gazetteer.search.api.osmdoc.TagStatisticsAPI;

public class REServerRoutes {

		private static final int MINUTE = 60;
		private static final int HOUR = 60 * MINUTE;
		private static final int DAY = 24 * HOUR;
		private static final int WEEK = 7 * DAY;
		
		private final SearchAPI searchAPI;
		private final OSMDocAPI osmdocAPI;
		private final TagStatisticsAPI tagStatsAPI;
		
		public REServerRoutes(SearchAPI searchAPI, OSMDocAPI osmdocAPI, TagStatisticsAPI tagStatsAPI) {
			this.searchAPI = searchAPI;
			this.osmdocAPI = osmdocAPI;
			this.tagStatsAPI = tagStatsAPI;
		}
		
		public void defineRoutes(RestExpress server, String serverWebRoot) {

			if (!StringUtils.startsWith(serverWebRoot, "/") && StringUtils.isNotEmpty(serverWebRoot)) {
				serverWebRoot = "/" + serverWebRoot;
			}

			LoggerFactory.getLogger(REServerRoutes.class).info("Define routes with web root: {}", serverWebRoot);
			
			server.uri(serverWebRoot + "/location/_search", searchAPI)
				.method(HttpMethod.GET)
				.name("feature")
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter(Parameters.Cache.MAX_AGE, MINUTE);
			
			server.uri(serverWebRoot + "/location/_search.{format}", searchAPI)
				.method(HttpMethod.GET)
				.name("feature")
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter(Parameters.Cache.MAX_AGE, MINUTE);
			
			server.uri(serverWebRoot + "/osmdoc/hierarchy/{lang}/{id}.{format}", osmdocAPI)
				.method(HttpMethod.GET)
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter("handler", "hierarchy")
				.parameter(Parameters.Cache.MAX_AGE, DAY);

			server.uri(serverWebRoot + "/osmdoc/poi-class/{lang}/{id}.{format}", osmdocAPI)
				.method(HttpMethod.GET)
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter("handler", "poi-class")
				.parameter(Parameters.Cache.MAX_AGE, DAY);
			
			server.uri(serverWebRoot + "/osmdoc/statistic/tagvalues.{format}", tagStatsAPI)
				.method(HttpMethod.GET)
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter(Parameters.Cache.MAX_AGE, DAY);
			
			server.uri(serverWebRoot + "/sendq.{format}", new SendQAPI())
				.method(HttpMethod.POST)
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.parameter(Parameters.Cache.MAX_AGE, DAY);
			
			server.regex(serverWebRoot + ".*", new SearchHtml())
				.flag(Flags.Auth.PUBLIC_ROUTE)
				.method(HttpMethod.GET).noSerialization();
			
			
//			server.uri(root + "/info.{format}",
//					new MetaInfoAPI(server))
//					.flag(Flags.Auth.PUBLIC_ROUTE)
//					.method(HttpMethod.GET);
//
//			server.uri(root + "/location/{id}/{_related}",
//					new FeatureAPI())
//						.alias(root + "/location/{id}")
//						.method(HttpMethod.GET)
//						.name("feature")
//						.flag(Flags.Auth.PUBLIC_ROUTE)
//						.parameter(Parameters.Cache.MAX_AGE, MINUTE);
//			
//			server.uri(root + "/location/latlon/{lat}/{lon}/{_related}",
//					new InverseGeocodeAPI())
//					.alias(root + "/location/latlon/{lat}/{lon}")
//					.alias(root + "/_inverse")
//					.method(HttpMethod.GET)
//					.name("feature")
//					.flag(Flags.Auth.PUBLIC_ROUTE)
//					.parameter(Parameters.Cache.MAX_AGE, MINUTE);
//
//
//			server.uri(root + "/osmdoc/_import",
//					new ImportOSMDoc())
//					.method(HttpMethod.GET);
//
//			server.uri(root + "/health.{format}",
//					new HealthAPI())
//					.method(HttpMethod.GET)
//					.flag(Flags.Auth.PUBLIC_ROUTE);
//
//			server.uri(root + "/index",
//					new IndexAPI())
//					.method(HttpMethod.GET);
//
//			server.uri(root + "/snapshot/.*",
//					new SnapshotsAPI(config))
//					.method(HttpMethod.GET)
//					.flag(Flags.Auth.PUBLIC_ROUTE)
//					.parameter(Parameters.Cache.MAX_AGE, MINUTE)
//					.noSerialization();
//			
//			if(config.isServeStatic()) {
//				server.uri(root + "/static/.*", new Static())
//					.method(HttpMethod.GET)
//					.flag(Flags.Auth.PUBLIC_ROUTE)
//					.noSerialization();
//			}
//			
//			server.uri(root + "/sitemap.*", new Sitemap())
//				.method(HttpMethod.GET)
//				.flag(Flags.Auth.PUBLIC_ROUTE)
//				.parameter(Parameters.Cache.MAX_AGE, WEEK)
//				.noSerialization();
	}

}
