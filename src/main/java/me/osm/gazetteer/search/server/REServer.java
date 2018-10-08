package me.osm.gazetteer.search.server;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.restexpress.RestExpress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

import me.osm.gazetteer.search.ServerOptions;
import me.osm.gazetteer.search.api.SearchAPI;
import me.osm.gazetteer.search.api.osmdoc.OSMDocAPI;
import me.osm.gazetteer.search.server.postprocessor.AllowOriginPP;
import me.osm.gazetteer.search.server.postprocessor.LastModifiedHeaderPostprocessor;
import me.osm.gazetteer.search.server.postprocessor.MarkHeaderPostprocessor;

public class REServer {
	
	private static final Logger log = LoggerFactory.getLogger(REServer.class);
	private static volatile REServer INSTANCE;
	private static volatile ServerOptions options = new ServerOptions();

	public static final REServer getInstance(ServerOptions opts) {
		options = opts;
		return getInstance();
	}
	
	public static final REServer getInstance() {
		if (INSTANCE != null) {
			return INSTANCE;
		}
		
		synchronized (REServer.class) {
			if (INSTANCE == null) {
				INSTANCE = new REServer();
			}
			return INSTANCE;
		}
	}

	private final RestExpress server;
	
	private REServer () {
		Injector injector = Guice.createInjector(new REServerModule());
		
		server = new RestExpress()
				.setUseSystemOut(false)
				.setPort(options.getPort())
				.setName(getServerName())
				.addPostprocessor(new LastModifiedHeaderPostprocessor())
				.addPostprocessor(new AllowOriginPP())
				.addPostprocessor(new MarkHeaderPostprocessor())
				.addPreprocessor(new BasikAuthPreprocessor(getRealmName(), getAdminPasswordHash()));
		
		SearchAPI searchAPI = injector.getInstance(SearchAPI.class);
		OSMDocAPI osmdocAPI = new OSMDocAPI(options.getOsmdocPath());
		REServerRoutes routes = new REServerRoutes(searchAPI, osmdocAPI);
		routes.defineRoutes(server, options.getApiRoot());
		
		server.addMessageObserver(new HttpLogger());
		server.bind(options.getPort());
		
		log.info("Listen on port: {}", options.getPort());
		
		long pid = Long.valueOf(StringUtils.substringBefore(ManagementFactory.getRuntimeMXBean().getName(), "@"));
		log.info("PID {}", pid);

		String pidFilePath = options.getPidFilePath();
		if (pidFilePath != null) {
			File pidFile = new File(pidFilePath);
			try {
				FileUtils.writeStringToFile(pidFile, String.valueOf(pid));
			} catch (IOException e) {
				log.warn("Can't save pid to file {}", pidFile);
				e.printStackTrace();
			}
			pidFile.deleteOnExit();
		}

		server.awaitShutdown();
	}

	private String getRealmName() {
		return "gazetteer-search";
	}


	private String getServerName() {
		return "gazetteer";
	}
	
	private String getAdminPasswordHash() {
		return Hex.encodeHexString(DigestUtils.sha("test"));
	}
	
	public static void main(String[] args) {
		REServer.getInstance();
	}

}
