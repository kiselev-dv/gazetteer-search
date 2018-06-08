package me.osm.gazetteer.search;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription="Start server")
public class ServerOptions {
	
	@Parameter(names= {"--port", "-p"})
	private int port = 8080;
	
	@Parameter(names= {"--pid-file", "-d"})
	private String pidFilePath = null;

	@Parameter(names= {"--api-root", "-r"})
	private String apiRoot = "";

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getPidFilePath() {
		return pidFilePath;
	}

	public void setPidFilePath(String pidFilePath) {
		this.pidFilePath = pidFilePath;
	}

	public String getApiRoot() {
		return apiRoot;
	}

	public void setApiRoot(String apiRoot) {
		this.apiRoot = apiRoot;
	}

	
	
}