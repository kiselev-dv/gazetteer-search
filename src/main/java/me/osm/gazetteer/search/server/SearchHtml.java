package me.osm.gazetteer.search.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.restexpress.Request;
import org.restexpress.Response;

public class SearchHtml {
	
	public String read(Request req, Response res) throws FileNotFoundException, IOException
	{
		String filename = req.getHeader("filename");
		if (StringUtils.isEmpty(filename)) {
			filename = "index.html";
		}
		
		File file = new File(System.getProperty("user.dir") + "/static/html/" + filename);
		if (file.exists()) {
			res.setContentType("text/html; charset=UTF-8");
			return IOUtils.toString(new FileInputStream(file));
		}
		
		res.setResponseCode(404);
		return null;
	}

}
