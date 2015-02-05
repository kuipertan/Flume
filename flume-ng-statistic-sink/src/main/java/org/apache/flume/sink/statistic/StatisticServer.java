
package org.apache.flume.sink.statistic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticServer{
	private static final Logger logger = LoggerFactory.getLogger(StatisticServer.class);
	private HttpServer server;
    private StatisticSink respSource;
	private int listenPort;

	public StatisticServer(StatisticSink sink, int port){
		listenPort = port;
		respSource = sink;
	}

	public void serverEver() throws IOException{
		HttpServerProvider provider = HttpServerProvider.provider();
		server = provider.createHttpServer(new InetSocketAddress(listenPort),32);
		server.createContext("/metric", new SimpleHttpHandler());
		server.setExecutor(null);
		server.start();
		logger.info("Metrics server started at port {}", listenPort);
	}

    public void stop(){
		server.stop(1);
	}

	class SimpleHttpHandler implements HttpHandler {
		public void handle(HttpExchange httpExchange) throws IOException{
			InputStream in = httpExchange.getRequestBody();
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String reqpath = httpExchange.getRequestURI().getRawPath();
			String [] paths = reqpath.split("/");
			String respMsg = "";
			int code = 200;
			if (paths.length != 4){
				code = 404;
				respMsg = "Unrecorgnized paths";
			}
			else{
				Metric m = respSource.fetchMetric(paths[2]);
			    if (m == null){
					code = 404;
					respMsg = "Not found sink: " + paths[2];
				}
				else if (paths[3].equals("bytes")){	
					respMsg = m.getBytes().toString();
				}
				else if (paths[3].equals("events")){	
					respMsg = m.getEvents().toString();
				}
			    else {
					code = 404;
					respMsg = "Not found item: " + paths[3];
				}
			}
            httpExchange.sendResponseHeaders(code, respMsg.length());   
            OutputStream out = httpExchange.getResponseBody(); 
            out.write(respMsg.getBytes());  
            out.flush();  
            httpExchange.close();                                 
		}
	}
}
