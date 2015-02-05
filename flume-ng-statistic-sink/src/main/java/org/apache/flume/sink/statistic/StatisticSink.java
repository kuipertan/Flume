/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.statistic;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.event.EventHelper;
import com.alibaba.fastjson.JSON;  
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;


public class StatisticSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(StatisticSink.class);
  private StatisticServer server;
  private Map<String, Metric> metrics = new HashMap<String, Metric>();
  private int port;
  public static final String SINK_HDR = "sink_name";


  public void updateMetric(String key, long incr){
    Metric val = metrics.get(key);
    if (val == null) {
      val = new Metric();
    }

	val.setBytes(val.getBytes() + incr);
	val.setEvents(val.getEvents() + 1);
    metrics.put(key, val);

  }

  public Metric fetchMetric(String key){
  	return metrics.get(key); 
  }

  @Override
  public Status process() throws EventDeliveryException{
    Status status = null;

    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();

    try{
      
      Event event = ch.take();
      if (event != null ){

        int bodyLen = event.getBody().length;
        Map<String, String> headers = event.getHeaders();
	    String sinkName = null;
        sinkName = headers.get(SINK_HDR);

        if (sinkName != null){
          updateMetric(sinkName, bodyLen);
        }

      }else {
	logger.info("Flume idle");
      }
      txn.commit();
      status = Status.READY;
    }catch (Throwable t) {
      txn.rollback();
      status = Status.BACKOFF;
      if (t instanceof Error) {
         throw (Error)t;
      }
    }finally{
      if (txn != null) {
        txn.close();
      }
    }
    return status;
  }

  @Override
  public synchronized void start() {
    // instantiate the producer
    //
    metrics.clear();
    server = new StatisticServer(this,port); 
	try{
    	server.serverEver();
	}catch (Throwable e){
		logger.error(e.getMessage());
	}

    super.start();
  }

  @Override
  public synchronized void stop() {
    server.stop();
    super.stop();
  }

  /*
   * We configure the sink and generate properties for the Kafka Producer
   *
   * producer properties is generated as follows:
   * 1. We generate a properties object with some static defaults that
   * can be overridden by Sink configuration
   *
   * @param context
   */

  @Override
  public void configure(Context context) {

    port = context.getInteger(StatisticSinkConstants.LISTEN, 56767);
 
  }
  
}
