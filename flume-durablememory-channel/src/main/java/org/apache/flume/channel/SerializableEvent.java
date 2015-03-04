
package org.apache.flume.channel;

import java.util.Map;
import java.util.HashMap;
import java.io.*;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.Event;

public class SerializableEvent implements Event, Serializable{
    private Map<String, String> headers;
    private byte[] body;

    public SerializableEvent() {
        headers = new HashMap<String, String>();
        body = new byte[0];
    }
    
    public SerializableEvent(Event ev) {
        headers = new HashMap<String, String>();
        body = new byte[0];
        headers = ev.getHeaders();
        body = ev.getBody();
    }
 
    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public byte[] getBody() {
        return body;
    }
   
    @Override
    public void setBody(byte[] body) {
        if(body == null){
            body = new byte[0];
        }
        this.body = body;
    }

    @Override
    public String toString() {
        Integer bodyLen = null;
        if (body != null)
            bodyLen = body.length;
        return "[Event headers = " + headers + ", body.length = " + bodyLen + " ]";
    }
}

