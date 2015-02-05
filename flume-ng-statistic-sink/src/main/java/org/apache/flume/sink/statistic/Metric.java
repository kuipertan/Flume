
package org.apache.flume.sink.statistic;

public class Metric{
    private Long events;
    private Long bytes;
    public Metric() {
      events = new Long(0);
      bytes = new Long(0);
    }

    public Long getEvents(){
      return events;
    }

    public void setEvents(Long evs){
      events = evs;
    }


    public Long getBytes(){
      return bytes;
    }

    public void setBytes(Long bs){
      bytes = bs;
    }

}

