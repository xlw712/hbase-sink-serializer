package com.run.flume.sink;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.dmp.ayena.mgrdata.DataTool;

public class AyenaHbaseEventSerializer implements HbaseEventSerializer {
	
	private List<Increment> i = new LinkedList<Increment>();

	private static final Logger logger = LoggerFactory.getLogger(AyenaHbaseEventSerializer.class);
	private Event event;
	private byte[] cf;
	private byte[] payload;
	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
	    this.payload = event.getBody();
	    this.cf = columnFamily;
	    this.event = event;
	}

	@Override
	public List<Row> getActions() {
		List<Row> rows = new LinkedList<Row>();
		try {
			Put put = new Put(event.getHeaders().get("RZ002001").getBytes("utf-8"));
			put.addColumn(cf, "pbc".getBytes(), DataTool.ayena2Protobuf(this.payload).toByteArray());
			rows.add(put);
			
		} catch (UnsupportedEncodingException e) {
			logger.error("ecode error",e);
		}
		return rows;
	}

	@Override
	public List<Increment> getIncrements() {
		return i;
	}

	@Override
	public void close() {

	}

}
