package com.mapr.demo;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mlalapet on 8/3/16.
 */
public class PropertyGlobalHourDAO {
    public static final String TABLE_NAME = "/tables/property_global_byhour";
    public static final byte[] CF = Bytes.toBytes("datacf");

    public static final byte[] BYTES_COL = Bytes.toBytes("bytes");
    public static final byte[] REQUESTS_COL = Bytes.toBytes("requests");
    public static final byte[] CONNECTIONS_COL = Bytes.toBytes("connections");
    public static final byte[] AVGFBL_COL = Bytes.toBytes("avgfbl");
    public static final byte[] CHITRATIO_COL = Bytes.toBytes("chitratio");
    public static final byte[] UNIQVIS_COL = Bytes.toBytes("uniqvis");

    private Connection connection = null;

    static class QueryBuilder {
        private PropertyGlobalHourDAO dao = null;
        private Date startTime;
        private Date endTime;
        private int accountId = -1;
        private int groupId = -1;
        private String flowDir;
        private String property;
        private String serviceType;

        QueryBuilder(Connection connection){
            this.dao = new PropertyGlobalHourDAO(connection);
        }
        QueryBuilder withTimeRange(Date startTime, Date endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
            return this;
        }
        QueryBuilder withAccountId(int accountId) {
            this.accountId = accountId;
            return this;
        }
        QueryBuilder withGroupId(int groupId) {
            this.groupId = groupId;
            return this;
        }
        QueryBuilder withFlowdir(String flowDir) {
            this.flowDir = flowDir;
            return this;
        }
        QueryBuilder withProperty(String property) {
            this.property = property;
            return this;
        }
        QueryBuilder withServiceType(String serviceType) {
            this.serviceType = serviceType;
            return this;
        }
        List<PropertyGlobalHour> query() throws IOException {
            if(validate()){
                String startRowKey = this.accountId+"_"+startTime.getTime();
                startRowKey = groupId != -1 ? startRowKey+"_"+this.groupId : startRowKey;
                startRowKey = flowDir != null ? startRowKey+"_"+this.flowDir : startRowKey;
                startRowKey = property != null ? startRowKey+"_"+this.property : startRowKey;
                startRowKey = serviceType != null ? startRowKey+"_"+this.serviceType : startRowKey;

                String endRowKey = this.accountId+"_"+endTime.getTime();
                endRowKey = groupId != -1 ? endRowKey+"_"+this.groupId : endRowKey;
                endRowKey = flowDir != null ? endRowKey+"_"+this.flowDir : endRowKey;
                endRowKey = property != null ? endRowKey+"_"+this.property : endRowKey;
                endRowKey = serviceType != null ? endRowKey+"_"+this.serviceType : endRowKey;

                System.out.println(startRowKey+" to "+endRowKey);
                return this.dao.scan(startRowKey, endRowKey);

            } else {
                System.err.println("accountId, groupId, startTime, endTime and flowDir are mandatory");
            }
            return null;
        }

        private boolean validate() {
            if(accountId != -1 && startTime != null && endTime !=null )
                return true;
            return false;
        }
    }

    private List<PropertyGlobalHour> scan(String startRowKey, String endRowKey) throws IOException {
        List<PropertyGlobalHour> returnResult = new ArrayList<>();
        Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));

        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            PropertyGlobalHour data = new PropertyGlobalHour();
            data.setBytes(Bytes.toDouble(result.getValue(CF,BYTES_COL)));
            data.setRequests(Bytes.toDouble(result.getValue(CF,REQUESTS_COL)));
            data.setConnections(Bytes.toLong(result.getValue(CF,CONNECTIONS_COL)));
            data.setAvgFbl(Bytes.toDouble(result.getValue(CF,AVGFBL_COL)));
            data.setChitRatio(Bytes.toDouble(result.getValue(CF,CHITRATIO_COL)));
            data.setUniqueVisitors(Bytes.toLong(result.getValue(CF,UNIQVIS_COL)));
            returnResult.add(data);
        }
        return returnResult;
    }

    public PropertyGlobalHourDAO(Connection connection)  {
        this.connection = connection;
    }

    public void insert(PropertyGlobalHour data) throws IOException {

        Put p = createPut(data);

        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        table.put(p);
        table.close();
    }

    public void insert(List<PropertyGlobalHour> dataList) throws IOException {

        List<Put> puts = new ArrayList<>();
        for(PropertyGlobalHour data : dataList) {
            Put p = createPut(data);
            puts.add(p);
        }

        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        table.put(puts);
        table.close();
    }

    private Put createPut(PropertyGlobalHour data) {
        String rowKey = generateRowkey(data);
        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(CF, BYTES_COL, Bytes.toBytes(data.getBytes()));
        p.addColumn(CF, REQUESTS_COL, Bytes.toBytes(data.getRequests()));
        p.addColumn(CF, CONNECTIONS_COL, Bytes.toBytes(data.getConnections()));
        p.addColumn(CF, AVGFBL_COL, Bytes.toBytes(data.getAvgFbl()));
        p.addColumn(CF, CHITRATIO_COL, Bytes.toBytes(data.getChitRatio()));
        p.addColumn(CF, UNIQVIS_COL, Bytes.toBytes(data.getUniqueVisitors()));
        return p;
    }

    private String generateRowkey(PropertyGlobalHour data) {
        return data.getAccountId()+"_"+data.getEpochStart()+"_"+data.getGroupId()
                +"_"+data.getFlowDir()+"_"+data.getProperty()+"_"+data.getServiceType();
    }
    //To upsert/increment a unique visitors column value
    public Result uniqvisIncrement(String rowKey, long amount) throws Exception {
		
    	Result result = null;
    	Table table = null;
    	try{
    		Increment inc = new Increment(Bytes.toBytes(rowKey));
    		//add the amount value to the current value in the column, if want to decrease just
    		// negate the amount
    		inc.addColumn(CF, UNIQVIS_COL, amount);

    	    table = connection.getTable(TableName.valueOf(TABLE_NAME));
    		result = table.increment(inc);
    	}
    	finally
    	{
    		if(table != null)
    			table.close();
    	}
    	
		return result;
	}
}
