package com.mapr.demo;

import com.opencsv.CSVReader;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mlalapet on 8/3/16.
 */
public class DataLoader {

    public static void main(String args[]) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("property_global_hour.tsv");

        CSVReader reader = new CSVReader(new InputStreamReader(is), '\t');

        List<PropertyGlobalHour> dataList = new ArrayList();

        String [] nextLine;
        int count = 0;
        while ((nextLine = reader.readNext()) != null) {
            if(count > 0) {
                PropertyGlobalHour data = new PropertyGlobalHour();
                data.setEpochStart(Long.valueOf(nextLine[0]));
                data.setAccountId(Integer.valueOf(nextLine[1]));
                data.setGroupId(Integer.valueOf(nextLine[2]));
                data.setProperty(nextLine[3]);
                data.setServiceType(nextLine[4]);
                data.setFlowDir(nextLine[5]);
                data.setBytes(Double.valueOf(nextLine[6]));
                data.setRequests(Double.valueOf(nextLine[7]));
                data.setConnections(Long.valueOf(nextLine[8]));
                data.setAvgFbl(Double.valueOf(nextLine[9]));
                data.setChitRatio(Double.valueOf(nextLine[10]));
                data.setUniqueVisitors(Long.valueOf(nextLine[11]));
                dataList.add(data);
            }
            count++;
        }

        PropertyGlobalHourDAO dao = new PropertyGlobalHourDAO(ConnectionFactory.createConnection(HBaseConfiguration.create()));
        dao.insert(dataList);
        System.out.println(dataList.size());
    }
}
