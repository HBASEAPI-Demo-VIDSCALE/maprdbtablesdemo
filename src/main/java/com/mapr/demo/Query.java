package com.mapr.demo;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by mlalapet on 8/3/16.
 */
public class Query {

    public static void main(String args[]) throws IOException, ParseException {
        //args[0] - account id
        //args[1] - start time in MM/dd/yyyHH:mm:ss
        //args[2] - end time in MM/dd/yyyHH:mm:ss
        //args[3] - group id (optional)

        SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyyHH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date startdate = df.parse(args[1]);
        Date enddate = df.parse(args[2]);

        PropertyGlobalHourDAO.QueryBuilder builder = new PropertyGlobalHourDAO.QueryBuilder(
                ConnectionFactory.createConnection(HBaseConfiguration.create()))
                .withAccountId(Integer.parseInt(args[0]))
                .withTimeRange(startdate,enddate);

        if(args.length > 3) {
            builder.withGroupId(Integer.parseInt(args[3]));
            builder.withFlowdir("out");
        }

        List<PropertyGlobalHour> result = builder.query();

        System.out.println(result.size());
    }
}
