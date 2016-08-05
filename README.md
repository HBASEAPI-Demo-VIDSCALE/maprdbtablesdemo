# MapR-DB Java API Demo

The Project has two main classes

1) DataLoader - loads data to the mapr-db table /tables/property_global_byhour from the property_global_hour.tsv file in the resources directory

Copy the generated uber jar file to the one of the node (e.g. below in 04 node in ghq cluster)

$ java -cp \`mapr classpath\`:/tmp/uber-maprdbtablesdemo-0.1.jar com.mapr.demo.DataLoader

2) Query - queries the above table for a given key range

The Query needs foour parameters account id, group id, start time and end time (time in MM/dd/yyyHH:mm:ss in GMT time)

$ java -cp \`mapr classpath\`:/tmp/uber-maprdbtablesdemo-0.1.jar com.mapr.demo.Query 20005 20006 07/14/201617:00:00 07/14/201618:00:00
