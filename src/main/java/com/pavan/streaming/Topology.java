package com.pavan.streaming;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.pavan.streaming.StockDataBolt;

/**
 *
 * @author hkropp
 */
public class Topology {

    public static final String KAFKA_SPOUT_ID = "kafka-spout";
    public static final String STOCK_PROCESS_BOLT_ID = "stock-process-bolt";
    public static final String HIVE_BOLT_ID = "hive-stock-price-bolt";

    public static void main(String... args) {
        Topology app = new Topology();
        app.run(args);
    }
    
    public void run(String... args){
        String kafkaTopic = "sampletest32";

        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("192.168.5.38"),
                kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        // Hive connection configuration
        String metaStoreURI = "thrift://192.168.5.39:9083";
        String dbName = "default";
        String tblName = "stock_prices";
        // Fields for possible partition
        String[] partNames = {"PRODUCT_ID"};
        // Fields for possible column data
        String[] colNames = {"DATE_TIME", "PRODUCT_ID", "USER_ID", "SCAN_STATUS", "SECURITY_PROCESS", "ANALYTIC_PROCESS", 
        		"ENTITY_ID", "ZIPCODE"};
        // Record Writer configuration
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));

        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(100)
                .withIdleTimeout(10)              
                .withCallTimeout(100);
        
                //.withKerberosKeytab(path_to_keytab)
                //.withKerberosPrincipal(krb_principal);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
        builder.setBolt(STOCK_PROCESS_BOLT_ID, new StockDataBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).shuffleGrouping(STOCK_PROCESS_BOLT_ID);
        
        String topologyName = "StormHiveStreamingTopo";
        Config config = new Config();
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(60);
        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
