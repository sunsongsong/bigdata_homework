package cn.sunss.learn;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @ClassName: Main
 * @Description: kafkaToStormToHdfs程序入口
 * @Author sunsongsong
 * @Date 2019/8/6 21:20
 * @Version 1.0
 */
public class Main {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //第一步：使用现成的kafkaSpout
        KafkaSpoutConfig.Builder<String, String> kafkaSpoutConfigBuilder =
                KafkaSpoutConfig.builder("node01:9092,node02:9092,node03:9092", "stormTopic2");
        //控制kafka的offset的消费策略
//        kafkaSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        kafkaSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST);
        kafkaSpoutConfigBuilder.setGroupId("kafkaToStorm");
        kafkaSpoutConfigBuilder.setOffsetCommitPeriodMs(1000L);
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = kafkaSpoutConfigBuilder.build();
        //kafkaSpout需要KafkaSpoutConfig
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(kafkaSpoutConfig);

        //第二步：创建wordCountBolt
        WordCountBolt wordCountBolt = new WordCountBolt();

        //第三步：创建HdfsBolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        //文件的控制策略。第一种：数据条数的多少 第二种：文件的内容大小
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.KB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat() .withPath("/stormToHdfs/");
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://node01:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout);
        builder.setBolt("wordCountBolt",wordCountBolt).localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("hdfsBolt",hdfsBolt).localOrShuffleGrouping("wordCountBolt");

        //最后：提交运行
        Config config  = new Config();
        if(args !=null && args.length >0){
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafkaToStormToHdfs",config,builder.createTopology());
        }


    }

}
