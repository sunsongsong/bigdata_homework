package cn.sunss.learn;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: WordCountBolt
 * @Description:
 * @Author sunsongsong
 * @Date 2019/8/6 21:28
 * @Version 1.0
 */
public class WordCountBolt extends BaseRichBolt {

    private  OutputCollector collector;

    private static ConcurrentHashMap<String,Integer> wordCountMap = new ConcurrentHashMap<String,Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //获取下标是4的那个数据，就是我们kafka当中的数据
        Object value = input.getValue(4);
        if(value != null && !"".equals(value.toString())){
            String[] line = value.toString().split(" ");

            for(String word : line){
//                System.out.println(word);
                if(wordCountMap.containsKey(word)){
                    wordCountMap.put(word,wordCountMap.get(word)+1);
                }else{
                    wordCountMap.put(word,1);
                }
            }
        }
        System.out.println(wordCountMap.toString());

        collector.emit(new Values(wordCountMap.toString()));

//        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordCountMap"));
    }
}
