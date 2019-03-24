package kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class KafkaWordCount {

    public static class SplitSentenceBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("str");
            // split with space
            String[] words = line.split(" ");
            for (String word : words) {
                if (!word.isEmpty()) {
                    this.collector.emit(tuple,new Values(word));
                    counter(CounterType.EMIT);
                }
            }
            this.collector.ack(tuple);
            counter(CounterType.ACK);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {

        private OutputCollector collector;
        private LinkedHashMap<String, Integer> counterMap;
        private int NumOfThe = 10;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            this.counterMap = new LinkedHashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            if (word.equals("the") && NumOfThe > 0){
                NumOfThe--;
                System.out.println("fail word \"the\" "+ NumOfThe + " time.");
                this.collector.fail(tuple);
            }else{
                if (counterMap.containsKey(word)) {
                    counterMap.put(word, counterMap.get(word) + 1);
                } else {
                    counterMap.put(word, 1);
                }
                this.collector.ack(tuple);
                counter(CounterType.ACK);
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

        @Override
        public void cleanup() {
            // dump counters into the console
            dumpCounters();

            System.out.println("cleanup, sortByValue counterMap start");
            // sort and save result into local file
            Utils.sortByValue(counterMap, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });

            System.out.println("cleanup, start to save counterMap into file");
            FileWriter fw = null;
            BufferedWriter writer = null;
            try {
                fw = new FileWriter(RESULT_PATH);
                writer = new BufferedWriter(fw);
                for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                    writer.write(entry.getKey() + "\t" + String.valueOf(entry.getValue()));
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                    if (fw != null) {
                        fw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("cleanup, end save counterMap into file");
        }
    }

    // Counter Code START
    // !!! LOCAL USED ONLY
    public static int emit_counter = 0;
    public static int ack_counter = 0;

    public static enum CounterType {EMIT, ACK};

    public static synchronized void counter(KafkaWordCount.CounterType type) {
        if (type == KafkaWordCount.CounterType.EMIT) {
            emit_counter += 1;
        } else if (type == KafkaWordCount.CounterType.ACK) {
            ack_counter += 1;
        }
    }

    public static void dumpCounters() {
        System.out.println("--------DUMP COUNTERS START--------");
        System.out.println("The number of tuple emitted:" + emit_counter);
        System.out.println("The number of tuple acked:" + ack_counter);
        System.out.println("The number of tuple failed:" + (emit_counter - ack_counter));
        System.out.println("--------DUMP COUNTERS END--------");
    }
    // Counter Code END

    // these two path need to be replaced to local path if debug in local
    public static String DATA_PATH = "/home/tian/hw2-src/StormData.txt";
    public static final String RESULT_PATH = "/home/tian/hw3/wordcount_result.txt";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        String zkConnString = "master:2181,slave-1:2181,slave-2:2181";
        String topicName = "wordcount";

        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName , "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // how to use KafkaSpout?
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("spout", kafkaSpout, 3);
        builder.setBolt("split", new KafkaWordCount.SplitSentenceBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new KafkaWordCount.WordCountBolt(), 1).globalGrouping("split");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1024);
        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(4);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());

                // local debug cluster only process 20min
                // make sure the time is long enough until the debug user stop manually
                Thread.sleep(60 * 1000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}

class Utils {
    public static <K, V> void sortByValue(
            LinkedHashMap<K, V> m, final Comparator<? super V> c) {
        List<Map.Entry<K, V>> entries = new ArrayList<Map.Entry<K, V>>(m.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> lhs, Map.Entry<K, V> rhs) {
                return c.compare(lhs.getValue(), rhs.getValue());
            }
        });

        m.clear();
        for (Map.Entry<K, V> e : entries) {
            m.put(e.getKey(), e.getValue());
        }
    }
}
