package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaFileProducer extends Thread {

    private static final String topicName
            = "wordcount";
    public static final String fileName = "/home/tian/hw2-src/StormData.txt";

    private final KafkaProducer<String, String> producer;
    private final Boolean isAsync;

    public KafkaFileProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks","1");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        this.isAsync = isAsync;
    }

    public void sendMessage(String key, String value) {
//        long startTime = System.currentTimeMillis();
//        if (isAsync) { // Send asynchronously
//
//        } else { // Send synchronously
        try {
            producer.send(
                    new ProducerRecord<String, String>(topicName,value))
                    .get();
            //System.out.println("Sent message: (" + key + ", " + value + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    public static void main(String [] args){
        KafkaFileProducer producer = new KafkaFileProducer(topicName, false);
        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(fileName);
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(fis));

            String line = null;
            while ((line = br.readLine()) != null) {
                lineCount++;
                producer.sendMessage(lineCount+"", line);
            }
            producer.producer.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
