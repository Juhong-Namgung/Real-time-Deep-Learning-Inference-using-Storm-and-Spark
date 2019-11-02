package kafka;

import kafka.producer.ProducerConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;

public class ImageJSONProducer implements Runnable {
    private static Log LOG = LogFactory.getLog(ImageJSONProducer.class);
    private String topic;
    private int interval;
    private Random rand = new Random();
    private File tFile;
    private String filePath;
    SimpleProducer<String, String> producerJSONstr;


    public ImageJSONProducer(String brokers, String topic, int interval, String filePath) {
        this.topic = topic;
        this.interval = interval;
        this.filePath = filePath;

        Properties properties = new Properties();
        properties.put("metadata.broker.list", brokers);
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producerJSONstr = new SimpleProducer<String, String>(properties, false);
    }

    @Override
    public void run() {

        for (int production=1; ; production++) {

            // read random image data
            String fileName = String.valueOf(rand.nextInt(50));
            tFile = new File(filePath + fileName + ".jpg");
            byte[] readImage = readAllBytesOrExit(tFile.toPath());

            // byte[] to str
            String base64String = Base64.encodeBase64String(readImage);

            // JSON data format
            JSONObject jSentence = new JSONObject();
            jSentence.put("image", base64String);
            jSentence.put("production", production);
            jSentence.put("createdTime", System.currentTimeMillis());

            // send to kafka
            producerJSONstr.send(this.topic, jSentence.toString());
            try {
                Thread.sleep(this.interval);
            } catch (InterruptedException e) {}
        }

    }

    private static byte[] readAllBytesOrExit(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println(e.toString());
            System.err.println("Failed to read [" + path + "]: " + e.getMessage());
            System.err.println("Here");
            System.exit(1);
        }
        return null;
    }
}
