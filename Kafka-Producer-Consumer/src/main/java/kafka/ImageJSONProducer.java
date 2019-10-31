package kafka;

import kafka.producer.ProducerConfig;
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
    SimpleProducer<String, JSONObject> producerJSON;

    public ImageJSONProducer(String brokers, String topic, int interval, String filePath) {
        this.topic = topic;
        this.interval = interval;
        this.filePath = filePath;

        Properties properties = new Properties();
        properties.put("metadata.broker.list", brokers);
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producerJSON = new SimpleProducer<String, JSONObject>(properties, false);
    }

    @Override
    public void run() {

        for (int production=1; ; production++) {
            JSONObject jSentence = new JSONObject();

            String fileName = String.valueOf(rand.nextInt(50));
            System.out.println(filePath+ fileName + ".jpg");
            tFile = new File(filePath + fileName + ".jpg");

            jSentence.put("image", readAllBytesOrExit(tFile.toPath()));
            jSentence.put("production", production);
            jSentence.put("createdTime", System.currentTimeMillis());
            producerJSON.send(this.topic, jSentence);

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

    /**
     * 바이너리 바이트 배열을 스트링으로 변환
     *
     * @param b
     * @return
     */
    public static String byteArrayToBinaryString(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.length; ++i) {
            sb.append(byteToBinaryString(b[i]));
        }
        return sb.toString();
    }

    /**
     * 바이너리 바이트를 스트링으로 변환
     *
     * @param n
     * @return
     */
    public static String byteToBinaryString(byte n) {
        StringBuilder sb = new StringBuilder("00000000");
        for (int bit = 0; bit < 8; bit++) {
            if (((n >> bit) & 1) > 0) {
                sb.setCharAt(7 - bit, '1');
            }
        }
        return sb.toString();
    }

    /**
     * 바이너리 스트링을 바이트배열로 변환
     *
     * @param s
     * @return
     */
    public static byte[] binaryStringToByteArray(String s) {
        int count = s.length() / 8;
        byte[] b = new byte[count];
        for (int i = 1; i < count; ++i) {
            String t = s.substring((i - 1) * 8, i * 8);
            b[i - 1] = binaryStringToByte(t);
        }
        return b;
    }

    /**
     * 바이너리 스트링을 바이트로 변환
     *
     * @param s
     * @return
     */
    public static byte binaryStringToByte(String s) {
        byte ret = 0, total = 0;
        for (int i = 0; i < 8; ++i) {
            ret = (s.charAt(7 - i) == '1') ? (byte) (1 << i) : 0;
            total = (byte) (ret | total);
        }
        return total;
    }

}
