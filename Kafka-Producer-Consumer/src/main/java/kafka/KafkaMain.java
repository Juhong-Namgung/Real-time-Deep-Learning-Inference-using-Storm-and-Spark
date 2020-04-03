package kafka;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class KafkaMain {

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--inputTopic", aliases = {"--input"}, metaVar = "INPUT TOPIC", usage = "name of input kafka topic")
    private static String inputTopic = "input";

    @Option(name = "--outputTopic", aliases = {"--output"}, metaVar = "OUTPUT TOPIC", usage = "name of output kafka topic")
    private static String outputTopic = "output";

    @Option(name = "--testTime", aliases = {"--t"}, metaVar = "TIME", usage = "how long should run topology")
    private static int testTime = 3;

    @Option(name = "--interval", aliases = {"--i"}, metaVar = "INTERVAL", usage = "interval time to produce")
    private static int interval = 10;

    @Option(name = "--zookeeperHosts", aliases = {"--zookeeper"}, metaVar = "ZOOKEEPER HOST", usage = "path of zookeeper host")
        private static String zkhosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";

    @Option(name = "--brokerList", aliases = {"--broker"}, metaVar = "BROKER LIST", usage = "path of broker list, bootstrap servers")
    private static String bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092";

    @Option(name = "--filePath", aliases = {"--path"}, metaVar = "FILE PATH", usage = "path of image files")
    private static String path = "./data/";

    public static void main(String[] args) throws InterruptedException {
        new KafkaMain().topoMain(args);
    }


    public void topoMain(String[] args) throws InterruptedException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(150);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            _help = true;
        }
        if (_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        Thread producer = new Thread(new ImageJSONProducer(bootstrap, inputTopic, interval, path));
        Thread consumer = new Thread(new PerfConsumer(zkhosts, outputTopic, "test-group", testTime));

        producer.start();
        consumer.start();
        try {
            for (int i=1; i<=10; i++) {
                Thread.sleep(testTime*1000*60/10);
                System.out.println("TESING IN PROGRESS... " + (i*10) + "%");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        producer.interrupt();
        producer.stop();

        try {
            System.out.println("SHUTTING DOWN... ");
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
