package perf.image;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.tensorflow.*;
import org.tensorflow.types.UInt8;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class InceptionDriver {


    private String modelPath = "/home/team1/juhong/kepco/tensorflowforjava/resultmodel/inception5h";
    private String modelName = "tensorflow_inception_graph.pb";
    private byte[] graphDefs = readAllBytesOrExit(Paths.get(modelPath, modelName));
    ;
    private List<String> labels = readAllLinesOrExit(Paths.get(modelPath, "imagenet_comp_graph_label_strings.txt"));
    ;
    private byte[] imageBytes;

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--appName", aliases = {"--name"}, metaVar = "APP NAME", usage = "name of App")
    private static String appName = "SparkApp";

    @Option(name = "--inputTopic", aliases = {"--input"}, metaVar = "INPUT TOPIC", usage = "name of input kafka topic")
    private static String inputTopic = "input";

    @Option(name = "--outputTopic", aliases = {"--output"}, metaVar = "OUTPUT TOPIC", usage = "name of output kafka topic")
    private static String outputTopic = "image-output-spark";

    @Option(name = "--testTime", aliases = {"--t"}, metaVar = "TIME", usage = "how long should run topology")
    private static int testTime = 3;

    @Option(name = "--interval", aliases = {"--i"}, metaVar = "INTERVAL", usage = "interval time")
    private static int interval = 3;

    @Option(name = "--zookeeperHosts", aliases = {"--zookeeper"}, metaVar = "ZOOKEEPER HOST", usage = "path of zookeeper host")
    private static String zkhosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";

    @Option(name = "--brokerList", aliases = {"--broker"}, metaVar = "BROKER LIST", usage = "path of broker list, bootstrap servers")
    private static String bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092";

//    @Option(name = "--modelPath", aliases = {"--model"} , metaVar = "TENSORFLOW MODEL PATH", usage ="path of deep learning model")
//    private static String modelPath = "./models/";


    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
        new InceptionDriver().topoMain(args);
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

        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.milliseconds(interval));

        // Kafka input
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", bootstrap);
        kafkaParams.put("bootstrap.servers", bootstrap);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("group.id", "test-group");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // Make Kafka Producer.
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "1");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // topic
        Collection<String> topics = Arrays.asList(inputTopic);

        // Streams.
        final JavaInputDStream<ConsumerRecord<String, byte[]>> kafkaStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );
//        final JavaInputDStream<ConsumerRecord<String,String>> stream =
//                KafkaUtils.createDirectStream(
//                        jssc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );

        // Processing
//        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        //JavaDStream<String[]> inputs = stream.map(ConsumerRecord::value);
        JavaDStream<byte[]> inputs = kafkaStream.map(ConsumerRecord::value);

        // To solve Serializable Exception
        byte[] graphDefsRDD = this.graphDefs;
        List<String> labelsRDD = this.labels;

        JavaDStream<String> files = inputs.map(input -> {
            try (Tensor<Float> image = constructAndExecuteGraphToNormalizeImage(input)) {

                float[] labelProbabilities = executeInceptionGraph(graphDefsRDD, image);

                int bestLabelIdx = maxIndex(labelProbabilities);
                String label = labelsRDD.get(bestLabelIdx) ;
                float prob = labelProbabilities[bestLabelIdx] * 100f;

                return  "Imgae(byte): " + input + " 's Predict Result: " + label + " (" + prob + "%)";
            }

        });
//        JavaPairDStream<byte[], String> results = inputs.mapToPair(input-> {
//            try (Tensor<Float> image = constructAndExecuteGraphToNormalizeImage(input)) {
//
//                float[] labelProbabilities = executeInceptionGraph(graphDefsRDD, image);
//
//                int bestLabelIdx = maxIndex(labelProbabilities);
//
//                return new Tuple2(input, labelsRDD.get(bestLabelIdx));
//            }
//
//        });

//        results.foreachRDD( result -> {
//            result.foreach(tokafka -> {
//
//                KafkaProducer<byte[], String> producer = new KafkaProducer<byte[], String>(props);
//                producer.send(new ProducerRecord<byte[], String>(outputTopic, tokafka.toString()));
//                producer.close();
//            });
//        });
//        for( String tuple: output.collect() ) {
//            producer.send(new ProducerRecord<String, String>(output_topic, tuple+","+System.currentTimeMillis()));
//        }
//    });

        files.foreachRDD( result -> {
            result.foreach(tokafka -> {
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
                String out = tokafka + "," + System.currentTimeMillis();
                System.out.println(out);
                producer.send(new ProducerRecord<String, String>(outputTopic, out));
//                KafkaProducer<byte[], String> producer = new KafkaProducer<byte[], String>(props);
//                producer.send(new ProducerRecord<byte[], String>(outputTopic, tokafka.toString().getBytes(), tokafka.toString()));
                producer.close();
            });
        });
        files.print();

        jssc.start();
        jssc.awaitTermination();


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

    private static List<String> readAllLinesOrExit(Path path) {
        try {
            return Files.readAllLines(path, Charset.forName("UTF-8"));
        } catch (IOException e) {
            System.err.println("Failed to read [" + path + "]: " + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    private static Tensor<Float> constructAndExecuteGraphToNormalizeImage(byte[] imageBytes) {
        try (Graph g = new Graph()) {
            GraphBuilder b = new GraphBuilder(g);
            // Some constants specific to the pre-trained model at:
            // https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
            //
            // - The model was trained with images scaled to 224x224 pixels.
            // - The colors, represented as R, G, B in 1-byte each were converted to
            // float using (value - Mean)/Scale.
            final int H = 224;
            final int W = 224;
            final float mean = 117f;
            final float scale = 1f;

            // Since the graph is being constructed once per execution here, we can use a
            // constant for the
            // input image. If the graph were to be re-used for multiple input images, a
            // placeholder would
            // have been more appropriate.
            final Output<String> input = b.constant("input", imageBytes);
            final Output<Float> output = b
                    .div(b.sub(
                            b.resizeBilinear(b.expandDims(b.cast(b.decodeJpeg(input, 3), Float.class),
                                    b.constant("make_batch", 0)), b.constant("size", new int[]{H, W})),
                            b.constant("mean", mean)), b.constant("scale", scale));
            try (Session s = new Session(g)) {
                // Generally, there may be multiple output tensors, all of them must be closed
                // to prevent resource leaks.
                return s.runner().fetch(output.op().name()).run().get(0).expect(Float.class);
            }
        }
    }

    private static float[] executeInceptionGraph(byte[] graphDef, Tensor<Float> image) {
        try (Graph g = new Graph()) {
            g.importGraphDef(graphDef);
            Tensor<Float> mid_result_out = null;

            try (Session s = new Session(g);
                 // Generally, there may be multiple output tensors, all of them must be closed
                 // to prevent resource leaks.
                 Tensor<Float> result = s.runner().feed("input", image).fetch("output").run().get(0)
                         .expect(Float.class)) {
                final long[] rshape = result.shape();
                if (result.numDimensions() != 2 || rshape[0] != 1) {
                    throw new RuntimeException(String.format(
                            "Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s",
                            Arrays.toString(rshape)));
                }
                int nlabels = (int) rshape[1];
                return result.copyTo(new float[1][nlabels])[0];
            }
        }
    }

    private static int maxIndex(float[] probabilities) {
        int best = 0;
        for (int i = 1; i < probabilities.length; ++i) {
            if (probabilities[i] > probabilities[best]) {
                best = i;
            }
        }
        return best;
    }

    static class GraphBuilder {
        GraphBuilder(Graph g) {
            this.g = g;
        }

        Output<Float> div(Output<Float> x, Output<Float> y) {
            return binaryOp("Div", x, y);
        }

        <T> Output<T> sub(Output<T> x, Output<T> y) {
            return binaryOp("Sub", x, y);
        }

        <T> Output<Float> resizeBilinear(Output<T> images, Output<Integer> size) {
            return binaryOp3("ResizeBilinear", images, size);
        }

        <T> Output<T> expandDims(Output<T> input, Output<Integer> dim) {
            return binaryOp3("ExpandDims", input, dim);
        }

        <T, U> Output<U> cast(Output<T> value, Class<U> type) {
            DataType dtype = DataType.fromClass(type);
            return g.opBuilder("Cast", "Cast").addInput(value).setAttr("DstT", dtype).build().<U>output(0);
        }

        Output<UInt8> decodeJpeg(Output<String> contents, long channels) {
            return g.opBuilder("DecodeJpeg", "DecodeJpeg").addInput(contents).setAttr("channels", channels).build()
                    .<UInt8>output(0);
        }

        <T> Output<T> constant(String name, Object value, Class<T> type) {
            try (Tensor<T> t = Tensor.<T>create(value, type)) {
                return g.opBuilder("Const", name).setAttr("dtype", DataType.fromClass(type)).setAttr("value", t).build()
                        .<T>output(0);
            }
        }

        Output<String> constant(String name, byte[] value) {
            return this.constant(name, value, String.class);
        }

        Output<Integer> constant(String name, int value) {
            return this.constant(name, value, Integer.class);
        }

        Output<Integer> constant(String name, int[] value) {
            return this.constant(name, value, Integer.class);
        }

        Output<Float> constant(String name, float value) {
            return this.constant(name, value, Float.class);
        }

        private <T> Output<T> binaryOp(String type, Output<T> in1, Output<T> in2) {
            return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
        }

        private <T, U, V> Output<T> binaryOp3(String type, Output<U> in1, Output<V> in2) {
            return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
        }

        private Graph g;
    }


}
