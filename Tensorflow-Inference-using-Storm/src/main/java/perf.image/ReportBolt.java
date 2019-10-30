package perf.image;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportBolt extends BaseRichBolt {

	private static final Logger logger = LoggerFactory.getLogger(ReportBolt.class);
	OutputCollector collector;
	byte[] imageBytes;
	private long lead_time;
	private long rcv_number_of_tuples;
	private long total_size;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
//		this.lead_time = 0L;
		this.rcv_number_of_tuples = 0L;
		this.total_size = 0L;
		
	}

	public void execute(Tuple input) {
		imageBytes = input.getBinaryByField("img");
		String label = (String) input.getValueByField("label");
		float prob = input.getFloatByField("probability");
//		this.lead_time += (System.currentTimeMillis() - input.getLongByField("start-time"));
		this.rcv_number_of_tuples += 1L;	
		this.total_size += input.getBinaryByField("img").length; 
		System.out.println("Imgae(byte): " + imageBytes + " 's Predict Result: " + label + " (" + prob + "%)");		
		collector.emit(new Values("Imgae(byte): " + imageBytes + " 's Predict Result: " + label + " (" + prob + "%)"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
	
	
	public void cleanup() {
		logger.info("\n\n\n\n Bolt cleanup, total_rcv_number_of_tuples: " + this.rcv_number_of_tuples + " // total_msg_sizes: " + this.total_size + "  // avg_latency: " + (this.lead_time / this.rcv_number_of_tuples)
				+ "\n\n\n");
	}

}