package org.apache.flink.streaming.examples.aggregate.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Random;

/**
 * <pre>
 *     java -classpath /home/flink/flink-1.9.0-partition/lib/flink-dist_2.11-1.9.0.jar:MqttDataProducer.jar org.apache.flink.streaming.examples.utils.MqttDataProducer \
 *     -input [hamlet|mobydick|dictionary|existing_file|sensorData] \
 *     -host [127.0.0.1] \
 *     -port [1883] \
 *     -maxCount [Long.MAX_VALUE] \
 *     -qtdSensors [50] \
 *     -qtdMetrics [200] \
 *     -delay [1000]
 *
 * 	   Description of the parameters
 *     -qtdSensors: number of sensors that is generating values. In order to produce a skew workload set this value to 1
 *     and it will generate different values with respect to the -qtdMetrics parameter.
 *     -qtdMetrics: number of metrics generated per sensor
 *     -delay: time in milliseconds that defines the frequency to produce data
 *
 *
 * Consume data from this producer:
 *     mosquitto_sub -h 127.0.0.1 -p 1883 -t topic-data-source
 *
 * Changes the data frequency that this producer emits to the broker:
 *     mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m "1000"
 * </pre>
 */
public class MqttDataProducer extends Thread {
	private static final String TOPIC_DATA_SOURCE = "topic-data-source";
	private static final String TOPIC_FREQUENCY_DATA_SOURCE = "topic-frequency-data-source";
	private static final String SOURCE_DATA_HAMLET = "hamlet";
	private static final String SOURCE_DATA_MOBY_DICK = "mobydick";
	private static final String SOURCE_DATA_DICTIONARY = "dictionary";
	private static final String SOURCE_SENSOR_DATA = "sensorData";
	private static final String SOURCE = "input";
	private static final String HOST = "host";
	private static final String PORT = "port";
	private static final String MAX_COUNT = "maxCount";
	private static final String QTD_SENSORS = "qtdSensors";
	private static final String QTD_METRICS = "qtdMetrics";
	private static final String DELAY = "delay";

	private final Random random;
	private final double sensorRangeMin;
	private final double sensorRangeMax;
	private final String host;
	private final int port;
	private final int numberOfSensors;
	private final int qtdOfMetrics;
	private final long maxCount;
	private final String topicToPublish;
	private final String topicFrequencyParameter;
	private final MqttDataType mqttDataType;
	private FutureConnection connection;
	private CallbackConnection connectionSideParameter;
	private URL url;
	private MQTT mqtt;
	private int delay;
	private long count;
	private boolean running = false;
	private boolean offlineData;
	private String resource;

	public MqttDataProducer(MqttDataType mqttDataType, String resource, String host, int port, long maxCount) throws MalformedURLException {
		this(mqttDataType, resource, host, port, maxCount, 50, 200, 10000);
	}

	public MqttDataProducer(MqttDataType mqttDataType, String resource, String host, int port, long maxCount,
							int numberOfSensors, int qtdOfMetrics, int delay) throws MalformedURLException {
		this.random = new Random();
		this.sensorRangeMin = -20.0;
		this.sensorRangeMax = 60.0;
		this.mqttDataType = mqttDataType;
		this.host = host;
		this.port = port;
		this.delay = delay;
		this.running = true;
		this.numberOfSensors = numberOfSensors;
		this.qtdOfMetrics = qtdOfMetrics;

		this.maxCount = maxCount;
		this.topicToPublish = TOPIC_DATA_SOURCE;
		this.topicFrequencyParameter = TOPIC_FREQUENCY_DATA_SOURCE;

		if (MqttDataType.HAMLET == this.mqttDataType) {
			this.url = new URL("http://www.gutenberg.org/files/1524/1524-0.txt");
			this.offlineData = false;
		} else if (MqttDataType.MOBY_DICK == this.mqttDataType) {
			this.url = new URL("http://www.gutenberg.org/files/2701/2701-0.txt");
			this.offlineData = false;
		} else if (MqttDataType.DICTIONARY == this.mqttDataType) {
			this.url = new URL("https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt");
			this.offlineData = false;
		} else if (MqttDataType.SOURCE_SENSOR_DATA == this.mqttDataType) {
			this.offlineData = true;
			this.resource = resource;
		} else if (MqttDataType.OFFLINE_FILE == this.mqttDataType) {
			this.offlineData = true;
			this.resource = resource;
		}
		this.disclaimer();
	}

	public static void main(String[] args) throws Exception {
		MqttDataProducer producer = null;
		final ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.get(SOURCE, "");
		String host = params.get(HOST, "127.0.0.1");
		int port = params.getInt(PORT, 1883);
		long maxCount = params.getLong(MAX_COUNT, Long.MAX_VALUE);
		int qtdSensors = params.getInt(QTD_SENSORS, 50);
		int qtdMetrics = params.getInt(QTD_METRICS, 200);
		int delay = params.getInt(DELAY, 1000);

		if (SOURCE_DATA_HAMLET.equalsIgnoreCase(input)) {
			producer = new MqttDataProducer(MqttDataType.HAMLET, "", host, port, maxCount);
		} else if (SOURCE_DATA_MOBY_DICK.equalsIgnoreCase(input)) {
			producer = new MqttDataProducer(MqttDataType.MOBY_DICK, "", host, port, maxCount);
		} else if (SOURCE_DATA_DICTIONARY.equalsIgnoreCase(input)) {
			producer = new MqttDataProducer(MqttDataType.DICTIONARY, "", host, port, maxCount);
		} else if (SOURCE_SENSOR_DATA.equalsIgnoreCase(input)) {
			producer = new MqttDataProducer(MqttDataType.SOURCE_SENSOR_DATA, "", host, port, maxCount, qtdSensors, qtdMetrics, delay);
		} else if (!Strings.isNullOrEmpty(input)) {
			producer = new MqttDataProducer(MqttDataType.OFFLINE_FILE, input, host, port, maxCount);
		} else if (Strings.isNullOrEmpty(input)) {
			throw new Exception("Please use some input data source available: -input [hamlet|mobydick|dictionary|existing_file]");
		}

		producer.connect();
		producer.start();
		producer.publish();
		// producer.disconnect();
	}

	public void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	public void disconnect() throws Exception {
		connection.disconnect().await();
	}

	public void run() {
		connectionSideParameter = mqtt.callbackConnection();
		connectionSideParameter.listener(new org.fusesource.mqtt.client.Listener() {
			public void onConnected() {
			}

			public void onDisconnected() {
			}

			public void onFailure(Throwable value) {
				value.printStackTrace();
				System.exit(-2);
			}

			public void onPublish(UTF8Buffer topic, Buffer msg, Runnable ack) {
				String body = msg.utf8().toString();

				if (isInteger(body)) {
					System.out.println("Reading new frequency to emit data to the MQTT broker: " + body + " milliseconds.");
					delay = Integer.parseInt(body);
				} else if ("SHUTDOWN".equalsIgnoreCase(body)) {
					running = false;
				} else {
					System.out.println(body);
				}
				ack.run();
			}
		});
		connectionSideParameter.connect(new Callback<Void>() {
			@Override
			public void onSuccess(Void value) {
				Topic[] topics = {new Topic(topicFrequencyParameter, QoS.AT_LEAST_ONCE)};
				connectionSideParameter.subscribe(topics, new Callback<byte[]>() {
					public void onSuccess(byte[] qoses) {
					}

					public void onFailure(Throwable value) {
						value.printStackTrace();
						System.exit(-2);
					}
				});
			}

			@Override
			public void onFailure(Throwable value) {
				value.printStackTrace();
				System.exit(-2);
			}
		});
	}

	public void publish() throws Exception {
		int size = 256;

		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(topicToPublish);

		while (running) {
			Buffer msg = new AsciiBuffer(readDataFromResource());

			// Send the publish without waiting for it to complete. This allows us
			// to send multiple message without blocking..
			queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));

			// Eventually we start waiting for old publish futures to complete
			// so that we don't create a large in memory buffer of outgoing message.s
			if (queue.size() >= 1000) {
				queue.removeFirst().await();
			}
			// This is the delay to publish items on the MQTT broker
			Thread.sleep(delay);
			this.checkEndOfStream();
		}

		queue.add(connection.publish(topic, new AsciiBuffer("SHUTDOWN"), QoS.AT_LEAST_ONCE, false));
		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	private String readDataFromResource() throws Exception {
		// get the data source file to collect data
		InputStream in = null;
		if (this.offlineData) {
			in = getDataSourceInputStream();
		} else {
			in = url.openStream();
		}
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
		StringBuilder builder = new StringBuilder();
		String line;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				builder.append(line + "\n");
			}
			bufferedReader.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return builder.toString();
	}

	private InputStream getDataSourceInputStream() throws Exception {
		if (MqttDataType.OFFLINE_FILE == this.mqttDataType) {
			return new FileInputStream(new File(this.resource));
		} else if (MqttDataType.SOURCE_SENSOR_DATA == this.mqttDataType) {
			String value = "";
			String result = "";
			if (numberOfSensors == 1) {
				// create a skew workload based only in one sensorID
				int sensorId = 1;
				for (int i = 0; i < qtdOfMetrics; i++) {
					double sensorValue = sensorRangeMin + (sensorRangeMax - sensorRangeMin) * random.nextDouble();
					value = value + sensorId + ";" + sensorValue + "|";
				}
				result = value;
			} else {
				// create a workload based on a normal distribution
				for (int sensorId = 1; sensorId <= numberOfSensors; sensorId++) {
					double sensorValue = sensorRangeMin + (sensorRangeMax - sensorRangeMin) * random.nextDouble();
					value = value + sensorId + ";" + sensorValue + "|";
				}
				for (int i = 0; i < qtdOfMetrics; i++) {
					result = result + value;
				}
			}
			return new ByteArrayInputStream(StandardCharsets.UTF_8.encode(result).array());
		} else {
			throw new Exception("DataSourceType is NULL!");
		}
	}

	private void checkEndOfStream() {
		if (this.maxCount != Long.MAX_VALUE) {
			this.count++;
			if (this.count >= this.maxCount) {
				this.running = false;
			}
		}
	}

	private void disclaimer() {
		// @formatter:off
		System.out.println("This is the application [" + MqttDataProducer.class.getSimpleName() + "].");
		System.out.println("Download data from:");
		System.out.println("HAMLET: wget http://www.gutenberg.org/files/1524/1524-0.txt");
		System.out.println("MOBYDICK: wget http://www.gutenberg.org/files/2701/2701-0.txt");
		System.out.println("DICTIONARY: wget https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt");
		System.out.println("It aims to collect online or offline data and publish in an MQTT broker.");
		System.out.println("To consume data on the terminal use:");
		System.out.println("mosquitto_sub -h " + host + " -p " + port + " -t " + topicToPublish);
		System.out.println("To change the frequency of emission use:");
		System.out.println("mosquitto_pub -h " + host + " -p " + port + " -t " + topicFrequencyParameter + " -m \"miliseconds\"");
		System.out.println("delay [milliseconds]: " + this.delay);
		System.out.println("number of sensors: " + this.numberOfSensors);
		System.out.println("quantity of metrics: " + this.qtdOfMetrics);
		System.out.println("number of records every " + this.delay + " milliseconds: " + (this.numberOfSensors * this.qtdOfMetrics));
		System.out.println();
		// @formatter:on
	}

	public boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	public boolean isInteger(String s, int radix) {
		if (s.isEmpty())
			return false;
		for (int i = 0; i < s.length(); i++) {
			if (i == 0 && s.charAt(i) == '-') {
				if (s.length() == 1)
					return false;
				else
					continue;
			}
			if (Character.digit(s.charAt(i), radix) < 0)
				return false;
		}
		return true;
	}
}
