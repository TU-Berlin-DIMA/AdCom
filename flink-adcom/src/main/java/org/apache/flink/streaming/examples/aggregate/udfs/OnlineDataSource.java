package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.examples.aggregate.util.UrlSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;


public class OnlineDataSource extends RichSourceFunction<String> {
	private static final String DISTINCT_WORDS_URL = "https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt";
	private static final String HAMLET_URL = "http://www.gutenberg.org/files/1524/1524-0.txt";
	private static final String MOBY_DICK_URL = "http://www.gutenberg.org/files/2701/2701-0.txt";
	private volatile boolean running = true;
	private String[] words;
	private long milliseconds;

	public OnlineDataSource() throws Exception {
		this(UrlSource.HAMLET, 1);
	}

	public OnlineDataSource(UrlSource urlSource) throws Exception {
		this(urlSource, 1);
	}

	public OnlineDataSource(UrlSource urlSource, long milliseconds) throws Exception {
		StringBuffer stringBuffer = null;
		if (urlSource == UrlSource.ENGLISH_DICTIONARY) {
			stringBuffer = new StringBuffer(readDataFromResource(DISTINCT_WORDS_URL));
		} else if (urlSource == UrlSource.HAMLET) {
			stringBuffer = new StringBuffer(readDataFromResource(HAMLET_URL));
		} else if (urlSource == UrlSource.MOBY_DICK) {
			stringBuffer = new StringBuffer(readDataFromResource(MOBY_DICK_URL));
		}
		this.words = stringBuffer.toString().split("\\W+");
		this.milliseconds = milliseconds;
	}

	public static void main(String[] args) throws Exception {
		OnlineDataSource zipfDistributionDataSource = new OnlineDataSource();
		// StringBuffer stringBuffer = new StringBuffer(zipfDistributionDataSource.readDataFromResource(DISTINCT_WORDS_URL));
		// String[] words = stringBuffer.toString().split("\n");
		System.out.println("size: " + zipfDistributionDataSource.words.length);

		System.out.println("Normal Distribution");
		NormalDistribution normalDistribution = new NormalDistribution(zipfDistributionDataSource.words.length / 2, 3);
		for (int i = 0; i < 10; i++) {
			int sample = (int) normalDistribution.sample();
			System.out.print("sample[" + sample + "]: ");
			System.out.println(zipfDistributionDataSource.words[sample]);
		}

		System.out.println();
		System.out.println("Zipf Distribution");
		ZipfDistribution zipfDistribution = new ZipfDistribution(zipfDistributionDataSource.words.length - 1, 3);
		for (int i = 0; i < 10; i++) {
			int sample = zipfDistribution.sample();
			System.out.print("sample[" + sample + "]: ");
			System.out.println(zipfDistributionDataSource.words[sample]);
		}
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			for (int i = 0; i < words.length; i++) {
				ctx.collect(words[i]);
				Thread.sleep(milliseconds);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	private String readDataFromResource(String urlSource) throws Exception {
		URL url = new URL(urlSource);
		InputStream in = url.openStream();
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
}
