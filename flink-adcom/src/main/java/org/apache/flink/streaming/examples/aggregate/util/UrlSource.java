package org.apache.flink.streaming.examples.aggregate.util;

/**
 * The English dictionary will have only distinct words which does not follow a Zipf distribution, but an even distribution.
 * The Mody Dick book follows the Zipf distribution (http://www.mfumagalli.com/wp/2015/10/14/moby-dick-the-and-zipfs-law/).
 */
public enum UrlSource {
	ENGLISH_DICTIONARY, HAMLET, MOBY_DICK
}
