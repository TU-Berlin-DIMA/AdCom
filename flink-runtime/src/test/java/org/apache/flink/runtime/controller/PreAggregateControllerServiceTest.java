package org.apache.flink.runtime.controller;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PreAggregateControllerServiceTest extends TestLogger {

	@Test
	public void testPreAggControllerExtractIp() throws Exception {
		PreAggregateControllerService preAggregateControllerService = new PreAggregateControllerService();

		String ip0 = preAggregateControllerService.extractIP(
			"akka.tcp://flink@192.168.56.1:6123/user/rpc/jobmanager_2");
		assertEquals("192.168.56.1", ip0);

		String ip1 = preAggregateControllerService.extractIP(
			"akka.tcp://flink@127.0.0.1:6123/user/rpc/jobmanager_2");
		assertEquals("127.0.0.1", ip1);

		String ip2 = preAggregateControllerService.extractIP(
			"akka.tcp://flink@localhost:6123/user/rpc/jobmanager_2");
		assertEquals("127.0.0.1", ip2);

		String ip3 = preAggregateControllerService.extractIP(
			"akka.tcp://flink@flink:6123/user/rpc/jobmanager_2");
		assertEquals("127.0.0.1", ip3);
	}
}
