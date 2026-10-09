/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core.support;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.util.StopWatch;

/**
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class SimpleBatchStrategyTests {

	@Test
	@Disabled
	public void testBatchingPerf() { // used to compare ByteBuffer Vs. System.arrayCopy()
		StopWatch watch = new StopWatch();
		byte[] bbBuff = new byte[10000];
		ByteBuffer bb = ByteBuffer.wrap(bbBuff);
		byte[] buff = new byte[10000];
		watch.start();
		for (int i = 0; i < 10000000; i++) {
			bb.position(0);
			bb.put(buff);
//			System.arraycopy(buff, 0, bbBuff, 0, 10000);
		}
		watch.stop();
//		System .out .println(watch.getTotalTimeMillis());
	}

}
