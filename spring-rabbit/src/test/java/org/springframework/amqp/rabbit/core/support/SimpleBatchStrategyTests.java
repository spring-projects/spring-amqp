/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core.support;

import java.nio.ByteBuffer;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.util.StopWatch;

/**
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class SimpleBatchStrategyTests {

	@Test
	@Ignore
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
