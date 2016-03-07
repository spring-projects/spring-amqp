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

package org.springframework.amqp.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

/**
 * @author Gary Russell
 * @since 1.4.2
 *
 */
public class MessagePostProcessorUtilsTests {

	@Test
	public void testOrderIng() {
		MPP[] pps = new MPP[] {
				new MPP(),
				new OMPP().order(3),
				new OMPP().order(1),
				new POMPP().order(6),
				new POMPP().order(2)
			};
		Collection<MessagePostProcessor> sorted = MessagePostProcessorUtils.sort(Arrays.<MessagePostProcessor>asList(pps));
		Iterator<MessagePostProcessor> iterator = sorted.iterator();
		MessagePostProcessor mpp = iterator.next();
		assertThat(mpp, Matchers.instanceOf(POMPP.class));
		assertEquals(2, ((POMPP) mpp).getOrder());
		mpp = iterator.next();
		assertThat(mpp, Matchers.instanceOf(POMPP.class));
		assertEquals(6, ((POMPP) mpp).getOrder());mpp = iterator.next();
		assertThat(mpp, Matchers.instanceOf(OMPP.class));
		assertEquals(1, ((OMPP) mpp).getOrder());mpp = iterator.next();
		assertThat(mpp, Matchers.instanceOf(OMPP.class));
		assertEquals(3, ((OMPP) mpp).getOrder());mpp = iterator.next();
		assertThat(mpp, Matchers.instanceOf(MPP.class));
	}

	class MPP implements MessagePostProcessor {

		@Override
		public Message postProcessMessage(Message message) throws AmqpException {
			return null;
		}

	}

	class OMPP extends MPP implements Ordered {

		private int order;

		@Override
		public int getOrder() {
			return this.order;
		}

		public OMPP order(int order) {
			this.order = order;
			return this;
		}

	}

	class POMPP extends OMPP implements PriorityOrdered {

	}

}
