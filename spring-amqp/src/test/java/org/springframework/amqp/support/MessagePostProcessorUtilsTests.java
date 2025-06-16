/*
 * Copyright 2014-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.support;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(mpp).isInstanceOf(POMPP.class);
		assertThat(((POMPP) mpp).getOrder()).isEqualTo(2);
		mpp = iterator.next();
		assertThat(mpp).isInstanceOf(POMPP.class);
		assertThat(((POMPP) mpp).getOrder()).isEqualTo(6);
		mpp = iterator.next();
		assertThat(mpp).isInstanceOf(OMPP.class);
		assertThat(((OMPP) mpp).getOrder()).isEqualTo(1);
		mpp = iterator.next();
		assertThat(mpp).isInstanceOf(OMPP.class);
		assertThat(((OMPP) mpp).getOrder()).isEqualTo(3);
		mpp = iterator.next();
		assertThat(mpp).isInstanceOf(MPP.class);
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
