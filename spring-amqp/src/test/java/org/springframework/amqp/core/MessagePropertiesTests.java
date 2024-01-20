/*
 * Copyright 2002-2024 the original author or authors.
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

package org.springframework.amqp.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;


/**
 * @author Dave Syer
 * @author Artem Yakshin
 * @author Artem Bilan
 * @author Gary Russell
 * @author Csaba Soti
 * @author Raylax Grey
 *
 */
public class MessagePropertiesTests {



	@Test
	public void testReplyTo() {
		MessageProperties properties = new MessageProperties();
		properties.setReplyTo("foo/bar");
		assertThat(properties.getReplyToAddress().getRoutingKey()).isEqualTo("bar");
	}

	@Test
	public void testReplyToNullByDefault() {
		MessageProperties properties = new MessageProperties();
		assertThat(properties.getReplyTo()).isEqualTo(null);
		assertThat(properties.getReplyToAddress()).isEqualTo(null);
	}

	@Test
	public void testDelayHeader() {
		MessageProperties properties = new MessageProperties();
		Long delay = 100L;
		properties.setDelayLong(delay);
		assertThat(properties.getHeaders().get(MessageProperties.X_DELAY)).isEqualTo(delay);
		properties.setDelayLong(null);
		assertThat(properties.getHeaders().containsKey(MessageProperties.X_DELAY)).isFalse();
	}

	@Test
	public void testContentLengthSet() {
		MessageProperties properties = new MessageProperties();
		properties.setContentLength(1L);
		assertThat(properties.isContentLengthSet()).isTrue();
	}

	@Test
	public void tesNoNullPointerInEquals() {
		MessageProperties mp = new MessageProperties();
		MessageProperties mp2 = new MessageProperties();
		assertThat(mp.equals(mp2)).isTrue();
	}

	 @Test
	  public void tesNoNullPointerInHashCode() {
	    Set<MessageProperties> messageList = new HashSet<>();
	    messageList.add(new MessageProperties());
	    assertThat(messageList).hasSize(1);
	  }

}
