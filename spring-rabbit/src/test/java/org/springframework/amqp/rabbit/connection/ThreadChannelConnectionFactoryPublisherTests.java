/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ThreadChannelConnectionFactory} publisher connection factory
 * configuration. These require no broker.
 *
 * @author Kumar Gaurav
 *
 * @since 4.0.6
 */
class ThreadChannelConnectionFactoryPublisherTests {

	@Test
	void simplePublisherConfirmsPropagateToDefaultPublisherFactory() {
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(new ConnectionFactory());

		tccf.setSimplePublisherConfirms(true);

		assertThat(tccf.isSimplePublisherConfirms()).isTrue();
		org.springframework.amqp.rabbit.connection.ConnectionFactory publisher =
				tccf.getPublisherConnectionFactory();
		assertThat(publisher).isNotNull();
		assertThat(((ThreadChannelConnectionFactory) publisher).isSimplePublisherConfirms())
				.as("simplePublisherConfirms must reach the default publisher sub-factory")
				.isTrue();
	}

}
