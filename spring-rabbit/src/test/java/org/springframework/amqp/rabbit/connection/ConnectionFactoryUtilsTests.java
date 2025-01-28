/*
 * Copyright 2017-2025 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 1.7.1
 *
 */
public class ConnectionFactoryUtilsTests {

	@Test
	public void testResourceHolder() {
		RabbitResourceHolder h1 = new RabbitResourceHolder();
		RabbitResourceHolder h2 = new RabbitResourceHolder();
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		TransactionSynchronizationManager.setActualTransactionActive(true);
		ConnectionFactoryUtils.bindResourceToTransaction(h1, connectionFactory, true);
		assertThat(ConnectionFactoryUtils.bindResourceToTransaction(h2, connectionFactory, true)).isSameAs(h1);
		TransactionSynchronizationManager.clear();
	}

}
