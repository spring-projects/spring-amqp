/*
 * Copyright 2002-2022 the original author or authors.
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
import org.mockito.Mockito;

/**
 * @author Leonardo Ferreira
 */
public class DelegatingConnectionFactoryTests {

	@Test
	void shouldDelegateTest() {
		final ConnectionFactory target = Mockito.mock(ConnectionFactory.class);

		final DelegatingConnectionFactory delegating = new DelegatingConnectionFactory(target);

		delegating.createConnection();
		Mockito.verify(target).createConnection();

		delegating.getHost();
		Mockito.verify(target).getHost();

		delegating.getPort();
		Mockito.verify(target).getPort();

		delegating.getVirtualHost();
		Mockito.verify(target).getVirtualHost();

		delegating.getUsername();
		Mockito.verify(target).getUsername();

		final ConnectionListener connectionListenerToAdd = Mockito.mock(ConnectionListener.class);
		delegating.addConnectionListener(connectionListenerToAdd);
		Mockito.verify(target).addConnectionListener(Mockito.eq(connectionListenerToAdd));

		final ConnectionListener connectionListenerToRemove = Mockito.mock(ConnectionListener.class);
		delegating.removeConnectionListener(connectionListenerToRemove);
		Mockito.verify(target).removeConnectionListener(Mockito.eq(connectionListenerToRemove));

		delegating.clearConnectionListeners();
		Mockito.verify(target).clearConnectionListeners();

		delegating.getPublisherConnectionFactory();
		Mockito.verify(target).getPublisherConnectionFactory();

		delegating.isSimplePublisherConfirms();
		Mockito.verify(target).isSimplePublisherConfirms();

		delegating.isPublisherConfirms();
		Mockito.verify(target).isPublisherConfirms();

		delegating.isPublisherReturns();
		Mockito.verify(target).isPublisherReturns();
	}

}
