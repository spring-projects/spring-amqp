/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * @author Wander Costa
 */
class MultiRabbitAdminNameResolverTests {

	private static final String DUMMY_ADMIN = "dummy-admin";
	private static final String DUMMY_CONTAINER_FACTORY = "dummy-container-factory";


	@Test
	@DisplayName("Should resolve admin bean name from listener")
	void testResolveFromAdmin() {
		RabbitListener listener = mock(RabbitListener.class);

		when(listener.admin()).thenReturn(DUMMY_ADMIN);
		assertEquals(DUMMY_ADMIN, MultiRabbitAdminNameResolver.resolve(listener));

		verify(listener).admin();
		verify(listener, never()).containerFactory();
	}


	@Test
	@DisplayName("Should resolve admin bean name from container factory when no admin is available")
	void testResolveFromContainerFactoryWhenNoAdminIsAvailable() {
		RabbitListener listener = mock(RabbitListener.class);

		when(listener.containerFactory()).thenReturn(DUMMY_CONTAINER_FACTORY);
		String expected = DUMMY_CONTAINER_FACTORY + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX;
		assertEquals(expected, MultiRabbitAdminNameResolver.resolve(listener));

		verify(listener).admin();
		verify(listener, times(2)).containerFactory();
	}


	@Test
	@DisplayName("Should resolve falback to default value")
	void testResolveFallbackToDefault() {
		RabbitListener listener = mock(RabbitListener.class);

		assertEquals(MultiRabbitConstants.DEFAULT_RABBIT_ADMIN_BEAN_NAME, MultiRabbitAdminNameResolver.resolve(listener));

		verify(listener).admin();
		verify(listener).containerFactory();
	}

}
