/*
 * Copyright 2020-present the original author or authors.
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

import java.util.concurrent.Callable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Wander Costa
 */
class ConnectionFactoryContextWrapperTests {

	@Test
	@DisplayName("Test execution of callable in context wrapper")
	void testExecuteCallable() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		ConnectionFactoryContextWrapper wrapper = new ConnectionFactoryContextWrapper(connectionFactory);

		String dummyContext = "dummy-context";
		String expectedResult = "dummy-result";
		Callable<String> callable = () -> expectedResult;

		String actualResult = wrapper.call(dummyContext, callable);
		assertThat(expectedResult).isEqualTo(actualResult);
	}

	@Test
	@DisplayName("Test exception of runnable in context wrapper")
	void testExecuteRunnable() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		ConnectionFactoryContextWrapper wrapper = new ConnectionFactoryContextWrapper(connectionFactory);

		String dummyContext = "dummy-context";
		Runnable runnable = mock(Runnable.class);

		wrapper.run(dummyContext, runnable);
		verify(runnable).run();
	}

	@Test
	@DisplayName("Test exception interception with runnable in context wrapper")
	void testExecuteCallableThrowsRuntimeException() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		ConnectionFactoryContextWrapper wrapper = new ConnectionFactoryContextWrapper(connectionFactory);

		String dummyContext = "dummy-context";
		Callable<String> callable = () -> {
			throw new RuntimeException("dummy-exception");
		};

		Assertions.assertThrows(RuntimeException.class, () -> wrapper.call(dummyContext, callable));
	}

}
