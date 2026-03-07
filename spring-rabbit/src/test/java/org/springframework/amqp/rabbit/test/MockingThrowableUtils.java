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

package org.springframework.amqp.rabbit.test;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Convenience methods for mocking {@link Throwable}.
 * <p>
 * It is useful when one needs to create an instance of {@link Throwable} with no public constructors or factories.
 * In other cases it is recommended to create a real object because {@link Throwable}'s may be processed
 * by logger in unexpected ways.
 * </p>
 *
 * @author Alexei Sischin
 * @since 4.1.0
 */
public final class MockingThrowableUtils {

	private MockingThrowableUtils() {
	}

	public static <T extends Throwable> T mockThrowable(Class<T> clazz) {
		return mockThrowable(clazz, null);
	}

	public static <T extends Throwable> T mockThrowable(Class<T> clazz, String message) {
		var exception = mock(clazz);
		given(exception.getMessage()).willReturn(message);
		given(exception.getLocalizedMessage()).willReturn(message);
		given(exception.getStackTrace()).willReturn(Thread.currentThread().getStackTrace());
		given(exception.getSuppressed()).willReturn(new Throwable[0]);
		return exception;
	}
}
