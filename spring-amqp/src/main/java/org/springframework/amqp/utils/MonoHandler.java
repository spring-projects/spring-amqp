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

package org.springframework.amqp.utils;

import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * Class to prevent direct links to {@link Mono}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 */
public final class MonoHandler {

	private MonoHandler() {
	}

	public static boolean isMono(@Nullable Object result) {
		return result instanceof Mono;
	}

	public static boolean isMono(Class<?> resultType) {
		return Mono.class.isAssignableFrom(resultType);
	}

	@SuppressWarnings("unchecked")
	public static void subscribe(Object returnValue, @Nullable Consumer<? super Object> success,
			Consumer<? super Throwable> failure, Runnable completeConsumer) {

		((Mono<? super Object>) returnValue).subscribe(success, failure, completeConsumer);
	}

}
