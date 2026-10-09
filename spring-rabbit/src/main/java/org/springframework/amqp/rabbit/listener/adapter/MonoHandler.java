/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * Class to prevent direct links to {@link Mono}.
 *
 * @author Gary Russell
 *
 * @since 2.2.21
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.utils.MonoHandler}.
 */
@Deprecated(since = "4.1", forRemoval = true)
final class MonoHandler { // NOSONAR - pointless to name it ..Utils|Helper

	private MonoHandler() {
	}

	static boolean isMono(@Nullable Object result) {
		return result instanceof Mono;
	}

	static boolean isMono(Class<?> resultType) {
		return Mono.class.isAssignableFrom(resultType);
	}

	@SuppressWarnings("unchecked")
	static void subscribe(Object returnValue, @Nullable Consumer<? super Object> success,
			Consumer<? super Throwable> failure, Runnable completeConsumer) {

		((Mono<? super Object>) returnValue).subscribe(success, failure, completeConsumer);
	}

}
