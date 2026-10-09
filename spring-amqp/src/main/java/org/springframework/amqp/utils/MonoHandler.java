/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.utils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * Class to prevent direct links to {@link Mono}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Arnab Nandy
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

	/**
	 * Convert a {@link Mono} to a {@link CompletableFuture}.
	 * @param returnValue the return value (expected to be a Mono)
	 * @return the CompletableFuture
	 * @since 4.2
	 */
	public static CompletableFuture<?> toCompletableFuture(Object returnValue) {
		return ((Mono<?>) returnValue).toFuture();
	}

}
