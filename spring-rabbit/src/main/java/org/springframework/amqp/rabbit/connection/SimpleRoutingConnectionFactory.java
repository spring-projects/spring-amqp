/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import org.jspecify.annotations.Nullable;

/**
 * An {@link AbstractRoutingConnectionFactory} implementation which gets a {@code lookupKey}
 * for current {@link ConnectionFactory} from thread-bound resource by key of the instance of
 * this {@link ConnectionFactory}.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.3
 */
public class SimpleRoutingConnectionFactory extends AbstractRoutingConnectionFactory {

	@Override
	protected @Nullable Object determineCurrentLookupKey() {
		return SimpleResourceHolder.get(this);
	}

}
