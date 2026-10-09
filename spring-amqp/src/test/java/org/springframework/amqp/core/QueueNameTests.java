/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 1.5.3
 *
 */
public class QueueNameTests {

	@Test
	public void testAnonymous() {
		AnonymousQueue q = new AnonymousQueue();
		assertThat(q.getName()).startsWith("spring.gen-");
		q = new AnonymousQueue(new Base64UrlNamingStrategy("foo-"));
		assertThat(q.getName()).startsWith("foo-");
		q = new AnonymousQueue(UUIDNamingStrategy.DEFAULT);
		assertThat(Pattern.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", q.getName())).as("Not a UUID: " + q.getName()).isTrue();
	}

}
