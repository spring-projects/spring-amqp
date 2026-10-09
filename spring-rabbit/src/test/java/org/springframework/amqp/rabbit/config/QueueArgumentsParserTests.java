/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Gary Russell
 * @since 1.0.1
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class QueueArgumentsParserTests {

	@Autowired
	private ApplicationContext ctx;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Test
	public void test() {
		@SuppressWarnings("unchecked")
		Map<String, Object> args = (Map<String, Object>) ctx.getBean("args");
		assertThat(args.get("foo")).isEqualTo("bar");

		assertThat(queue1.getArguments().get("baz")).isEqualTo("qux");
		assertThat(queue2.getArguments().get("foo")).isEqualTo("bar");
	}

}
