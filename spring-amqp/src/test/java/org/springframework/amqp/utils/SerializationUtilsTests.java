/*
 * Copyright 2026 Broadcom Inc. and/or its subsidiaries. All Rights Reserved.
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Artem Bilan
 *
 * @since 2.4.19
 */
public class SerializationUtilsTests {

	// non-empty and non-matching, so the TRUST_ALL env var (set for the test JVM) doesn't short-circuit the check
	private static final Set<String> NON_MATCHING_PATTERNS = Collections.singleton("does.not.Match");

	@Test
	void arrayOfPrimitiveOrAllowedType() {
		SerializationUtils.checkAllowedList(int[].class, NON_MATCHING_PATTERNS);
		SerializationUtils.checkAllowedList(int[][].class, NON_MATCHING_PATTERNS);
		SerializationUtils.checkAllowedList(String[].class, NON_MATCHING_PATTERNS);
		SerializationUtils.checkAllowedList(Integer[].class, NON_MATCHING_PATTERNS);
	}

	@Test
	void arrayOfDisallowedTypeIsRejected() {
		assertThatExceptionOfType(SecurityException.class)
				.isThrownBy(() -> SerializationUtils.checkAllowedList(Object[].class, NON_MATCHING_PATTERNS));
		assertThatExceptionOfType(SecurityException.class)
				.isThrownBy(() -> SerializationUtils.checkAllowedList(TestBean[].class, NON_MATCHING_PATTERNS));
		assertThatExceptionOfType(SecurityException.class)
				.isThrownBy(() -> SerializationUtils.checkAllowedList(TestBean[][].class, NON_MATCHING_PATTERNS));
	}

	@Test
	void arrayOfExplicitlyAllowedTypeIsAllowed() {
		Set<String> patterns = Collections.singleton("*$TestBean");
		SerializationUtils.checkAllowedList(TestBean.class, patterns);
		SerializationUtils.checkAllowedList(TestBean[].class, patterns);
		SerializationUtils.checkAllowedList(TestBean[][].class, patterns);
	}

	@SuppressWarnings("serial")
	static final class TestBean implements Serializable {

	}

}
