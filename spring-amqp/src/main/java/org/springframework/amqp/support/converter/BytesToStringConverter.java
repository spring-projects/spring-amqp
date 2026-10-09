/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import java.nio.charset.Charset;

import org.springframework.core.convert.converter.Converter;

/**
 * The {@link Converter} implementation to convert {@code byte[]} to {@link String} using the provided {@link Charset}.
 *
 * @param charset the charset to use.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public record BytesToStringConverter(Charset charset) implements Converter<byte[], String> {

	@Override
	public String convert(byte[] source) {
		return new String(source, this.charset);
	}

}
