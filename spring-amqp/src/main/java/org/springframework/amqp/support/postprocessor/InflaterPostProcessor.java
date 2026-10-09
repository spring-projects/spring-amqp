/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * A post processor that uses a {@link InflaterInputStream} to decompress the
 * message body.
 *
 * @author David Diehl
 * @since 2.2
 */
public class InflaterPostProcessor extends AbstractDecompressingPostProcessor {

	public InflaterPostProcessor() {
	}

	public InflaterPostProcessor(boolean alwaysDecompress) {
		super(alwaysDecompress);
	}

	@Override
	protected InputStream getDecompressorStream(InputStream zipped) {
		return new InflaterInputStream(zipped);
	}

	@Override
	protected String getEncoding() {
		return "deflate";
	}

}
