/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * A post processor that uses a {@link GZIPInputStream} to decompress the
 * message body.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class GUnzipPostProcessor extends AbstractDecompressingPostProcessor {

	public GUnzipPostProcessor() {
	}

	public GUnzipPostProcessor(boolean alwaysDecompress) {
		super(alwaysDecompress);
	}

	@Override
	protected InputStream getDecompressorStream(InputStream zipped) throws IOException {
		return new GZIPInputStream(zipped);
	}

	@Override
	protected String getEncoding() {
		return "gzip";
	}

}
