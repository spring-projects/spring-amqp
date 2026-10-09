/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A post processor that uses a {@link GZIPOutputStream} to compress the message body.
 * Sets {@link org.springframework.amqp.core.MessageProperties#SPRING_AUTO_DECOMPRESS} to
 * true by default.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class GZipPostProcessor extends AbstractDeflaterPostProcessor {

	public GZipPostProcessor() {
	}

	public GZipPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	@Override
	protected OutputStream getCompressorStream(OutputStream zipped) throws IOException {
		return new SettableLevelGZIPOutputStream(zipped, getLevel());
	}

	@Override
	protected String getEncoding() {
		return "gzip";
	}

	private static final class SettableLevelGZIPOutputStream extends GZIPOutputStream {

		SettableLevelGZIPOutputStream(OutputStream out, int level) throws IOException {
			super(out);
			this.def.setLevel(level);
		}

	}

}
