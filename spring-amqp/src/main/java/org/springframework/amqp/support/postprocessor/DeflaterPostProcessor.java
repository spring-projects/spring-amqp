/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

/**
 * A post processor that uses a {@link DeflaterOutputStream} to compress the message body.
 * Sets {@link org.springframework.amqp.core.MessageProperties#SPRING_AUTO_DECOMPRESS} to
 * true by default.
 *
 * @author David Diehl
 * @since 2.2
 */
public class DeflaterPostProcessor extends AbstractDeflaterPostProcessor {

	public DeflaterPostProcessor() {
	}

	public DeflaterPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	@Override
	protected OutputStream getCompressorStream(OutputStream zipped) {
		return new DeflaterPostProcessor.SettableLevelDeflaterOutputStream(zipped, getLevel());
	}

	@Override
	protected String getEncoding() {
		return "deflate";
	}

	private static final class SettableLevelDeflaterOutputStream extends DeflaterOutputStream {

		SettableLevelDeflaterOutputStream(OutputStream out, int level) {
			super(out);
			this.def.setLevel(level);
		}

	}

}
