/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A post processor that uses a {@link ZipOutputStream} to compress the message body. Sets
 * {@link org.springframework.amqp.core.MessageProperties#SPRING_AUTO_DECOMPRESS} to true
 * by default.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class ZipPostProcessor extends AbstractDeflaterPostProcessor {

	public ZipPostProcessor() {
	}

	public ZipPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	@Override
	protected OutputStream getCompressorStream(OutputStream zipped) throws IOException {
		ZipOutputStream zipper = new SettableLevelZipOutputStream(zipped, getLevel());
		zipper.putNextEntry(new ZipEntry("amqp"));
		return zipper;
	}

	@Override
	protected String getEncoding() {
		return "zip";
	}

	private static final class SettableLevelZipOutputStream extends ZipOutputStream {

		SettableLevelZipOutputStream(OutputStream zipped, int level) {
			super(zipped);
			this.setLevel(level);
		}

	}

}
