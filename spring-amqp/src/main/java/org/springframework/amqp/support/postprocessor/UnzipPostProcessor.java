/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.util.Assert;

/**
 * A post processor that uses a {@link ZipInputStream} to decompress the
 * message body.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class UnzipPostProcessor extends AbstractDecompressingPostProcessor {

	public UnzipPostProcessor() {
	}

	public UnzipPostProcessor(boolean alwaysDecompress) {
		super(alwaysDecompress);
	}

	@Override
	protected InputStream getDecompressorStream(InputStream zipped) throws IOException {
		ZipInputStream zipper = new ZipInputStream(zipped);
		ZipEntry entry = zipper.getNextEntry();
		String entryName = entry.getName();
		Assert.state("amqp".equals(entryName), () -> "Zip 'entryName' must be 'amqp', not '" + entryName + "'");
		return zipper;
	}

	@Override
	protected String getEncoding() {
		return "zip";
	}

}
