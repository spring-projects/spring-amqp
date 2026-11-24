/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
