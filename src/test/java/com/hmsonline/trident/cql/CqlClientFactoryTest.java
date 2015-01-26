package com.hmsonline.trident.cql;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class CqlClientFactoryTest {
	private static final Logger LOG = LoggerFactory.getLogger(CqlClientFactoryTest.class);
	public CqlClientFactory clientFactory;

	@Test
	public void testCompressionConf() throws Exception {
		clientFactory = new CqlClientFactory("", null, null, ConsistencyLevel.ANY, ProtocolOptions.Compression.valueOf("LZ4"));
		clientFactory = new CqlClientFactory("", null, null, ConsistencyLevel.ANY, ProtocolOptions.Compression.valueOf("NONE"));
	}
}