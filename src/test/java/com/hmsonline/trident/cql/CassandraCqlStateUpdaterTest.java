package com.hmsonline.trident.cql;

import com.datastax.driver.core.Statement;
import com.hmsonline.trident.cql.mappers.CqlTupleMapper;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.MockTridentTuple;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class CassandraCqlStateUpdaterTest extends TestCase {

	@Test
	public void testTuplesPropagation() throws Exception {
		List<TridentTuple> tuples = getTridentTuples();
		MockTridentCollector mockTridentCollector = new MockTridentCollector();

		// propagateTuples=false
		CassandraCqlStateUpdater<String, String> stateUpdaterNoTuplesPropagation = new CassandraCqlStateUpdater<String, String>(new MockCqlTupleMapper());
		stateUpdaterNoTuplesPropagation.updateState(new CassandraCqlState(null, null), tuples, mockTridentCollector);
		assertTrue(mockTridentCollector.emittedTuples.isEmpty());

		// propagateTuples=true
		CassandraCqlStateUpdater<String, String> stateUpdaterTuplesPropagation = new CassandraCqlStateUpdater<String, String>(new MockCqlTupleMapper(), true);
		stateUpdaterTuplesPropagation.updateState(new CassandraCqlState(null, null), tuples, mockTridentCollector);
		assertEquals(mockTridentCollector.emittedTuples, tuples);
	}

	private List<TridentTuple> getTridentTuples() {
		List<TridentTuple> tuples = new ArrayList<TridentTuple>();
		List<String> mockFieldList = new ArrayList<String>();
		mockFieldList.add("testField");
		TridentTuple tuple = new MockTridentTuple(mockFieldList, "testValue");
		tuples.add(tuple);
		return tuples;
	}

	private static class MockTridentCollector implements TridentCollector {

		public List<Object> emittedTuples = new ArrayList<>();

		@Override
		public void emit(List<Object> values) {
			emittedTuples.add(values);
		}

		@Override
		public void reportError(Throwable t) {
			// Do nothing.
		}
	}

	private static class MockCqlTupleMapper implements CqlTupleMapper {

		@Override
		public Statement map(Object key, Object value) {
			// Do nothing.
			return null;
		}

		@Override
		public Statement map(TridentTuple tuple) {
			// Do nothing.
			return null;
		}

		@Override
		public Statement retrieve(Object key) {
			// Do nothing.
			return null;
		}
	}
}