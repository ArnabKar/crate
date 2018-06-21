/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.collectors;


import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.OrderBy;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.metadata.Reference;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.crate.testing.TestingHelpers.createReference;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OrderedLuceneBatchIteratorFactoryTest extends CrateUnitTest {

    private static final RowAccounting ROW_ACCOUNTING = new RowAccounting(Collections.singleton(LongType.INSTANCE),
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA))
    );

    private String columnName = "x";
    private Reference reference = createReference(columnName, DataTypes.LONG);
    private IndexSearcher searcher1;
    private IndexSearcher searcher2;
    private OrderBy orderBy;
    private List<Object[]> expectedResult;
    private boolean[] reverseFlags = new boolean[]{true};
    private Boolean[] nullsFirst = new Boolean[]{null};

    @Before
    public void prepareSearchers() throws Exception {
        IndexWriter iw1 = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        IndexWriter iw2 = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer()));

        expectedResult = LongStream.range(0, 20)
            .mapToObj(i -> new Object[]{i})
            .collect(Collectors.toList());
        // expect descending order to differentiate between insert order
        expectedResult.sort(Comparator.comparingLong((Object[] o) -> ((long) o[0])).reversed());

        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(columnName, i));
            if (i % 2 == 0) {
                iw1.addDocument(doc);
            } else {
                iw2.addDocument(doc);
            }
        }
        iw1.commit();
        iw2.commit();

        searcher1 = new IndexSearcher(DirectoryReader.open(iw1));
        searcher2 = new IndexSearcher(DirectoryReader.open(iw2));
        orderBy = new OrderBy(
            Collections.singletonList(reference),
            reverseFlags,
            nullsFirst
        );
    }

    @Test
    public void testOrderedLuceneBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                LuceneOrderedDocCollector collector1 = createOrderedCollector(searcher1, 1);
                LuceneOrderedDocCollector collector2 = createOrderedCollector(searcher2, 2);
                return OrderedLuceneBatchIteratorFactory.newInstance(
                    Arrays.asList(collector1, collector2),
                    OrderingByPosition.rowOrdering(new int[]{0}, reverseFlags, nullsFirst),
                    ROW_ACCOUNTING,
                    MoreExecutors.directExecutor(),
                    () -> 1,
                    true
                );
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testSingleCollectorOrderedLuceneBatchIteratorTripsCircuitBreaker() throws Exception {
        RowAccounting rowAccounting = mock(RowAccounting.class);
        CircuitBreakingException circuitBreakingException = new CircuitBreakingException("tripped circuit breaker");
        doThrow(circuitBreakingException)
            .when(rowAccounting).accountForAndMaybeBreak(any(Row.class));

        BatchIterator<Row> rowBatchIterator = OrderedLuceneBatchIteratorFactory.newInstance(
            Arrays.asList(createOrderedCollector(searcher1, 1)),
            OrderingByPosition.rowOrdering(new int[]{0}, reverseFlags, nullsFirst),
            rowAccounting,
            MoreExecutors.directExecutor(),
            () -> 2,
            true
        );

        consumeIteratorAndVerifyResultIsException(rowBatchIterator, circuitBreakingException);
    }

    @Test
    public void testOrderedLuceneBatchIteratorWithMultipleCollectorsTripsCircuitBreaker() throws Exception {
        RowAccounting rowAccounting = mock(RowAccounting.class);
        CircuitBreakingException circuitBreakingException = new CircuitBreakingException("tripped circuit breaker");
        doThrow(circuitBreakingException)
            .when(rowAccounting).accountForAndMaybeBreak(any(Row.class));

        LuceneOrderedDocCollector collector1 = createOrderedCollector(searcher1, 1);
        LuceneOrderedDocCollector collector2 = createOrderedCollector(searcher2, 2);
        BatchIterator<Row> rowBatchIterator = OrderedLuceneBatchIteratorFactory.newInstance(
            Arrays.asList(collector1, collector2),
            OrderingByPosition.rowOrdering(new int[]{0}, reverseFlags, nullsFirst),
            rowAccounting, MoreExecutors.directExecutor(),
            () -> 1,
            true
        );

        consumeIteratorAndVerifyResultIsException(rowBatchIterator, circuitBreakingException);
    }

    private void consumeIteratorAndVerifyResultIsException(BatchIterator<Row> rowBatchIterator, Exception exception)
        throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(rowBatchIterator, null);

        expectedException.expect(exception.getClass());
        expectedException.expectMessage(exception.getMessage());
        consumer.getResult();
    }

    private LuceneOrderedDocCollector createOrderedCollector(IndexSearcher searcher, int shardId) {
        CollectorContext collectorContext = new CollectorContext(
            mock(IndexFieldDataService.class),
            new CollectorFieldsVisitor(0)
        );
        List<LuceneCollectorExpression<?>> expressions = Collections.singletonList(
            new OrderByCollectorExpression(reference, orderBy, o -> o));
        return new LuceneOrderedDocCollector(
            new ShardId("dummy", UUIDs.randomBase64UUID(), shardId),
            searcher,
            new MatchAllDocsQuery(),
            null,
            false,
            5, // batchSize < 10 to have at least one searchMore call.
            collectorContext,
            f -> null,
            new Sort(new SortedNumericSortField(columnName, SortField.Type.LONG, reverseFlags[0])),
            expressions,
            expressions
        );
    }
}
