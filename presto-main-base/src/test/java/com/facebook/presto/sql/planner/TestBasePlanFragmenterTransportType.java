/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.TransportType;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.NATIVE_CUDF_EXCHANGE_ENABLED;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.sql.planner.BasePlanFragmenter.remoteStreamingExchangeTransportType;
import static com.facebook.presto.sql.planner.PlannerUtils.containsCoordinatorOnlyNode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestBasePlanFragmenterTransportType
{
    private PlanBuilder planBuilder;
    private PlanNodeIdAllocator idAllocator;
    private VariableReferenceExpression col;

    @BeforeMethod
    public void setUp()
    {
        idAllocator = new PlanNodeIdAllocator();
        planBuilder = new PlanBuilder(TEST_SESSION, idAllocator, dummyMetadata());
        col = new VariableReferenceExpression(Optional.empty(), "col", BigintType.BIGINT);
    }

    // -----------------------------------------------------------------------
    // Tests for containsCoordinatorOnlyNode
    // -----------------------------------------------------------------------

    @Test
    public void testWorkerTableScanDoesNotRunOnCoordinator()
    {
        // A regular (non-system) table scan should NOT be detected as coordinator
        assertFalse(containsCoordinatorOnlyNode(
                planBuilder.tableScan("hive", ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testInformationSchemaTableScanRunsOnCoordinator()
    {
        // information_schema connector is a system connector → should run on coordinator
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertTrue(containsCoordinatorOnlyNode(
                planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testSystemTablesConnectorRunsOnCoordinator()
    {
        // $system@ connector is a system connector → should run on coordinator
        ConnectorId systemId = ConnectorId.createSystemTablesConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                systemId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertTrue(containsCoordinatorOnlyNode(
                planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testExplainAnalyzeNodeRunsOnCoordinator()
    {
        // ExplainAnalyzeNode is a coordinator-only plan node
        ValuesNode source = planBuilder.values(col);
        ExplainAnalyzeNode explainAnalyze = new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                col,
                false,
                ExplainFormat.Type.TEXT);
        assertTrue(containsCoordinatorOnlyNode(explainAnalyze));
    }

    @Test
    public void testValuesNodeDoesNotRunOnCoordinator()
    {
        // A plain ValuesNode is not coordinator-only
        assertFalse(containsCoordinatorOnlyNode(planBuilder.values(col)));
    }

    @Test
    public void testStopsAtRemoteExchangeBoundary()
    {
        // A coordinator-only node behind a remote exchange should NOT be detected,
        // because remote exchanges create fragment boundaries
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        ExchangeNode remoteExchange = planBuilder.exchange(e -> e
                .scope(ExchangeNode.Scope.REMOTE_STREAMING)
                .type(ExchangeNode.Type.GATHER)
                .addSource(planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of()))
                .addInputsSet(ImmutableList.of())
                .singleDistributionPartitioningScheme(ImmutableList.of()));

        // The remote exchange itself should NOT be flagged as coordinator
        // (containsCoordinatorOnlyNode stops at remote exchange boundaries)
        assertFalse(containsCoordinatorOnlyNode(remoteExchange));
    }

    @Test
    public void testWalksThroughLocalExchange()
    {
        // A coordinator-only node behind a LOCAL exchange SHOULD be detected,
        // because local exchanges don't create fragment boundaries
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        ExchangeNode localExchange = planBuilder.exchange(e -> e
                .scope(ExchangeNode.Scope.LOCAL)
                .type(ExchangeNode.Type.GATHER)
                .addSource(planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of()))
                .addInputsSet(ImmutableList.of())
                .singleDistributionPartitioningScheme(ImmutableList.of()));

        assertTrue(containsCoordinatorOnlyNode(localExchange));
    }

    // -----------------------------------------------------------------------
    // Tests for FragmentProperties transport type
    // -----------------------------------------------------------------------

    @Test
    public void testFragmentPropertiesDefaultsToHttp()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        assertEquals(props.getOutputTransportType(), TransportType.HTTP);
    }

    @Test
    public void testFragmentPropertiesSetTransportType()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        props.setOutputTransportType(TransportType.ANY);
        assertEquals(props.getOutputTransportType(), TransportType.ANY);
    }

    @Test
    public void testHasCoordinatorOnlyDistribution()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        // Default is not coordinator-only
        assertFalse(props.hasCoordinatorOnlyDistribution());

        // After setting coordinator-only distribution via a coordinator-only node, it returns true
        ValuesNode source = planBuilder.values(col);
        ExplainAnalyzeNode explainAnalyze = new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                col,
                false,
                ExplainFormat.Type.TEXT);
        props.setCoordinatorOnlyDistribution(explainAnalyze);
        assertTrue(props.hasCoordinatorOnlyDistribution());
    }

    // -----------------------------------------------------------------------
    // Tests for the per-edge transport decision
    // -----------------------------------------------------------------------

    @Test
    public void testWorkerToWorkerEdgeIsHttpWhenCudfExchangeDisabled()
    {
        // A native worker fails the query when the plan names a transport it has not registered, so with the cuDF
        // exchange switch off the coordinator must not name UCX anywhere.
        ExchangeNode exchange = remoteStreamingExchange(workerSource());
        assertEquals(
                remoteStreamingExchangeTransportType(cudfExchangeSession(false), exchange, singleDistributionProperties()),
                TransportType.HTTP);
    }

    @Test
    public void testWorkerToWorkerEdgeIsAnyWhenCudfExchangeEnabled()
    {
        ExchangeNode exchange = remoteStreamingExchange(workerSource());
        assertEquals(
                remoteStreamingExchangeTransportType(cudfExchangeSession(true), exchange, singleDistributionProperties()),
                TransportType.ANY);
    }

    @Test
    public void testProducerOnCoordinatorEdgeStaysHttpWhenCudfExchangeEnabled()
    {
        ExchangeNode exchange = remoteStreamingExchange(coordinatorOnlySource());
        assertEquals(
                remoteStreamingExchangeTransportType(cudfExchangeSession(true), exchange, singleDistributionProperties()),
                TransportType.HTTP);
    }

    @Test
    public void testConsumerOnCoordinatorEdgeStaysHttpWhenCudfExchangeEnabled()
    {
        ExchangeNode exchange = remoteStreamingExchange(workerSource());
        BasePlanFragmenter.FragmentProperties consumerProperties = singleDistributionProperties();
        consumerProperties.setCoordinatorOnlyDistribution(coordinatorOnlyNode());
        assertEquals(
                remoteStreamingExchangeTransportType(cudfExchangeSession(true), exchange, consumerProperties),
                TransportType.HTTP);
    }

    @Test
    public void testEveryEdgeIsHttpWhenCudfExchangeDisabled()
    {
        // With the switch off no topology matters: every edge, worker-to-worker or coordinator-facing, stays HTTP.
        Session session = cudfExchangeSession(false);
        BasePlanFragmenter.FragmentProperties coordinatorConsumer = singleDistributionProperties();
        coordinatorConsumer.setCoordinatorOnlyDistribution(coordinatorOnlyNode());

        assertEquals(
                remoteStreamingExchangeTransportType(session, remoteStreamingExchange(workerSource()), singleDistributionProperties()),
                TransportType.HTTP);
        assertEquals(
                remoteStreamingExchangeTransportType(session, remoteStreamingExchange(coordinatorOnlySource()), singleDistributionProperties()),
                TransportType.HTTP);
        assertEquals(
                remoteStreamingExchangeTransportType(session, remoteStreamingExchange(workerSource()), coordinatorConsumer),
                TransportType.HTTP);
    }

    @Test
    public void testUnorderedWorkerToWorkerExchangeUsesAny()
    {
        // The counterpart of testOrderedExchangeStaysOnHttp: the same topology without an ordering scheme
        // is the case UCX exists for, so it must still be annotated ANY.
        assertEquals(
                remoteStreamingExchangeTransportType(
                        cudfExchangeSession(true),
                        remoteStreamingExchange(workerSource()),
                        singleDistributionProperties()),
                TransportType.ANY);
    }

    // -----------------------------------------------------------------------
    // Tests for PlanFragment transport type serialization
    // -----------------------------------------------------------------------

    @Test
    public void testPlanFragmentConvenienceConstructorDefaultsToHttp()
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(0),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.HTTP);
    }

    @Test
    public void testPlanFragmentFullConstructorPreservesAny()
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(1),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                TransportType.ANY,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.ANY);
    }

    @Test
    public void testPlanFragmentNullTransportTypeDefaultsToHttp()
    {
        // Simulates backward-compatible deserialization where outputTransportType is null
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(2),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                null,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.HTTP);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Session cudfExchangeSession(boolean enabled)
    {
        return Session.builder(TEST_SESSION)
                .setSystemProperty(NATIVE_CUDF_EXCHANGE_ENABLED, Boolean.toString(enabled))
                .build();
    }

    private BasePlanFragmenter.FragmentProperties singleDistributionProperties()
    {
        return new BasePlanFragmenter.FragmentProperties(new PartitioningScheme(
                Partitioning.create(SystemPartitioningHandle.SINGLE_DISTRIBUTION, ImmutableList.of()),
                ImmutableList.of(col)));
    }

    private ExchangeNode remoteStreamingExchange(PlanNode source)
    {
        return planBuilder.exchange(e -> e
                .scope(ExchangeNode.Scope.REMOTE_STREAMING)
                .type(ExchangeNode.Type.GATHER)
                .addSource(source)
                .addInputsSet(ImmutableList.of())
                .singleDistributionPartitioningScheme(ImmutableList.of()));
    }

    // A scan of the information_schema connector, which only runs on the coordinator.
    private PlanNode coordinatorOnlySource()
    {
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        return planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of());
    }

    // A scan of a regular connector, which runs on the workers.
    private PlanNode workerSource()
    {
        return planBuilder.tableScan("hive", ImmutableList.of(), ImmutableMap.of());
    }

    // A plan node that forces its fragment onto the coordinator.
    private PlanNode coordinatorOnlyNode()
    {
        ValuesNode source = planBuilder.values(col);
        return new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                col,
                false,
                ExplainFormat.Type.TEXT);
    }
}
