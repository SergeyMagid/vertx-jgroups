/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.test.core;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.java.spi.cluster.impl.jgroups.JGroupsClusterManager;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class JGroupsClusteredEventbusTest extends ClusteredEventBusTest {

    private static final Logger log = LoggerFactory.getLogger(JGroupsClusteredEventbusTest.class);
    public JGroupsClusteredEventbusTest() {
        disableThreadChecks();
    }

    public static final String WRAPPED_CHANNEL = "wrapper-channel";
    private static final String ADDRESS1 = "some-address1";


    @Test
    public void testSubsRemovedForKilledNode2() throws Exception {
        testSubsRemoved(null);
    }

    private void testSubsRemoved(Consumer<CountDownLatch> action) throws Exception {
        startNodes(2);

        CountDownLatch regLatch = new CountDownLatch(1);
        AtomicInteger cnt = new AtomicInteger();


        vertices[1].eventBus().consumer(WRAPPED_CHANNEL, wrapperEvent -> {
            EventBus ebSender = vertices[1].eventBus();
            ebSender.send(ADDRESS1, "foo" + (int) wrapperEvent.body(), reply -> {
                assertEquals("ok", reply.result().body().toString().substring(0, 2));
                wrapperEvent.reply("ok");
            });
        });

        vertices[0].eventBus().consumer(ADDRESS1, msg -> {
            int c = cnt.getAndIncrement();
            assertEquals(msg.body(), "foo" + c);

            System.out.println(">> receive and reply in consumer before kill");

            msg.reply("ok" + c);

            if (c > 1) {
                fail("too many messages");
            }
        }).completionHandler(onSuccess(v -> {
            vertices[1].runOnContext(v1 -> {
                // Now send some messages from node 2 - they should ALL go to node 0
                EventBus ebSender = vertices[1].eventBus();
                ebSender.send(WRAPPED_CHANNEL, 0);
                regLatch.countDown();
            });
        }));


        awaitLatch(regLatch);
        kill(0);

        // Allow time for kill to be propagate
        Thread.sleep(2000);

        addNodes(1);

        Thread.sleep(500);

        vertices[2].eventBus().consumer(ADDRESS1, msg -> {

            int c = cnt.getAndIncrement();
            assertEquals(msg.body(), "foo" + c);
            msg.reply("ok" + c);

            if (c == 0) {
                fail("should not get first messages");
            }
        }).completionHandler(onSuccess(v2 -> {

            CountDownLatch directCount = new CountDownLatch(1);

            //Check that this consumer available directly
            vertices[1].runOnContext(v -> {
                EventBus ebSender = vertices[1].eventBus();
                ebSender.send(ADDRESS1, "foo" + 1, replyHandler -> {
                    System.out.println(">>> direct request is " + replyHandler.succeeded());
                    if (replyHandler.failed()) {
                        fail("I should receive success reply");
                    }
                    directCount.countDown();
                });
            });

            try {
                directCount.await();
            } catch (InterruptedException e) {
            }


            vertices[1].runOnContext(v1 -> {

                System.out.printf("Send request throw wrapper");
                EventBus ebSender = vertices[1].eventBus();
                ebSender.send(WRAPPED_CHANNEL, 2, handler -> {
                    if (handler.succeeded()) {
                        complete();
                    }
                });
                regLatch.countDown();
            });

        }));

        await();
    }


    protected void addNodes(int numNodes) {
        System.out.println("Before add node");

        addNodes(numNodes, getOptions());
    }

    protected void addNodes(int numNodes, VertxOptions options) {
        CountDownLatch latch = new CountDownLatch(numNodes);
        Vertx[] currentVertices = vertices;
        int totalVert = numNodes + vertices.length;
        vertices = new Vertx[totalVert];

        for (int i = 0; i < currentVertices.length; i++) {
            vertices[i] = currentVertices[i];
        }


        for (int i = currentVertices.length; i < totalVert; i++) {
            int index = i;
            Vertx.clusteredVertx(options.setClusterHost("localhost").setClusterPort(0).setClustered(true)
                    .setClusterManager(getClusterManager()), ar -> {
                if (ar.failed()) {
                    ar.cause().printStackTrace();
                }
                assertTrue("Failed to start node", ar.succeeded());
                vertices[index] = ar.result();
                latch.countDown();
            });
        }
        try {
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        System.out.println("Added new nodes count " + numNodes + ", total count " + vertices.length);
    }

    protected void kill(int pos) {
        VertxInternal v = (VertxInternal) vertices[pos];
        v.executeBlocking(fut -> {
            v.simulateKill();
            fut.complete();
        }, ar -> {
            assertTrue(ar.succeeded());
        });
    }

    @Override
    protected ClusterManager getClusterManager() {
        return new JGroupsClusterManager();
    }

}
