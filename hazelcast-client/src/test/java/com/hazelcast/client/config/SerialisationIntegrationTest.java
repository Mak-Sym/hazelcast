package com.hazelcast.client.config;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.cluster.impl.ConfigCheck;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.Packet;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.version.MemberVersion;
import example.serialization.TestDeserialized;
import example.serialization.TestDeserializedOk;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.closeResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SerialisationIntegrationTest {
    private static int flag = 0;
    private static final int MULTICAST_PORT = 53535;
    private static final String MULTICAST_GROUP = "224.2.2.3";
    // TTL = 0 : restricted to the same host, won't be output by any interface
    private static final int MULTICAST_TTL = 0;
    private HazelcastInstance instance;
    private HazelcastInstance instance2;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        TestDeserialized.isDeserialized = false;
        TestDeserialized.isSerialized = false;
        flag = 0;
        if (instance != null)
            instance.shutdown();
        if (instance2 != null)
            instance2.shutdown();
        if (client != null)
            client.shutdown();
        instance = null;
        instance2 = null;
        client = null;
    }

    // this test works as expected and blocks TestDeserialized object, which is sent as multicast request
    @Test
    public void testBlacklistedMulticastObject() throws Exception {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);

        sendJoinDatagram(new TestDeserialized(), ((HazelcastInstanceProxy) instance).getOriginal());
        assertTrue(TestDeserialized.isSerialized);
        Thread.sleep(5000L);
        assertFalse("Untrusted deserialization is possible", TestDeserialized.isDeserialized);
    }

    // this test works as expected and blocks JoinRequest, which contains TestDeserialized object as its attribute
    @Test
    public void testBlacklistedPropertyInJoinRequest() throws Exception {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("bad.guy", new TestDeserialized());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION,
                10,
                new MemberVersion(1,2,3),
                instance.getCluster().getLocalMember().getAddress(),
                UUID.randomUUID().toString(),
                false,
                new ConfigCheck(config, "multicast"),
                new UsernamePasswordCredentials("test", "test"),
                attributes,
                Collections.EMPTY_SET);

        sendJoinDatagram(joinRequest, ((HazelcastInstanceProxy) instance).getOriginal());
        assertTrue(TestDeserialized.isSerialized);
        Thread.sleep(5000L);
        assertFalse("Untrusted deserialization is possible", TestDeserialized.isDeserialized);
    }

    // this test works as expected, and TestDeserializedOk gets deserialized
    @Test
    public void testNonBlacklistedJoinRequest() throws Exception {
        assertFalse(TestDeserializedOk.isDeserialized);
        assertFalse(TestDeserializedOk.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);

        sendJoinDatagram(new TestDeserializedOk(), ((HazelcastInstanceProxy) instance).getOriginal());
        assertTrue(TestDeserializedOk.isSerialized);
        Thread.sleep(5000L);
        assertTrue("Deserialization should happen", TestDeserializedOk.isDeserialized);
    }

    // this test is green, so blacklist seems to not affect map operations
    @Test
    public void testMapOperationsStillWork() {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = getClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap cache = client.getMap("mycache");
        for (int i = 0; i < 100; i++) {
            cache.put(i, new TestDeserialized(i));
        }
        assertEquals(100, cache.size());
        for (int i = 0; i < 100; i++) {
            assertNotNull(cache.get(i));
        }
        assertTrue(TestDeserialized.isSerialized);
        assertTrue(TestDeserialized.isDeserialized);
    }

    // this test is green, so blacklist seems to not affect map operations
    @Test
    public void testMapOperationsStillWork2() {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);
        instance2 = Hazelcast.newHazelcastInstance(config);

        IMap cache = instance.getMap("mycache");
        for (int i = 0; i < 100; i++) {
            cache.put(i, new TestDeserialized(i));
        }
        assertEquals(100, cache.size());
        cache = instance2.getMap("mycache");
        for (int i = 0; i < 100; i++) {
            assertNotNull(cache.get(i));
        }
        assertTrue(TestDeserialized.isSerialized);
        assertTrue(TestDeserialized.isDeserialized);
    }

    // this test is red - blacklisted objects cannot be passed to remote exec services
    @Test
    public void testRemoteExecutionStillWorks() throws ExecutionException, InterruptedException {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = getClientConfig();
        client = HazelcastClient.newHazelcastClient(clientConfig);
        final int randomNumber = new Random().nextInt();

        Future<TestDeserialized> future = client.getExecutorService("myservice").submit(new RemoteExecTask(randomNumber));
        assertEquals(future.get().getI(), randomNumber);
        assertTrue(TestDeserialized.isSerialized);
        assertTrue(TestDeserialized.isDeserialized);
    }

    // this test is red - blacklisted objects cannot be passed to remote exec services
    @Test
    public void testRemoteExecutionStillWorks2() throws ExecutionException, InterruptedException {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);
        instance2 = Hazelcast.newHazelcastInstance(config);
        final Member member2 = instance2.getCluster().getLocalMember();
        assertEquals(2, instance.getCluster().getMembers().size());

        final int randomNumber = new Random().nextInt();

        Future<TestDeserialized> future = instance.getExecutorService("myservice")
                .submit(new RemoteExecTask(randomNumber), new MemberSelector() {
                    @Override
                    public boolean select(Member member) {
                        return member.getUuid().equals(member2.getUuid());
                    }
                });
        assertEquals(future.get().getI(), randomNumber);
        assertTrue(TestDeserialized.isSerialized);
        assertTrue(TestDeserialized.isDeserialized);
    }

    // this test is green - it seems that blacklist does not affect objects, which are passed as topic messages
    @Test
    public void testITopicsStillWork() throws InterruptedException {
        assertFalse(TestDeserialized.isDeserialized);
        assertFalse(TestDeserialized.isSerialized);
        Config config = getConfig();
        instance = Hazelcast.newHazelcastInstance(config);
        instance2 = Hazelcast.newHazelcastInstance(config);
        ITopic<TestDeserialized> topic1 = instance.getTopic("m");
        ITopic<TestDeserialized>  topic2 = instance2.getTopic("m");
        assertEquals(2, instance.getCluster().getMembers().size());

        final int rnd = new Random().nextInt(1000) + 1;
        final CountDownLatch cl = new CountDownLatch(1);
        topic2.addMessageListener(new MessageListener<TestDeserialized>() {
            @Override
            public void onMessage(Message<TestDeserialized> message) {
                flag = message.getMessageObject().getI();
                cl.countDown();
            }
        });

        topic1.publish(new TestDeserialized(rnd));
        cl.await(120, TimeUnit.SECONDS);
        assertEquals(flag, rnd);
    }

    private Config getConfig() {
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                .setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist()
                .addClasses(TestDeserialized.class.getName());

        Config config = new Config();
        config.getSerializationConfig()
                .setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getTcpIpConfig()
                .setEnabled(false);
        join.getMulticastConfig()
                .setEnabled(true)
                .setMulticastPort(MULTICAST_PORT)
                .setMulticastGroup(MULTICAST_GROUP)
                .setMulticastTimeToLive(MULTICAST_TTL);
        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig config = new ClientConfig();
        DiscoveryStrategyConfig strategy = new DiscoveryStrategyConfig("com.hazelcast.spi.discovery.multicast.MulticastDiscoveryStrategy");
        strategy.addProperty("group", MULTICAST_GROUP);
        strategy.addProperty("port", MULTICAST_PORT);
        config.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(strategy);
        return config;
    }

    private void sendJoinDatagram(Object object, HazelcastInstanceImpl instance) throws IOException {
        ByteBuffer bbuf;
        if (object instanceof JoinRequest) {
            byte[] data = instance.getNode().getSerializationService().toBytes(object);
            bbuf = ByteBuffer.allocate(data.length);
            bbuf.put(data);
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            try {
                oos.writeObject(object);
            } finally {
                closeResource(oos);
            }
            byte[] data = bos.toByteArray();
            int msgSize = data.length;
            bbuf = ByteBuffer.allocate(1 + 4 + msgSize);
            bbuf.put(Packet.VERSION);
            bbuf.putInt(SerializationConstants.JAVA_DEFAULT_TYPE_SERIALIZABLE);
            bbuf.put(data);
        }

        MulticastSocket multicastSocket = null;
        try {
            multicastSocket = new MulticastSocket(MULTICAST_PORT);
            multicastSocket.setTimeToLive(MULTICAST_TTL);
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            multicastSocket.joinGroup(group);

            byte[] packetData = bbuf.array();
            DatagramPacket packet = new DatagramPacket(packetData, packetData.length, group, MULTICAST_PORT);
            multicastSocket.send(packet);
            multicastSocket.leaveGroup(group);
        } finally {
            if (multicastSocket != null) {
                multicastSocket.close();
            }
        }
    }
}
