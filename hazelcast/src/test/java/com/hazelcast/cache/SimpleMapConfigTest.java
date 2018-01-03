package com.hazelcast.cache;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleMapConfigTest {
    private HazelcastInstance hazelcast;

    @Before
    public void setUp() {
        Config hazelcastConfig = new Config();
        hazelcastConfig.addMapConfig(new MapConfig("my.custom.map.*")
                .setMaxSizeConfig(new MaxSizeConfig(5000, MaxSizeConfig.MaxSizePolicy.PER_NODE))
                .setTimeToLiveSeconds(3600)
                .setBackupCount(1)
                .setMaxIdleSeconds(3600)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setReadBackupData(false)
                .setNearCacheConfig(null)
                .setMapStoreConfig(null)
                .setWanReplicationRef(null)
                .setEntryListenerConfigs(Lists.<EntryListenerConfig>newArrayList())
                .setMapIndexConfigs(Lists.<MapIndexConfig>newArrayList())
                .setMergePolicy("com.hazelcast.map.merge.PutIfAbsentMapMergePolicy"));
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastConfig);
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConfig() {
        final String mapName = "my.custom.map.cache";
        Config config = hazelcast.getConfig();

        config.addMapConfig(new MapConfig(mapName)
                .setMaxSizeConfig(new MaxSizeConfig(500, MaxSizeConfig.MaxSizePolicy.PER_PARTITION))
                .setTimeToLiveSeconds(3600)
                .setBackupCount(1)
                .setMaxIdleSeconds(3600)
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setReadBackupData(false)
                .setNearCacheConfig(null)
                .setMapStoreConfig(null)
                .setWanReplicationRef(null)
                .setEntryListenerConfigs(Lists.<EntryListenerConfig>newArrayList())
                .setMapIndexConfigs(Lists.<MapIndexConfig>newArrayList())
                .setMergePolicy("com.hazelcast.map.merge.PutIfAbsentMapMergePolicy"));

        final IMap<String, String> map = hazelcast.getMap(mapName);

        MapConfig mapConfig = hazelcast.getConfig().findMapConfig(mapName);
        assertThat(mapConfig.getName(), equalTo(mapName));
    }
}
