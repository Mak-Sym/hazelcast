package com.hazelcast.cache;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleMapConfigUpdateTest {
    private static final String SETTINGS_MAP_NAME = "my_custom_settings";


    private HazelcastInstance hazelcast1;
    private HazelcastInstance hazelcast2;

    @Before
    public void setUp() {
        Config hazelcastConfig = createHazelcastConfig();

        hazelcast1 = Hazelcast.newHazelcastInstance(hazelcastConfig);
        hazelcast2 = Hazelcast.newHazelcastInstance(hazelcastConfig);
        initSettingsMap(hazelcast1);
        initSettingsMap(hazelcast2);
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    /**
     * This test works in Hazelcast v. < 3.9.x It shows the way of re-configuring hazelcast maps at runtime. It's a
     * bit hacky way, but it worked before new dynamic map configuration was introduced in 3.9
     * (http://docs.hazelcast.org/docs/latest/manual/html-single/#dynamically-adding-data-structure-configuration-on-a-cluster).
     * With this new feature, dynamic map configuration is supported by Hazelcast natively, but the problem is
     * that maps cannot be re-configured at runtime. Re-configuration attempt may throw an exception or do nothing
     * (if "hazelcast.dynamicconfig.ignore.conflicts" system property is set to "true)"
     */
    @Test
    public void testEmulateMapReConfiguration() {
        final String mapName = "my.custom.map.test";

        // Configure new map
        configureMap(hazelcast1, mapName, new MyCustomSettings(1000));
        MapConfig config1 = hazelcast1.getConfig().findMapConfig(mapName);
        assertEquals(mapName, config1.getName());
        assertEquals(1000, config1.getMaxSizeConfig().getSize());

        MapConfig config2 = hazelcast2.getConfig().findMapConfig(mapName);
        assertEquals(mapName, config2.getName());
        assertEquals(1000, config2.getMaxSizeConfig().getSize());

        // Re-configure existent map
        hazelcast1.getMap(mapName);
        reconfigureMap(hazelcast1, mapName, new MyCustomSettings(2000));
        config1 = hazelcast1.getConfig().findMapConfig(mapName);
        assertEquals(2000, config1.getMaxSizeConfig().getSize());
        config2 = hazelcast2.getConfig().findMapConfig(mapName);
        assertEquals(2000, config2.getMaxSizeConfig().getSize());

    }

    private void initSettingsMap(final HazelcastInstance hazelcast) {

        hazelcast.getMap(SETTINGS_MAP_NAME).addEntryListener(
                new EntryUpdatedListener<String, MyCustomSettings>() {
                    @Override
                    public void entryUpdated(EntryEvent<String, MyCustomSettings> event) {
                        reconfigureMap(hazelcast, event.getKey(),
                                event.getValue());
                    }
                },
                true);

        hazelcast.getMap(SETTINGS_MAP_NAME).addEntryListener(
                new EntryAddedListener<String, MyCustomSettings>() {
                    @Override
                    public void entryAdded(EntryEvent<String, MyCustomSettings> event) {
                        SimpleMapConfigUpdateTest.this.configureMap(hazelcast, event.getKey(),
                                event.getValue());
                    }
                },
                true);

        hazelcast.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                updateMapContainers(hazelcast);
            }
        });
    }

    private void reconfigureMap(HazelcastInstance hazelcast, String mapName, MyCustomSettings settings) {
        Config config = hazelcast.getConfig();
        MapConfig mapConfig = new MapConfig(config.findMapConfig(mapName));
        mapConfig.getMaxSizeConfig().setSize(settings.getCapacity());
        mapConfig.setName(mapName);
        config.addMapConfig(mapConfig);
    }

    private void updateMapContainers(HazelcastInstance hazelcast) {
        for (Map.Entry<Object, Object> entry : hazelcast.getMap(SETTINGS_MAP_NAME).entrySet()) {
            configureMap(hazelcast, entry.getKey().toString(), (MyCustomSettings) entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void configureMap(HazelcastInstance hazelcast, String mapName, MyCustomSettings settings) {
        IMap mapSettings = hazelcast.getMap(SETTINGS_MAP_NAME);
        Config config = hazelcast.getConfig();
        MapConfig mapConfig = config.findMapConfig(mapName);
        if (!mapConfig.getName().equals(mapName)) {
            MapConfig newMapConfig = new MapConfig(mapConfig);
            newMapConfig.getMaxSizeConfig().setSize(settings.getCapacity());
            newMapConfig.setName(mapName);
            config.addMapConfig(newMapConfig);
            mapConfig = newMapConfig;

            // store the config in the settings map to trigger other nodes to also configure the IMap
            mapSettings.putIfAbsent(mapName, settings);
        } else {
            System.out.println("Using existing cache configuration for cache " + mapName);
        }

        updateMapContainerIfAbsent(hazelcast, mapName, mapConfig);
    }

    private void updateMapContainerIfAbsent(HazelcastInstance hazelcast, String mapName, MapConfig mapConfig) {
        MapProxyImpl<String, MyCustomSettings> proxy = hazelcast.getDistributedObject(MapService.SERVICE_NAME, SETTINGS_MAP_NAME);
        MapServiceContext mapServiceContext = proxy.getService().getMapServiceContext();
        final MapContainer container = mapServiceContext.getMapContainer(mapName);
        if (container != null && !container.getMapConfig().getName().equals(mapConfig.getName())) {
            // container does exist and is not configured
            container.setMapConfig(mapConfig);
        }
    }

    private Config createHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().setPortAutoIncrement(true);
        config.addMapConfig(new MapConfig("my.custom.map.*")
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
        return config;
    }
}

class MyCustomSettings implements Serializable {
    private int capacity;

    public MyCustomSettings(int capacity) {
        setCapacity(capacity);
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Capacity is less than 0!");
        }
        this.capacity = capacity;
    }
}