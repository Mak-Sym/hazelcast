package com.hazelcast.client.config;

import example.serialization.TestDeserialized;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class RemoteExecTask implements Callable<TestDeserialized>, Serializable {
    private TestDeserialized val;

    public RemoteExecTask(int i) {
        val = new TestDeserialized(i);
    }

    @Override
    public TestDeserialized call() {
        return new TestDeserialized(val.getI());
    }
}
