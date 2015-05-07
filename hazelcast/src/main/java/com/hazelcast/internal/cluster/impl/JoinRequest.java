/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.instance.Capability;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.Credentials;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JoinRequest extends JoinMessage implements DataSerializable {

    private Credentials credentials;
    private int tryCount;
    private Map<String, Object> attributes;
    private Set<Capability> capabilities;

    public JoinRequest() {
    }

    public JoinRequest(byte packetVersion, int buildNumber, Address address, String uuid, boolean liteMember, ConfigCheck config,
                       Credentials credentials, Set<Capability> capabilities, Map<String, Object> attributes) {
        super(packetVersion, buildNumber, address, uuid, liteMember, config);
        this.credentials = credentials;
        this.capabilities = capabilities;
        this.attributes = attributes;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public MemberInfo toMemberInfo() {
        return new MemberInfo(address, uuid, capabilities, attributes, liteMember);
    }

    public Set<Capability> getCapabilities() {
        return capabilities;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        credentials = in.readObject();
        if (credentials != null) {
            credentials.setEndpoint(getAddress().getHost());
        }
        tryCount = in.readInt();
        capabilities = Capability.readCapabilities(in);
        int size = in.readInt();
        attributes = new HashMap<String, Object>(size, 1.0f);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Object value = in.readObject();
            attributes.put(key, value);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(credentials);
        out.writeInt(tryCount);
        Capability.writeCapabilities(out, capabilities);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public String toString() {
        return "JoinRequest{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", address=" + address
                + ", uuid='" + uuid + "'"
                + ", liteMember=" + liteMember
                + ", credentials=" + credentials
                + ", memberCount=" + getMemberCount()
                + ", tryCount=" + tryCount
                + '}';
    }

}
