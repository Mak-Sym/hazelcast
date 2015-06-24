# Hazelcast Java Client

There are currently three ways to connect to a running Hazelcast cluster:

* Native Clients (Java, C++, .NET)
* [Memcache Client](#memcache-client)
* [REST Client](#rest-client)

Native Clients enable you to perform almost all Hazelcast operations without being a member of the cluster. It connects to one of the cluster members and delegates all cluster wide operations to it (_dummy client_), or it connects to all of them and delegates operations smartly (_smart client_). When the relied cluster member dies, the client will transparently switch to another live member.

There can be hundreds or even thousands of clients connected to the cluster. By default, there are *core count* * *10* threads on the server side that will handle all the requests (e.g. if the server has 4 cores, it will be 40).

Imagine a trading application where all the trading data are stored and managed in a Hazelcast cluster with tens of nodes. Swing/Web applications at the traders' desktops can use Native Clients to access and modify the data in the Hazelcast cluster.

Currently, Hazelcast has Native Java, C++ and .NET Clients available. This chapter describes the Java Client.

<br><br>
![image](images/NoteSmall.jpg) ***IMPORTANT:*** *Starting with the Hazelcast 3.5. release, a new Java Native Client Library is introduced in the release package: `hazelcast-client-new-<version>.jar`. This library contains clients which use the new Hazelcast Binary Client Protocol. This library does not exist for the releases before 3.5.*

<br><br>