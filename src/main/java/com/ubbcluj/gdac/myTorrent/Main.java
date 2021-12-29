package com.ubbcluj.gdac.myTorrent;


import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.processor.Node;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    private static List<Protocol.NodeId> nodeIds;

    public static void main(String[] args) throws InterruptedException {

        String hubHost = args[0];
        int hubPort = Integer.parseInt(args[1]);
        Protocol.NodeId hubId = Protocol.NodeId
                .newBuilder()
                .setHost(hubHost)
                .setPort(hubPort)
                .setOwner("hub")
                .build();

        final String nodeHost = args[2];
        final List<Integer> nodePorts = Arrays.asList(Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        final String nodeOwner = args[6];

        nodeIds = nodePorts.stream()
                .map(port -> Protocol.NodeId.newBuilder()
                        .setHost(nodeHost)
                        .setPort(port)
                        .setOwner(nodeOwner)
                        .setIndex(nodePorts.indexOf(port) + 1)
                        .build())
                .collect(Collectors.toList());

        Thread node1 = new Thread(new Node(nodeIds.get(0), hubId), nodeHost + ":" + nodePorts.get(0));
        Thread node2 = new Thread(new Node(nodeIds.get(1), hubId), nodeHost + ":" + nodePorts.get(1));
        Thread node3 = new Thread(new Node(nodeIds.get(2), hubId), nodeHost + ":" + nodePorts.get(2));

        node1.start();
        node2.start();
        node3.start();

        node1.join();
        node2.join();
        node3.join();
    }
}
