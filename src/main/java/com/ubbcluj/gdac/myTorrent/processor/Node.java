package com.ubbcluj.gdac.myTorrent.processor;

import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.network.NetworkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private final Protocol.NodeId nodeId;
    private final Protocol.NodeId hubId;

    private final NetworkHandler networkHandler;

    public Node(Protocol.NodeId nodeId, Protocol.NodeId hubId) {
        this.nodeId = nodeId;
        this.hubId = hubId;
        this.networkHandler = new NetworkHandler(nodeId.getHost(), nodeId.getPort(), this);
    }

    @Override
    public void run() {
        log.info("Running process {}-{}", nodeId.getOwner(), nodeId.getIndex());

        Thread messageListener = new Thread(networkHandler::listenForRequests);
        messageListener.start();
        sendRegistrationRequest();

        try {
            messageListener.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sendRegistrationRequest() {
        Protocol.Message registrationRequest = Protocol.Message.newBuilder()
                .setType(Protocol.Message.Type.REGISTRATION_REQUEST)
                .setRegistrationRequest(Protocol.RegistrationRequest.newBuilder()
                        .setIndex(nodeId.getIndex())
                        .setOwner(nodeId.getOwner())
                        .setPort(nodeId.getPort())
                        .build())
                .build();

        Protocol.Message registrationResponse = networkHandler.sendRequestAndReceiveResponse(registrationRequest, hubId.getHost(), hubId.getPort());

        if (registrationResponse == null) {
            log.error("{}-{} could not send RegistrationRequest", nodeId.getOwner(), nodeId.getIndex());
        } else if (registrationResponse.getRegistrationResponse().getStatus() != Protocol.Status.SUCCESS) {
            log.error("{}-{} RegistrationRequest returned error: {}", nodeId.getOwner(), nodeId.getIndex(), registrationResponse.getRegistrationResponse().getErrorMessage());
        } else {
            log.info("{}-{} RegistrationRequest returned RegistrationResponse with status SUCCESS", nodeId.getOwner(), nodeId.getIndex());
        }
    }

    public Protocol.Message processMessage(Protocol.Message message) {
        System.out.println("process message");
        return null;
    }
}
