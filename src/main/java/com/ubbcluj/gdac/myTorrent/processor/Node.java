package com.ubbcluj.gdac.myTorrent.processor;

import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.model.File;
import com.ubbcluj.gdac.myTorrent.network.NetworkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class Node implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private final Protocol.NodeId nodeId;
    private final Protocol.NodeId hubId;

    private final NetworkHandler networkHandler;
    private final Map<String, File> storedFiles;

    public Node(Protocol.NodeId nodeId, Protocol.NodeId hubId) {
        this.nodeId = nodeId;
        this.hubId = hubId;
        this.networkHandler = new NetworkHandler(nodeId.getHost(), nodeId.getPort(), this);
        this.storedFiles = new HashMap<>();
    }

    @Override
    public void run() {
        log.info("Running process {}", getNodeName());

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

        Protocol.Message registrationResponse = null;
        try {
            registrationResponse = networkHandler.sendRequestAndReceiveResponse(registrationRequest, hubId.getHost(), hubId.getPort());
        } catch (IOException e) {
            log.error("{} could not connect to hub", getNodeName());
        }

        if (registrationResponse == null) {
            log.error("{} could not send RegistrationRequest", getNodeName());
        } else if (registrationResponse.getRegistrationResponse().getStatus() != Protocol.Status.SUCCESS) {
            log.error("{} RegistrationRequest returned error: {}", getNodeName(), registrationResponse.getRegistrationResponse().getErrorMessage());
        } else {
            log.info("{} RegistrationRequest returned RegistrationResponse with status SUCCESS", getNodeName());
        }
    }

    public Protocol.Message processMessage(Protocol.Message message) {
        log.info("{} processing {}", getNodeName(), message.getType().toString());

        Protocol.Message.Builder response = Protocol.Message.newBuilder();

        switch (message.getType()) {
            case LOCAL_SEARCH_REQUEST:
                Protocol.LocalSearchResponse localSearchResponse = processLocalSearchRequest(message.getLocalSearchRequest());
                response.setType(Protocol.Message.Type.LOCAL_SEARCH_RESPONSE);
                response.setLocalSearchResponse(localSearchResponse);
                break;
            case SEARCH_REQUEST:
                Protocol.SearchResponse searchResponse = processSearchRequest(message.getSearchRequest());
                response.setType(Protocol.Message.Type.SEARCH_RESPONSE);
                response.setSearchResponse(searchResponse);
        }

        return response.build();
    }

    private Protocol.LocalSearchResponse processLocalSearchRequest(Protocol.LocalSearchRequest localSearchRequest) {
        try {
            Pattern pattern = Pattern.compile(localSearchRequest.getRegex());

            List<Protocol.FileInfo> matchingFiles = storedFiles.entrySet().stream()
                    .filter(entry -> pattern.matcher(entry.getKey()).find())
                    .map(entry -> entry.getValue().getFileInfo())
                    .collect(Collectors.toList());

            return Protocol.LocalSearchResponse.newBuilder()
                    .setStatus(Protocol.Status.SUCCESS)
                    .addAllFileInfo(matchingFiles)
                    .build();

        } catch (PatternSyntaxException e) {
            return Protocol.LocalSearchResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("The request regexp is invalid")
                    .build();
        } catch (Exception e) {
            return Protocol.LocalSearchResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing LocalSearchRequest")
                    .build();
        }
    }

    private Protocol.SearchResponse processSearchRequest(Protocol.SearchRequest searchRequest) {
        try {
//            Pattern pattern = Pattern.compile(searchRequest.getRegex());
            // get peers using SubnetRequest
            Protocol.SubnetResponse subnetResponse = sendSubnetRequest(searchRequest.getSubnetId());
            List<Protocol.NodeId> peers = subnetResponse.getNodesList();
            peers.removeIf(peer -> nodeId.getHost().equals(peer.getHost()) && nodeId.getPort() == peer.getPort());

            // get LocalSearchResponse from current node
            Protocol.LocalSearchRequest crtNodeLocalSearchRequest = Protocol.LocalSearchRequest.newBuilder()
                    .setRegex(searchRequest.getRegex())
                    .build();
            Protocol.LocalSearchResponse crtNodeLocalSearchResponse = processLocalSearchRequest(crtNodeLocalSearchRequest);
            if (Protocol.Status.MESSAGE_ERROR.equals(crtNodeLocalSearchResponse.getStatus())) {
                return Protocol.SearchResponse.newBuilder()
                        .setStatus(Protocol.Status.MESSAGE_ERROR)
                        .setErrorMessage("The request regexp is invalid")
                        .build();
            }

            Protocol.NodeSearchResult crtNodeSearchResult = Protocol.NodeSearchResult.newBuilder()
                    .setNode(nodeId)
                    .setStatus(crtNodeLocalSearchResponse.getStatus())
                    .addAllFiles(crtNodeLocalSearchResponse.getFileInfoList())
                    .build();

            // get results from peer nodes
            List<Protocol.NodeSearchResult> nodeSearchResults = peers.parallelStream()
                    .map(peerNode -> sendLocalSearchRequest(peerNode, searchRequest.getRegex()))
                    .collect(Collectors.toList());

            return Protocol.SearchResponse.newBuilder()
                    .setStatus(Protocol.Status.SUCCESS)
                    .addResults(crtNodeSearchResult)
                    .addAllResults(nodeSearchResults)
                    .build();

        } catch (PatternSyntaxException e) {
            return Protocol.SearchResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("The request regexp is invalid")
                    .build();
        } catch (Exception e) {
            return Protocol.SearchResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing SearchRequest")
                    .build();
        }
    }

    private Protocol.SubnetResponse sendSubnetRequest(int subnetId) {
        Protocol.SubnetRequest subnetRequest = Protocol.SubnetRequest.newBuilder()
                .setSubnetId(subnetId)
                .build();

        Protocol.Message message = Protocol.Message.newBuilder()
                .setType(Protocol.Message.Type.SUBNET_REQUEST)
                .setSubnetRequest(subnetRequest)
                .build();

        try {
            Protocol.Message response = networkHandler.sendRequestAndReceiveResponse(message, hubId.getHost(), hubId.getPort());
            return response.getSubnetResponse();
        } catch (Exception e) {
            return Protocol.SubnetResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing SubnetRequest")
                    .build();
        }
    }

    private Protocol.NodeSearchResult sendLocalSearchRequest(Protocol.NodeId peerNode, String regex) {
        Protocol.LocalSearchRequest localSearchRequest = Protocol.LocalSearchRequest.newBuilder()
                .setRegex(regex)
                .build();

        Protocol.Message message = Protocol.Message.newBuilder()
                .setType(Protocol.Message.Type.LOCAL_SEARCH_REQUEST)
                .setLocalSearchRequest(localSearchRequest)
                .build();

        try {
            Protocol.Message response = networkHandler.sendRequestAndReceiveResponse(message, nodeId.getHost(), nodeId.getPort());

            if (!Protocol.Message.Type.LOCAL_SEARCH_RESPONSE.equals(response.getType())) {
                return Protocol.NodeSearchResult.newBuilder()
                        .setNode(peerNode)
                        .setStatus(Protocol.Status.MESSAGE_ERROR)
                        .setErrorMessage("Incorrect response type")
                        .build();
            }

            return Protocol.NodeSearchResult.newBuilder()
                    .setNode(peerNode)
                    .setStatus(response.getLocalSearchResponse().getStatus())
                    .addAllFiles(response.getLocalSearchResponse().getFileInfoList())
                    .build();
        } catch (IOException e) {
            return Protocol.NodeSearchResult.newBuilder()
                    .setNode(peerNode)
                    .setStatus(Protocol.Status.NETWORK_ERROR)
                    .setErrorMessage("Could not connect to node")
                    .build();
        }
    }

    private String getNodeName() {
        return nodeId.getOwner() + "-" + nodeId.getIndex();
    }
}
