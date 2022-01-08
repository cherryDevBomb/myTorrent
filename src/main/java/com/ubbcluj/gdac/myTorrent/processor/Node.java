package com.ubbcluj.gdac.myTorrent.processor;

import com.google.protobuf.ByteString;
import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.model.File;
import com.ubbcluj.gdac.myTorrent.model.MessageErrorException;
import com.ubbcluj.gdac.myTorrent.network.NetworkHandler;
import com.ubbcluj.gdac.myTorrent.util.FileInfoUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class Node implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private final Protocol.NodeId nodeId;
    private final Protocol.NodeId hubId;

    private final NetworkHandler networkHandler;
    private final FileInfoUtil fileInfoUtil;
    private final Map<String, File> storedFiles;

    public Node(Protocol.NodeId nodeId, Protocol.NodeId hubId) {
        this.nodeId = nodeId;
        this.hubId = hubId;
        this.networkHandler = new NetworkHandler(nodeId.getHost(), nodeId.getPort(), this);
        this.fileInfoUtil = new FileInfoUtil();
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
                break;
            case UPLOAD_REQUEST:
                Protocol.UploadResponse uploadResponse = processUploadRequest(message.getUploadRequest());
                response.setType(Protocol.Message.Type.UPLOAD_RESPONSE);
                response.setUploadResponse(uploadResponse);
                break;
            case DOWNLOAD_REQUEST:
                Protocol.DownloadResponse downloadResponse = processDownloadRequest(message.getDownloadRequest());
                response.setType(Protocol.Message.Type.DOWNLOAD_RESPONSE);
                response.setDownloadResponse(downloadResponse);
                break;
            case REPLICATE_REQUEST:
                Protocol.ReplicateResponse replicateResponse = processReplicateRequest(message.getReplicateRequest());
                response.setType(Protocol.Message.Type.REPLICATE_RESPONSE);
                response.setReplicateResponse(replicateResponse);
                break;
            case CHUNK_REQUEST:
                Protocol.ChunkResponse chunkResponse = processChunkRequest(message.getChunkRequest());
                response.setType(Protocol.Message.Type.CHUNK_RESPONSE);
                response.setChunkResponse(chunkResponse);
                break;
            default:
                log.error("Incorrect message type {}", message.getType());
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
            // get peers using SubnetRequest
            List<Protocol.NodeId> peers = getPeerNodes(searchRequest.getSubnetId());

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

    private Protocol.UploadResponse processUploadRequest(Protocol.UploadRequest uploadRequest) {
        if (StringUtils.isEmpty(uploadRequest.getFilename())) {
            return Protocol.UploadResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("The filename is empty")
                    .build();
        }

        try {
            if (!storedFiles.containsKey(uploadRequest.getFilename())) {
                Protocol.FileInfo fileInfo = fileInfoUtil.getFileInfoFromUploadRequest(uploadRequest);
                storedFiles.put(uploadRequest.getFilename(), new File(fileInfo, uploadRequest.getData().toByteArray()));
            }

            return Protocol.UploadResponse.newBuilder()
                    .setStatus(Protocol.Status.SUCCESS)
                    .setFileInfo(storedFiles.get(uploadRequest.getFilename()).getFileInfo())
                    .build();
        } catch (Exception e) {
            return Protocol.UploadResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing UploadRequest")
                    .build();
        }
    }

    private Protocol.DownloadResponse processDownloadRequest(Protocol.DownloadRequest downloadRequest) {
        if (downloadRequest.getFileHash().toByteArray().length != 16) {
            return Protocol.DownloadResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("File hash has incorrect size")
                    .build();
        }

        try {
            Optional<File> storedFile = fileInfoUtil.findFileByMD5Hash(storedFiles, downloadRequest.getFileHash().toByteArray());
            if (storedFile.isPresent()) {
                return Protocol.DownloadResponse.newBuilder()
                        .setStatus(Protocol.Status.SUCCESS)
                        .setData(ByteString.copyFrom(storedFile.get().getFileContent()))
                        .build();
            } else {
                return Protocol.DownloadResponse.newBuilder()
                        .setStatus(Protocol.Status.UNABLE_TO_COMPLETE)
                        .setErrorMessage("File not found for download")
                        .build();
            }
        } catch (Exception e) {
            return Protocol.DownloadResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing DownloadRequest")
                    .build();
        }
    }

    private Protocol.ReplicateResponse processReplicateRequest(Protocol.ReplicateRequest replicateRequest) {
        if (StringUtils.isEmpty(replicateRequest.getFileInfo().getFilename())) {
            return Protocol.ReplicateResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("The filename is empty")
                    .build();
        }

        try {
            // get peers using SubnetRequest
            List<Protocol.NodeId> peers = getPeerNodes(replicateRequest.getSubnetId());

            // get file chunks from other nodes
            int numberOfChunks = replicateRequest.getFileInfo().getChunksList().size();
            List<Protocol.ChunkRequest> chunkRequests = new ArrayList<>();
            for (int chunkId = 0; chunkId < numberOfChunks; chunkId++) {
                Protocol.ChunkRequest chunkRequest = Protocol.ChunkRequest.newBuilder()
                        .setFileHash(replicateRequest.getFileInfo().getHash())
                        .setChunkIndex(chunkId)
                        .build();
                chunkRequests.add(chunkRequest);
            }

            final Map<Integer, List<Protocol.NodeReplicationStatus>> replicationStatusesMap = new ConcurrentHashMap<>();
            final Map<Integer, Protocol.ChunkResponse> foundChunks = new ConcurrentHashMap<>();
            ExecutorService executor = Executors.newFixedThreadPool(50);
            for (Protocol.ChunkRequest chunkRequest : chunkRequests) {
                executor.submit(() -> {
                    List<Protocol.NodeReplicationStatus> currentChunkStatuses = new ArrayList<>();
                    Protocol.Status lastNodeStatus = null;
                    int counter = 0;

                    // send chunk request to nodes until the chunk is found or all nodes were queried
                    while (lastNodeStatus != Protocol.Status.SUCCESS && counter++ < peers.size()) {
                        int destinationPeerIndex = (chunkRequest.getChunkIndex() + counter) % peers.size();
                        Protocol.NodeId destinationPeer = peers.get(destinationPeerIndex);

                        // convert ChunkResponse to NodeReplicationStatus
                        Protocol.ChunkResponse currentPeerResponse = null;
                        Protocol.NodeReplicationStatus currentPeerStatus;
                        try {
                            currentPeerResponse = sendChunkRequest(destinationPeer, chunkRequest);
                            currentPeerStatus = Protocol.NodeReplicationStatus.newBuilder()
                                    .setNode(destinationPeer)
                                    .setChunkIndex(chunkRequest.getChunkIndex())
                                    .setStatus(currentPeerResponse.getStatus())
                                    .build();
                        } catch (MessageErrorException e) {
                            currentPeerStatus = Protocol.NodeReplicationStatus.newBuilder()
                                    .setNode(destinationPeer)
                                    .setChunkIndex(chunkRequest.getChunkIndex())
                                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                                    .setErrorMessage("Incorrect response type")
                                    .build();
                        } catch (IOException e) {
                            currentPeerStatus = Protocol.NodeReplicationStatus.newBuilder()
                                    .setNode(destinationPeer)
                                    .setChunkIndex(chunkRequest.getChunkIndex())
                                    .setStatus(Protocol.Status.NETWORK_ERROR)
                                    .setErrorMessage("Could not connect to node")
                                    .build();
                        }

                        currentChunkStatuses.add(currentPeerStatus);
                        lastNodeStatus = currentPeerStatus.getStatus();

                        // save ChunkResponse if it was found
                        if (lastNodeStatus == Protocol.Status.SUCCESS && currentPeerResponse != null) {
                            foundChunks.put(chunkRequest.getChunkIndex(), currentPeerResponse);
                        }
                    }
                    replicationStatusesMap.put(chunkRequest.getChunkIndex(), currentChunkStatuses);
                });
            }

            // wait for executor to finish processing chunk requests
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            // compute the list of all statuses from all nodes
            List<Protocol.NodeReplicationStatus> replicationStatusList = replicationStatusesMap.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            // check if all chunks were found
            boolean allChunksFound = replicationStatusesMap.values().stream()
                    .map(chunkStatuses -> chunkStatuses.stream()
                            .filter(status -> Protocol.Status.SUCCESS == status.getStatus())
                            .findFirst())
                    .allMatch(Optional::isPresent);

            if (!allChunksFound) {
                return Protocol.ReplicateResponse.newBuilder()
                        .setStatus(Protocol.Status.UNABLE_TO_COMPLETE)
                        .setErrorMessage("Missing chunks for ReplicateRequest")
                        .addAllNodeStatusList(replicationStatusList)
                        .build();
            }

            // replication success
            byte[] replicatedFileContent = fileInfoUtil.buildFileFromChunks(foundChunks);
            storedFiles.put(replicateRequest.getFileInfo().getFilename(), new File(replicateRequest.getFileInfo(), replicatedFileContent));

            return Protocol.ReplicateResponse.newBuilder()
                    .setStatus(Protocol.Status.SUCCESS)
                    .addAllNodeStatusList(replicationStatusList)
                    .build();

        } catch (Exception e) {
            return Protocol.ReplicateResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing ReplicateRequest")
                    .build();
        }
    }

    private Protocol.ChunkResponse processChunkRequest(Protocol.ChunkRequest chunkRequest) {
        if (chunkRequest.getFileHash().toByteArray().length != 16) {
            return Protocol.ChunkResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("File hash has incorrect size")
                    .build();
        }
        if (chunkRequest.getChunkIndex() < 0) {
            return Protocol.ChunkResponse.newBuilder()
                    .setStatus(Protocol.Status.MESSAGE_ERROR)
                    .setErrorMessage("ChunkIndex is less than zero")
                    .build();
        }

        try {
            Optional<File> storedFile = fileInfoUtil.findFileByMD5Hash(storedFiles, chunkRequest.getFileHash().toByteArray());
            if (storedFile.isPresent()) {
                byte[] chunkData = fileInfoUtil.extractChunkFromFileContent(storedFile.get().getFileContent(), chunkRequest.getChunkIndex());
                return Protocol.ChunkResponse.newBuilder()
                        .setStatus(Protocol.Status.SUCCESS)
                        .setData(ByteString.copyFrom(chunkData))
                        .build();
            } else {
                return Protocol.ChunkResponse.newBuilder()
                        .setStatus(Protocol.Status.UNABLE_TO_COMPLETE)
                        .setErrorMessage("Chunk not found")
                        .build();
            }
        } catch (Exception e) {
            return Protocol.ChunkResponse.newBuilder()
                    .setStatus(Protocol.Status.PROCESSING_ERROR)
                    .setErrorMessage("Error processing ChunkRequest")
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
            Protocol.Message response = networkHandler.sendRequestAndReceiveResponse(message, peerNode.getHost(), peerNode.getPort());

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

    private Protocol.ChunkResponse sendChunkRequest(Protocol.NodeId peerNode, Protocol.ChunkRequest chunkRequest) throws MessageErrorException, IOException {
        Protocol.Message message = Protocol.Message.newBuilder()
                .setType(Protocol.Message.Type.CHUNK_REQUEST)
                .setChunkRequest(chunkRequest)
                .build();

        Protocol.Message response = networkHandler.sendRequestAndReceiveResponse(message, peerNode.getHost(), peerNode.getPort());

        if (!Protocol.Message.Type.CHUNK_RESPONSE.equals(response.getType())) {
            throw new MessageErrorException("Incorrect response type for ChunkRequest");
        }

        return response.getChunkResponse();
    }

    private List<Protocol.NodeId> getPeerNodes(int subnetId) {
        Protocol.SubnetResponse subnetResponse = sendSubnetRequest(subnetId);
        List<Protocol.NodeId> peers = new ArrayList<>(subnetResponse.getNodesList());
        peers.removeIf(peer -> nodeId.getHost().equals(peer.getHost()) && nodeId.getPort() == peer.getPort());
        return peers;
    }

    private String getNodeName() {
        return nodeId.getOwner() + "-" + nodeId.getIndex();
    }
}
