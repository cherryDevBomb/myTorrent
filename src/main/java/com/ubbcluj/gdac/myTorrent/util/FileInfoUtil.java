package com.ubbcluj.gdac.myTorrent.util;

import com.google.protobuf.ByteString;
import com.ubbcluj.gdac.myTorrent.communication.Protocol;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileInfoUtil {

    private static final int CHUNK_SIZE = 1024;

    private MessageDigest md;

    public FileInfoUtil() {
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public byte[] getMD5(byte[] bytes) {
        byte[] hash = md.digest(bytes);
        md.reset();

        return hash;
    }

    public Protocol.FileInfo getFileInfoFromUploadRequest(Protocol.UploadRequest uploadRequest) {
        byte[] fileContent = uploadRequest.getData().toByteArray();

        return Protocol.FileInfo.newBuilder()
                .setHash(ByteString.copyFrom(getMD5(fileContent)))
                .setSize(fileContent.length)
                .setFilename(uploadRequest.getFilename())
                .addAllChunks(splitFileIntoChunks(fileContent))
                .build();
    }

    private List<Protocol.ChunkInfo> splitFileIntoChunks(byte[] bytes) {
        int numberOfChunks = (int) Math.ceil(bytes.length / CHUNK_SIZE);

        List<Protocol.ChunkInfo> chunks = new ArrayList<>();
        for (int chunkId = 0; chunkId < numberOfChunks; chunkId++) {
            int start = chunkId * numberOfChunks;
            int end = Math.min(bytes.length, (chunkId + 1) * CHUNK_SIZE);
            byte[] chunkBytes = Arrays.copyOfRange(bytes, start, end);

            chunks.add(Protocol.ChunkInfo.newBuilder()
                    .setIndex(chunkId)
                    .setSize(chunkBytes.length)
                    .setHash(ByteString.copyFrom(getMD5(chunkBytes)))
                    .build());
        }
        return chunks;
    }
}
