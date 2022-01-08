package com.ubbcluj.gdac.myTorrent.util;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.model.File;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

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

    public Optional<File> findFileByMD5Hash(Map<String, File> storedFiles, byte[] md5Hash) {
        return storedFiles.values().stream()
                .filter(f -> Arrays.equals(f.getFileInfo().getHash().toByteArray(), md5Hash))
                .findFirst();
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
        int numberOfChunks = (int) Math.ceil((double) bytes.length / CHUNK_SIZE);

        List<Protocol.ChunkInfo> chunks = new ArrayList<>();
        for (int chunkIndex = 0; chunkIndex < numberOfChunks; chunkIndex++) {
            byte[] chunkBytes = extractChunkFromFileContent(bytes, chunkIndex);
            chunks.add(Protocol.ChunkInfo.newBuilder()
                    .setIndex(chunkIndex)
                    .setSize(chunkBytes.length)
                    .setHash(ByteString.copyFrom(getMD5(chunkBytes)))
                    .build());
        }
        return chunks;
    }

    public byte[] buildFileFromChunks(Map<Integer, Protocol.ChunkResponse> foundChunks) {
        return Bytes.toArray(foundChunks.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getValue().getData())
                .map(ByteString::toByteArray)
                .map(Bytes::asList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

    public byte[] extractChunkFromFileContent(byte[] byteContent, int chunkIndex) {
        int start = chunkIndex * CHUNK_SIZE;
        int end = Math.min(byteContent.length, (chunkIndex + 1) * CHUNK_SIZE);

        return Arrays.copyOfRange(byteContent, start, end);
    }
}
