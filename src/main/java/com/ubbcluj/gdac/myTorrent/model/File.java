package com.ubbcluj.gdac.myTorrent.model;

import com.ubbcluj.gdac.myTorrent.communication.Protocol;

public class File {

    private Protocol.FileInfo fileInfo;
    private byte[] fileContent;

    public File(Protocol.FileInfo fileInfo, byte[] fileContent) {
        this.fileInfo = fileInfo;
        this.fileContent = fileContent;
    }

    public Protocol.FileInfo getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(Protocol.FileInfo fileInfo) {
        this.fileInfo = fileInfo;
    }

    public byte[] getFileContent() {
        return fileContent;
    }

    public void setFileContent(byte[] fileContent) {
        this.fileContent = fileContent;
    }
}
