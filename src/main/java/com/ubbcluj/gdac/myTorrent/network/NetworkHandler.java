package com.ubbcluj.gdac.myTorrent.network;

import com.ubbcluj.gdac.myTorrent.communication.Protocol;
import com.ubbcluj.gdac.myTorrent.processor.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NetworkHandler {

    private static final Logger log = LoggerFactory.getLogger(NetworkHandler.class);

    //    private String host;
    private int port;
    private Node node;

    public NetworkHandler(String host, int port, Node node) {
//        this.host = host;
        this.port = port;
        this.node = node;
    }

    public void listenForRequests() {
        log.info("Waiting for requests");
        while (true) {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                new Thread(receiveRequestAndSendResponse(serverSocket.accept())).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Runnable receiveRequestAndSendResponse(Socket socket) {
        return () -> {
            try (DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())
            ) {
                Protocol.Message request = receiveMessage(dataInputStream);
                Protocol.Message response = node.processMessage(request);
                sendMessage(dataOutputStream, response);
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    public Protocol.Message sendRequestAndReceiveResponse(Protocol.Message message, String destinationHost, int destinationPort) throws IOException {
        log.info("Sent {} to {}", message.getType(), destinationPort);
        Protocol.Message response = null;

        try (Socket socket = new Socket(destinationHost, destinationPort);
             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())
        ) {
            sendMessage(dataOutputStream, message);
            response = receiveMessage(dataInputStream);
        }
        return response;
    }

    private Protocol.Message receiveMessage(DataInputStream dataInputStream) throws IOException {
        int messageSize = dataInputStream.readInt();
        byte[] byteBuffer = new byte[messageSize];
        int readMessageSize = dataInputStream.read(byteBuffer, 0, messageSize);

        if (messageSize != readMessageSize) {
            throw new RuntimeException("Network message has incorrect size: expected = " + messageSize + ", actual = " + readMessageSize);
        }

        Protocol.Message message = Protocol.Message.parseFrom(byteBuffer);
        log.info("Received {}", message.getType());

        return message;
    }

    private void sendMessage(DataOutputStream dataOutputStream, Protocol.Message message) throws IOException {
        byte[] serializedMessage = message.toByteArray();
        dataOutputStream.writeInt(serializedMessage.length);
        dataOutputStream.write(serializedMessage);
    }
}
