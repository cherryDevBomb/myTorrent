## myTorrent
🌊 BitTorrent distributed file system implementation using Protobuf

### Supported operations
* **Local search:** Search for filenames matching the regex on the node.
* **Global search:** Search all nodes for filenames matching the regex and aggregate the results.
* **Upload:** Store the given file locally on the node.
* **Download:** Download the specified chunk.
* **File replication:** Replicate a file locally, if not present already, asking for chunks from the other nodes.

