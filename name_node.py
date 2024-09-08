import socket
import pickle
import os
import logging
import random
import math
import traceback
import shutil  
from flask import Flask, request, jsonify
import time

# Import shared constants
from common import NAME_NODE_PORT

def setup_logging():
    # Set up logging to a file and console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        handlers=[
            logging.FileHandler("namenode_log.txt"),  # Log to a file
            logging.StreamHandler()  # Log to the console
        ]
    )

setup_logging()

class NameNode:
    def __init__(self):
        # In-memory storage for metadata
        self.metadata = {}
        self.blocks = {}

        # Dictionary to track active DataNodes and their last heartbeat times
        self.active_data_nodes = {}

        # Start the server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', NAME_NODE_PORT))
        self.server_socket.listen()

        print("NameNode is listening for socket connections.")

    def handle_client(self, client_socket):
        request = client_socket.recv(1024)
        if request:
            try:
                request_data = pickle.loads(request)
                action = request_data.get('action')
                if action == 'create_directory':
                    response = self.create_directory(request_data['parent_path'], request_data['directory_name'])
                elif action == 'upload_file':
                    response = self.upload_file(request_data['local_path'], request_data['dfs_path'])
                elif action == 'download_file':
                    response = self.download_file(request_data['dfs_path'], request_data['local_path'])
                elif action == 'delete_directory':
                    response = self.delete_directory(request_data['directory_path'])
                elif action == 'move_file':
                    response = self.move_file(request_data['source_path'], request_data['destination_path'])
                elif action == 'register':
                    self.register_data_node(request_data)
                    response = 'Registration successful'
                elif action == 'status':
                    response = self.get_status()
                elif action == 'list_directory_contents':
                    response = self.list_directory_contents(request_data['dfs_path'])
                elif action == 'traverse_directory':
                    response = self.traverse_directory(request_data['dfs_path'])
                else:
                    response = 'Invalid action'
            except Exception as e:
                response = f"Error processing request: {e}"
        else:
            response = 'Empty request'

        if response is None:
            response = 'Error: No valid response'

        client_socket.sendall(str(response).encode())
        client_socket.close()

    def register_data_node(self, registration_data):
        # Implement logic to register a DataNode with the NameNode
        data_node_id = registration_data.get('data_node_id')
        data_node_address = registration_data.get('data_node_address')
        data_node_port = registration_data.get('data_node_port')

        # Example: Store DataNode information in the 'metadata' dictionary
        self.metadata[data_node_id] = {
            'address': data_node_address,
            'port': data_node_port,
            'status': 'active'
        }

        logging.info(f"DataNode {data_node_id} registered with NameNode")

    def create_directory(self, parent_path, directory_name):
        # Implement the logic to create a directory
        directory_path = os.path.join(parent_path, directory_name)
        try:
            os.makedirs(directory_path)
            return f"Directory '{directory_name}' created successfully."
        except OSError as e:
            return f"Error creating directory '{directory_name}': {e}"

    def upload_file(self, local_path, dfs_path):
        # Step 1: File Splitting
        block_size = 1024  # Choose an appropriate block size
        block_count = int((os.path.getsize(local_path) + block_size - 1) / block_size)

        # Step 2: Block Creation
        block_ids = list(range(block_count))

        # Step 3: Uploading Blocks
        for block_id in block_ids:
            with open(local_path, "rb") as file:
                file.seek(block_id * block_size)
                block_data = file.read(block_size)

            # Step 3: Uploading Blocks
            data_nodes = self.get_data_nodes()

            if not data_nodes:
                return "Error: No available data nodes."

            data_node_id = block_id % len(data_nodes)
            data_node_address = data_nodes[data_node_id][1]

            # Upload block to the DataNode
            self.store_block(block_id, block_data, dfs_path, data_node_id)

            # Step 4: Replication
            replication_factor = 2
            self.replicate_block(block_id, dfs_path, replication_factor, data_node_id)

            # Step 5: Metadata Update
            block_size = 1024  # Same as the block size chosen earlier
            file_metadata = {
                'size': os.path.getsize(local_path),
                'parent_folder': os.path.dirname(dfs_path),
                'block_size': block_size,
                'data_nodes': [node[0] for node in data_nodes],
            }
            self.store_file_metadata(local_path, dfs_path, file_metadata)

            # Step 6: Store block information in 'blocks' dictionary
            self.store_file_blocks(local_path, dfs_path, block_id, data_node_id)

        return f"File '{local_path}' uploaded to DFS path '{dfs_path}' successfully."

    def download_file(self, dfs_path, local_path):
        # Step 1: Check Metadata
        file_metadata = self.metadata.get(dfs_path)
        if file_metadata:
            print(f"Step 1: Metadata found for file '{dfs_path}'")
            try:
                # Step 2: Calculate Block Count
                block_count = math.ceil(file_metadata['size'] / file_metadata['block_size'])
                block_ids = list(range(block_count))
                print(f"Step 2: Calculated block count: {block_count}")

                # Step 3: Data Block Retrieval
                data_nodes = self.get_data_nodes()
                print(f"Step 3: Total reachable data nodes: {len(data_nodes)}")

                for block_id in block_ids:
                    data_node_id = block_id % len(data_nodes)
                    data_node_address = data_nodes[data_node_id][1]

                    # Step 4: Loop Over Blocks
                    print(f"Step 4: Retrieving Block {block_id} from DataNode {data_node_id}")

                    # Retrieve block from the DataNode
                    block_data = self.read_block(block_id, dfs_path, data_node_id)

                    # Step 5: Write to Local File
                    with open(local_path, "ab") as file:
                        file.write(block_data)

                    print(f"Step 5: Appended Block {block_id} data to local file")

                # Step 6: Logging
                logging.info(f"File '{dfs_path}' downloaded to local path '{local_path}' successfully.")
                print("Download process completed successfully.")
            except Exception as e:
                traceback.print_exc()
                logging.error(f"Error downloading file '{dfs_path}': {e}")
                print(f"Error: {e}")
        else:
            logging.error(f"File '{dfs_path}' not found in DFS.")
            print(f"Error: File '{dfs_path}' not found in DFS.")

    def delete_directory(self, directory_path):
        # Implement logic to delete a directory and its contents
        try:
            os.rmdir(directory_path)
            return f"Directory '{directory_path}' deleted successfully."
        except OSError as e:
            return f"Error deleting directory '{directory_path}': {e}"

    def move_file(self, source_path, destination_path):
        # Implement logic to move a file to another directory
        try:
            os.rename(source_path, destination_path)
            return f"File '{source_path}' moved to '{destination_path}' successfully."
        except FileNotFoundError:
            return f"Error: File '{source_path}' not found."
        except OSError as e:
            return f"Error moving file '{source_path}' to '{destination_path}': {e}"

    def get_data_nodes(self):
        # Implement logic to retrieve available and reachable data nodes
        reachable_data_nodes = [node for node in self.metadata.items() if 'status' in node[1] and node[1]['status'] == 'active']
        if not reachable_data_nodes:
            print("No reachable data nodes.")
        else:
            print(f"Total reachable data nodes: {len(reachable_data_nodes)}")

        return reachable_data_nodes

    def store_block(self, block_id, block_data, dfs_path, data_node_id):
        # Implement logic to store a block on a DataNode
        self.blocks[(dfs_path, block_id)] = {
            'data': block_data,
            'data_node_id': data_node_id
        }

        logging.info(f"Block {block_id} stored on DataNode {data_node_id}")

    def replicate_block(self, block_id, dfs_path, replication_factor, source_data_node_id):
        # Implement logic to replicate a block to other DataNodes
        data_nodes = self.get_data_nodes()
        if len(data_nodes) < replication_factor:
            logging.error("Insufficient data nodes for replication.")
            return

        # Choose replication_factor - 1 random data nodes (excluding the source)
        target_data_nodes = [node for node in data_nodes if node[0] != source_data_node_id]
        target_data_nodes = random.sample(target_data_nodes, min(replication_factor - 1, len(target_data_nodes)))

        # Retrieve block data from the source DataNode
        source_block_data = self.blocks.get((dfs_path, block_id), {}).get('data', b'')

        # Replicate the block to the selected target DataNodes
        for target_data_node in target_data_nodes:
            target_data_node_id = target_data_node[0]
            self.store_block(block_id, source_block_data, dfs_path, target_data_node_id)

        logging.info(f"Block {block_id} replicated to {len(target_data_nodes)} DataNodes")

    def store_file_metadata(self, local_path, dfs_path, file_metadata):
        # Implement logic to store file metadata in the 'metadata' dictionary
        self.metadata[dfs_path] = file_metadata

        logging.info(f"Metadata for {dfs_path} stored in the 'metadata' dictionary")
        print(f"Contents of 'metadata' dictionary after storing metadata for {dfs_path}: {self.metadata}")

    def store_file_blocks(self, local_path, dfs_path, block_id, data_node_id):
        # Implement logic to store block information in 'blocks' dictionary
        self.blocks[(dfs_path, block_id)]["data_node_id"] = data_node_id

        logging.info(f"Block {block_id} information stored in the 'blocks' dictionary")

    
    def read_block(self, block_id, dfs_path, data_node_id):
        
        # Implement the actual logic to read a block from a DataNode
        return self.blocks.get((dfs_path, block_id), {}).get('data', b'')

    def get_status(self):
        # Implement logic to retrieve and return the status of the DFS
        
        return "DFS Status: OK"

    def list_directory_contents(self, dfs_path):
        # Implement logic to list the contents of a directory in the DFS
        directory_contents = []
        for key, metadata in self.metadata.items():
            if key.startswith(dfs_path) and key != dfs_path:
                relative_path = key[len(dfs_path) + 1:]
                components = relative_path.split(os.path.sep)
                if len(components) == 1:
                    # It's a file, not a subdirectory
                    directory_contents.append(components[0])
                else:
                    # It's a subdirectory
                    subdirectory = components[0]
                    if subdirectory not in directory_contents:
                        directory_contents.append(subdirectory)

        return directory_contents

    def traverse_directory(self, dfs_path):
        # Implement logic to traverse the directory structure in the DFS
        directory_structure = {}
        for key, metadata in self.metadata.items():
            if key.startswith(dfs_path) and key != dfs_path:
                relative_path = key[len(dfs_path) + 1:]
                components = relative_path.split(os.path.sep)
                current_dict = directory_structure

                for component in components[:-1]:
                    current_dict = current_dict.setdefault(component, {})

                current_dict[components[-1]] = {
                    'size': metadata['size'],
                    'block_size': metadata['block_size'],
                    'data_nodes': metadata['data_nodes'],
                }

        return directory_structure

    def run(self):
        while True:
            try:
                client_socket, _ = self.server_socket.accept()
                self.handle_client(client_socket)
                self.check_data_node_heartbeats()  # Check DataNode heartbeats after handling each client request
            except KeyboardInterrupt:
                print("Shutting down the NameNode.")
                break
            except Exception as e:
                logging.error(f"Error in run loop: {e}")

    def check_data_node_heartbeats(self):
        # Check and update the status of active DataNodes
        current_time = time.time()
        inactive_data_nodes = []

        for data_node_id, last_heartbeat_time in self.active_data_nodes.items():
            if current_time - last_heartbeat_time > 10:  # Adjust the threshold as needed
                inactive_data_nodes.append(data_node_id)

        # Handle inactive DataNodes (remove or mark them as inactive)
        for data_node_id in inactive_data_nodes:
            logging.warning(f"DataNode {data_node_id} is inactive.")
            # You may implement further actions here, such as re-replicating blocks stored on the inactive DataNode.

    def send_heartbeat(self, data_node_id):
        # Update the last heartbeat time for the given DataNode
        self.active_data_nodes[data_node_id] = time.time()

if __name__ == "__main__":
    namenode = NameNode()
    namenode.run()
