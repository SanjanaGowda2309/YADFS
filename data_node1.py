import socket
import pickle
import logging
import time
import os

from common import DATA_NODE_PORT_1, NAME_NODE_ADDRESS, NAME_NODE_PORT

def setup_logging():
    # Set up logging to a file and console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        handlers=[
            logging.FileHandler("datanode_log.txt"),  # Log to a file
            logging.StreamHandler()  # Log to the console
        ]
    )

setup_logging()

class DataNode:
    def __init__(self, data_node_id, data_directory):
        self.data_node_id = data_node_id
        self.data_directory = data_directory

        # Set up the server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', DATA_NODE_PORT_1))
        self.server_socket.listen()

        # Connect to the NameNode
        self.name_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.name_node_socket.connect((NAME_NODE_ADDRESS, NAME_NODE_PORT))

        # Register with the NameNode
        self.register_with_name_node()

    def register_with_name_node(self):
        registration_data = {
            'action': 'register',
            'data_node_id': self.data_node_id,
            'data_node_address': 'localhost',
            'data_node_port': DATA_NODE_PORT_1,
        }

        self.name_node_socket.sendall(pickle.dumps(registration_data))
        response = self.name_node_socket.recv(1024).decode()
        logging.info(response)

    def send_heartbeat(self):
        heartbeat_data = {
            'action': 'heartbeat',
            'data_node_id': self.data_node_id,
        }

        self.name_node_socket.sendall(pickle.dumps(heartbeat_data))

    def store_block(self, block_id, block_data):
        block_path = os.path.join(self.data_directory, f"block_{block_id}.dat")

        with open(block_path, 'wb') as block_file:
            block_file.write(block_data)

        logging.info(f"Block {block_id} stored locally.")

    def handle_requests(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            request = client_socket.recv(1024)
            if request:
                try:
                    request_data = pickle.loads(request)
                    action = request_data.get('action')
                    if action == 'store_block':
                        block_id = request_data['block_id']
                        block_data = request_data['block_data']
                        self.store_block(block_id, block_data)
                    else:
                        logging.warning('Invalid action received.')
                except Exception as e:
                    logging.error(f"Error processing request: {e}")
            client_socket.close()

    def run(self):
        while True:
            try:
                self.send_heartbeat()
                self.handle_requests()
                time.sleep(5)  # Adjust the sleep interval based on your requirements
            except KeyboardInterrupt:
                print("Shutting down the DataNode.")
                break
            except Exception as e:
                logging.error(f"Error in run loop: {e}")

if __name__ == "__main__":
    data_node = DataNode(data_node_id=1, data_directory="data_directory")
    data_node.run()
