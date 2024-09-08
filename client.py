from flask import Flask, render_template, request, jsonify
import socket
import pickle
import threading
import time

app = Flask(__name__)

class DFSClient:
    def __init__(self, name_node_address, name_node_port):
        self.name_node_address = name_node_address
        self.name_node_port = name_node_port

    def send_request(self, request_data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((self.name_node_address, self.name_node_port))
            serialized_request = pickle.dumps(request_data)
            client_socket.sendall(serialized_request)
            response = client_socket.recv(1024)
            return response.decode()

    def register_data_node(self, data_node_id, data_node_address, data_node_port):
        request_data = {
            'action': 'register',
            'data_node_id': data_node_id,
            'data_node_address': data_node_address,
            'data_node_port': data_node_port
        }
        return self.send_request(request_data)

    def create_directory(self, parent_path, directory_name):
        request_data = {
            'action': 'create_directory',
            'parent_path': parent_path,
            'directory_name': directory_name
        }
        return self.send_request(request_data)

    def delete_directory(self, directory_path):
        request_data = {
            'action': 'delete_directory',
            'directory_path': directory_path
        }
        return self.send_request(request_data)

    def move_file(self, source_path, destination_path):
        request_data = {
            'action': 'move_file',
            'source_path': source_path,
            'destination_path': destination_path
        }
        return self.send_request(request_data)

    def upload_file(self, local_path, dfs_path):
        request_data = {
            'action': 'upload_file',
            'local_path': local_path,
            'dfs_path': dfs_path
        }
        return self.send_request(request_data)

    def download_file(self, dfs_path, local_path):
        request_data = {
            'action': 'download_file',
            'dfs_path': dfs_path,
            'local_path': local_path
        }
        return self.send_request(request_data)

    def get_status(self):
        request_data = {
            'action': 'status'
        }
        return self.send_request(request_data)
    def list_directory_contents(self, dfs_path):
        request_data = {
            'action': 'list_directory_contents',
            'dfs_path': dfs_path
        }
        return self.send_request(request_data)

    def traverse_directory(self, dfs_path):
        request_data = {
            'action': 'traverse_directory',
            'dfs_path': dfs_path
        }
        return self.send_request(request_data)

    def send_heartbeat(self, data_node_id):
        request_data = {
            'action': 'heartbeat',
            'data_node_id': data_node_id
        }
        return self.send_request(request_data)


# Initialize the DFSClient
dfs_client = DFSClient(name_node_address='localhost', name_node_port=5007)  # Replace 12345 with your actual NameNode port
# Heartbeat function for the DataNode
def heartbeat(data_node_id, interval):
    while True:
        print(f"Heartbeat from DataNode {data_node_id}")
        time.sleep(interval)

# Example of starting the thread
data_node_id = 1
interval = 5
heartbeat_thread = threading.Thread(target=heartbeat, args=(data_node_id, interval))
heartbeat_thread.start()
# Example of starting the thread
data_node_id = 0
interval = 5
heartbeat_thread = threading.Thread(target=heartbeat, args=(data_node_id, interval))
heartbeat_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/register_data_node', methods=['POST'])
def register_data_node():
    data = request.form
    response = dfs_client.register_data_node(data['data_node_id'], data['data_node_address'], data['data_node_port'])
    return render_template('index.html', message=response)

@app.route('/create_directory', methods=['POST'])
def create_directory():
    data = request.form
    response = dfs_client.create_directory(data['parent_path'], data['directory_name'])
    return render_template('index.html', message=response)

@app.route('/delete_directory', methods=['POST'])
def delete_directory():
    data = request.form
    response = dfs_client.delete_directory(data['directory_path'])
    return render_template('index.html', message=response)

@app.route('/move_file', methods=['POST'])
def move_file():
    data = request.form
    response = dfs_client.move_file(data['source_path'], data['destination_path'])
    return render_template('index.html', message=response)

@app.route('/upload_file', methods=['POST'])
def upload_file():
    data = request.form
    response = dfs_client.upload_file(data['local_path'], data['dfs_path'])
    return render_template('index.html', message=response)

@app.route('/download_file', methods=['POST'])
def download_file():
    data = request.form
    response = dfs_client.download_file(data['dfs_path'], data['local_path'])
    return render_template('index.html', message=response)


@app.route('/get_status', methods=['GET'])
def get_status():
    response = dfs_client.get_status()
    return render_template('index.html', message=response)
    


@app.route('/traverse_directory', methods=['POST'])
def traverse_directory():
    data = request.get_json()
    response = dfs_client.traverse_directory(
        dfs_path=data['dfs_path']
    )
    return jsonify({"response": response})


    
@app.route('/list_directory_contents', methods=['POST'])
def list_directory_contents():
    data = request.get_json()
    response = dfs_client.list_directory_contents(
        dfs_path=data['dfs_path']
    )
    return jsonify({"response": response})
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # Perform any necessary logic to get heartbeat information
    heartbeat_info = {'status': 'OK'}  # Replace this with actual heartbeat information
    return jsonify(heartbeat_info)


if __name__ == '__main__':
    app.run(debug=True)
