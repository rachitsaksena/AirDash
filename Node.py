import zmq
import datetime
from bigchaindb_driver import BigchainDB

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

# TODO: change bdb url
bdb_root_url = 'https://thicchaindb.lunchb0ne.me:9984'
bdb = BigchainDB(bdb_root_url)

with open('keys.json') as file:
    keys = json.load(file)

while True:
    packet = socket.recv_pyobj()
    print(f"Received request: {packet}")
    # TODO: create blockchain asset and send with the data
    if packet['type']  == 'create_task':
        task = {
            'data' : pocket['task']
        }
        metadata = { 'notes' : f'This task was created by cam_1 at {str(datetime.datetime.now())}'}
        prepared_creation_tx = bdb.transactions.prepare(
            operation = 'CREATE',
            signers = keys['public']['this'],
            asset = task,
            metadata = metadata
        )
        fulfilled_creation_tx = bdb.transactions.fulfill(prepared_creation_tx, private_keys=keys['private'])
        sent_creation_tx = bdb.transactions.send_commit(fulfilled_creation_tx)
        txid = fulfilled_creation_tx['id']
        block_height = bdb.blocks.get(txid=signed_tx['id'])
        while block_height == None:
            block_height = bdb.blocks.get(txid=signed_tx['id'])

    resp = {
        "response" : "OK"
    }
     socket.send_pyobj(resp)