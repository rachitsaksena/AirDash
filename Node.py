import zmq
import time
from bigchaindb_driver import BigchainDB

id = 1

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

# TODO: change bdb url
bdb_root_url = 'https://thicchaindb.lunchb0ne.me:443'
bdb = BigchainDB(bdb_root_url)

with open('keys.json') as file:
    keys = json.load(file)

while True:
    packet = socket.recv_pyobj()
    print(f"Received request: {packet}")
    if packet['type']  == 'create_task':
        task = {
            'data': {
                'created by' : id,
                'type' : packet['task']['type'],
                'time' : time.time(),
                'description' : packet['task']['description'],
                'listener' : str(id)+str(time.time())
            }
        }
        metadata = { 'notes' : f'This task was created by cam_{id} at {time.ctime(time.time())}'}
        prepared_creation_tx = bdb.transactions.prepare(
            operation = 'CREATE',
            signers = keys['public']['this'],
            asset = task,
            metadata = metadata
        )
        fulfilled_creation_tx = bdb.transactions.fulfill(prepared_creation_tx, private_keys=keys['private'])
        sent_creation_tx = bdb.transactions.send_commit(fulfilled_creation_tx)
        asset_id = fulfilled_creation_tx['id']
        # block_height = bdb.blocks.get(txid=signed_tx['id'])
        # while block_height == None:
        #     block_height = bdb.blocks.get(txid=signed_tx['id'])
        output_index = 0
        output = fulfilled_creation_tx['outputs'][output_index]
        transfer_input = {
            'fulfillment' : output['condition']['details'],
            'fulfills' : {
                'output_index' : output_index,
                'transaction_id' : fulfilled_creation_tx['id']
            },
            'owners_before' : output['public_keys'],
        }
        prepared_transfer_tx = bdb.transactions.prepare(
            operation='TRANSFER',
            asset=transfer_asset,
            inputs=transfer_input,
            recipients=keys['public']['maintenance_worker'],
        )
        fulfilled_transfer_tx = bdb.transactions.fulfill(
            prepared_transfer_tx,
            private_keys=keys['private'],
        )
        sent_transfer_tx = bdb.transactions.send_commit(fulfilled_transfer_tx)
    elif packet['type'] == 'data':
        data = {
            'data':{
                'created by' : id,
                'type' : packet['data']['type'],
                'value' : packet['data']['values'],
                'density' : packet['data']['density'],
                'listener' : str(id) + str(time.time())
            }
        }
        metadata = { 'notes' : f'This data was created by cam_{id} at {time.ctime(time.time())}'}
        prepared_creation_tx = bdb.transactions.prepare(
            operation = 'CREATE',
            signers = keys['public']['this'],
            asset = data,
            metadata = metadata
        )
        sent_creation_tx = bdb.transactions.send_commit(fulfilled_creation_tx)
        asset_id = fulfilled_creation_tx['id']
        # block_height = bdb.blocks.get(txid=signed_tx['id'])
        # while block_height == None:
        #     block_height = bdb.blocks.get(txid=signed_tx['id'])
        output_index = 0
        output = fulfilled_creation_tx['outputs'][output_index]
        transfer_input = {
            'fulfillment' : output['condition']['details'],
            'fulfills' : {
                'output_index' : output_index,
                'transaction_id' : fulfilled_creation_tx['id']
            },
            'owners_before' : output['public_keys'],
        }
        prepared_transfer_tx = bdb.transactions.prepare(
            operation='TRANSFER',
            asset=transfer_asset,
            inputs=transfer_input,
            recipients=keys['public']['server'],
        )
        fulfilled_transfer_tx = bdb.transactions.fulfill(
            prepared_transfer_tx,
            private_keys=keys['private'],
        )
        sent_transfer_tx = bdb.transactions.send_commit(fulfilled_transfer_tx)
            
    resp = {
        "response" : "OK"
    }
     socket.send_pyobj(resp)