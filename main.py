import subprocess
import threading
import producer
import consumer

def zookeeper_start():
    zookeeper_command = '.\\kafka\\bin\\windows\\zookeeper-server-start.bat'
    zookeeper_config = '.\\kafka\\config\\zookeeper.properties'

    def read_output():
        for line in process.stdout:
            print(line.decode('utf-8'), end='')

    try:
        process = subprocess.Popen([zookeeper_command, zookeeper_config], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, shell=True)
        #output_thread = threading.Thread(target=read_output)
        #output_thread.start()
        #process.wait()
    except Exception as e:
        print("zookeeper execution error: ", str(e))
def kafka_server_start():
    kafka_server_command = '.\\kafka\\bin\\windows\\kafka-server-start.bat'
    kafka_server_config = '.\\kafka\\config\\server.properties'
    try:
        process = subprocess.Popen([kafka_server_command, kafka_server_config], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
    except Exception as e:
        print("kafka server starting error:", str(e))

zookeeper_tread = threading.Thread(target=zookeeper_start).start()
print("zookeeper started successfully.")
kafka_server_tread = threading.Thread(target=kafka_server_start).start()
print("kafka server started successfully.")

producer.producer_start(transactions_number=15)
consumer.consumer_start()
