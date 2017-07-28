from bunnylink import *


def test1():

    class Client1(Client):
        """Client that receives messages and dispatches messages to itslef"""

        def on_message(self, unused_channel, basic_deliver, properties, body):
            try:
                message = json.loads(body)
            except:
                self._logger.error('Could not json decode message')
                return

            self.maybe_log("Received message %s", message)
            try:
                self.send(message)
            except OfflineError:
                self._logger.warning(
                    'Message not dispatched because publisher is offline')

    class Queuer(threading.Thread):
        """Enqueue messages to the client each 20 seconds
        so that it can start to speak to rabbitmq
        """

        def __init__(self, client):
            super(Queuer, self).__init__()
            self.should_run = True
            self.client = client

        def run(self):
            while self.should_run:
                time.sleep(20)
                logging.info('adding message to queue')
                try:
                    self.client.send(
                        dict(body="message body here", routing_key='example.text'))
                except OfflineError:
                    logging.error(
                        "Publisher offline. Message could not be sent.")

        def stop(self):
            self.should_run = False

    URL = 'amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3'
    to_rabbitmq = Queue.Queue()
    from_rabbitmq = Queue.Queue()
    client = Client1(URL, to_rabbitmq=to_rabbitmq, from_rabbitmq=from_rabbitmq,
                     exchange='message', amqp_queue='text', routing_key=['example.text', 'rk2'], app_id='my_app', log_body=False)
    client.start()
    t1 = Queuer(client)
    t1.setDaemon(True)
    t1.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            client.stop()
            client.join()
            logging.info('client has stopped. now stopping enqueuer ...')
            t1.stop()
            t1.join()
            break


def test2():
    """A client speaks to itself using the RabbitMQ server"""
    import base64

    class Client1(Client):
        """Client that receives messages and dispatches messages to itslef"""

        def on_message(self, unused_channel, basic_deliver, properties, body):
            try:
                message = json.loads(body)
                self._logger.warning(len(message))
            except:
                self._logger.error('Could not json decode message')
                return

            self.maybe_log("Received message %s", message)
            try:
                self.send(message)
            except OfflineError:
                self._logger.warning(
                    'Message not dispatched because publisher is offline')

    class Queuer(threading.Thread):
        """Enqueue messages to the client each 20 seconds
        so that it can start to speak to rabbitmq
        """

        def __init__(self, client):
            super(Queuer, self).__init__()
            self.should_run = True
            self.client = client

        def run(self):
            while self.should_run:
                time.sleep(5)
                logging.info('adding message to queue')
                try:
                    with open('Archive.zip', 'rb') as f:
                        # 29104744
                        contents = base64.b64encode(f.read())
                        logging.info("message length: %s" % len(contents))
                        self.client.send(
                            dict(body=contents, routing_key='example.text'))
                except OfflineError:
                    logging.error(
                        "Publisher offline. Message could not be sent.")
                return

        def stop(self):
            self.should_run = False

    URL = 'amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3'
    to_rabbitmq = Queue.Queue()
    from_rabbitmq = Queue.Queue()
    client = Client1(URL, to_rabbitmq=to_rabbitmq, from_rabbitmq=from_rabbitmq,
                     exchange='message', amqp_queue='text', routing_key=['example.text', 'rk2'], app_id='my_app', log_body=False)
    client.start()
    t1 = Queuer(client)
    t1.setDaemon(True)
    t1.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            client.stop()
            client.join()
            logging.info('client has stopped. now stopping enqueuer ...')
            t1.stop()
            t1.join()
            break


def test3():
    """Test sending a huge file through the RabbitMQ server"""
    import base64

    class MyPublisher(Publisher):

        def publish_message(self):
            with open('Archive.zip', 'rb') as f:
                contents = base64.b64encode(f.read())
                # contents = contents[:150000]
                logging.info("message length: %s" % len(contents))

                properties = pika.BasicProperties(app_id=self._app_id,
                                                  content_type='application/json')

                self._channel.basic_publish(self._exchange, 'example.text',
                                            json.dumps(
                                                contents, ensure_ascii=False),
                                            properties)
                self._message_number += 1
                self._deliveries.append(self._message_number)
                logging.info('Published message')

    URL = 'amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3'
    publisher = MyPublisher(url=URL, queue=Queue.Queue(), exchange='message')
    publisher.start()


def test4():
    """Test the Payload object"""
    d1 = dict(published_at=datetime.datetime.now(),
              routing_key="test",
              exchange="exchange_name",
              host="hostname.here",
              app="app.name",
              app_meta="/path/to/app/script.py",
              message_type="type.of.message",
              corellation_id=None,  # corellation id used in RPC
              reply_to=None,        # channel to reply to if RPC
              body="this here is the body of the message")

    message = Payload(**d1)
    print message

    as_json = message.to_json()
    print as_json

    as_json_bad = as_json + "broken json"

    another = Payload.from_json(as_json_bad)
    print another


def test5():
    """Test subscribing to multiple routing keys"""
    to_rabbitmq = Queue.Queue()
    from_rabbitmq = Queue.Queue()
    URL = 'amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3'
    client = Client(URL, to_rabbitmq=to_rabbitmq, from_rabbitmq=from_rabbitmq,
                    exchange='message', amqp_queue='text', routing_key=['example.text', 'rk2'], app_id='my_app', log_body=False)
    client.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            client.stop()
            client.join()
            logging.info('client has stopped. now stopping enqueuer ...')
            break


def test6():
    """
    Test the RPC pattern

    Open a RPC client and a RPC server.
    The client dispatches a RPC request
    The server responds
    The client receives the response and matches it to it's original query hint.
    """
    URL = 'amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3'

    client_to_rabbitmq = Queue.Queue()
    client_from_rabbitmq = Queue.Queue()

    class SimpleRPCClient(RPCClient):

        def handle_reply(self, unused_channel, basic_deliver, properties, reply, query_hint):

            now = datetime.datetime.now()
            dt = now - query_hint['dispatched_at']

            self._logger.info("ok, got the reply in %s seconds" % dt)

    class RPCServer(Client):

        def on_payload(self, unused_channel, basic_deliver, properties, payload):
            self._logger.info("Received payload %s" % payload.to_json())

            reply = Payload(**dict(
                routing_key=payload.reply_to,
                exchange="message",
                host="hostname.here",
                app="app.name",
                app_meta="/path/to/app/script.py",
                message_type="type.of.message",
                corellation_id=payload.corellation_id,
                body="thi is the rpc response"
            ))

            return self.send(reply)

    # A RPC client will bind it's channel with a topic equal to the channel name and it will send this topic in the "reply_to"
    # field of the Payload() object so that the server knows with what topic
    # to publish the reponse
    rpc_client = SimpleRPCClient(URL, to_rabbitmq=client_to_rabbitmq, from_rabbitmq=client_from_rabbitmq,
                                 exchange='message', amqp_queue='rpc_client',
                                 routing_key=['rk_rpc_client', 'rk_rpc_client_response'], reply_to='rpc_client_response_here',
                                 app_id='rpc_client', logger_name="RPC_Client"
                                 # log_level_publisher=logging.INFO, log_level_consumer=logging.INFO
                                 )
    rpc_client.start()

    server_to_rabbitmq = Queue.Queue()
    server_from_rabbitmq = Queue.Queue()
    rpc_server = RPCServer(URL, to_rabbitmq=server_to_rabbitmq, from_rabbitmq=server_from_rabbitmq,
                           exchange='message', amqp_queue='rpc_server', routing_key='rk_rpc_server', app_id='rpc_client',
                           logger_name="RPC_Server"
                           # log_level_publisher=logging.INFO, log_level_consumer=logging.INFO
                           )
    rpc_server.start()

    time.sleep(5)
    logging.info("Now putting a message on the queue")

    payload = Payload(**dict(
        routing_key="rk_rpc_server",
        exchange="message",
        host="hostname.here",
        app="app.name",
        app_meta="/path/to/app/script.py",
        message_type="type.of.message",

        # these are not needed if using the query method
        # which adds this information automatically
        # corellation_id=5344,           # corellation id used in RPC
        # reply_to='rk_rpc_client_response',         #
        # channel to reply to if RPC

        body="this is the rpc query"
    ))

    while True:
        try:
            for i in xrange(100):
                try:
                    rpc_client.query(payload, query_hint=payload)
                except OfflineError as e:
                    logging.error(str(e))
                except Exception as e:
                    logging.error("Unhandled error while trying to send", exc_info=True)
                time.sleep(5)
        except KeyboardInterrupt:
            rpc_client.stop()
            rpc_server.stop()
            rpc_client.join()
            rpc_server.join()
            break


if __name__ == '__main__':
    # test1()
    # test5()
    # test3()
    # test4()
    test6()
