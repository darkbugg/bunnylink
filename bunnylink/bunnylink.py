# -*- coding: utf-8 -*-
import logging
import pika
import sys
import threading
import time
import json
import Queue
import datetime
import uuid


if pika.__version__ != '0.10.0':
    raise Exception("This package only works with pika version '0.10.0'")

LOG_FORMAT = ('%(asctime)s %(levelname) -10s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOG_FORMAT)


class Consumer(threading.Thread):

    def __init__(self, url, queue, exchange, amqp_queue, routing_key, reply_to, control_queue=None, log_level=logging.DEBUG):
        super(Consumer, self).__init__()
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        """

        self._exchange = exchange
        self._exchange_type = 'topic'
        self._amqp_queue = amqp_queue

        if isinstance(routing_key, list):
            self._routing_keys = routing_key
        else:
            self._routing_keys = [routing_key]

        # if we have a reply_to we also bind our channel to the reply_to topic.
        # RPC servers have to publish this replies to this topic so that our RPC client
        # is able to receive them.
        # Make sure the reply_to is unique inside the exchange or else RPC replies
        # will end up God knows where
        self._reply_to = reply_to
        if self._reply_to is not None:
            self._routing_keys.append(self._reply_to)

        # each time we bind a key icrement this. when all keys have been bound,
        # proceed
        self._bound_keys = 0

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = url
        self._queue = queue
        self._control_queue = control_queue

        self._logger = logging.getLogger('CONSUMER')
        self._logger.setLevel(log_level)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        self._logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     on_open_callback=self.on_connection_open,
                                     # on_open_error_callback=self.on_open_error_callback,
                                     stop_ioloop_on_close=False)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self._logger.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self._logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self.signal_online(False)
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                                 reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self._logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()
        if not self._closing:

            self._bound_keys = 0

            # Create a new connection
            self._connection = self.connect()
            self._logger.info("got a connection. now proceeding ...")
            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self._logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self.signal_online(False)
        self._logger.warning('Channel %i was closed: (%s) %s',
                             channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self._logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self._exchange_type)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self._logger.info('Exchange declared')
        self.setup_queue(self._amqp_queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self._logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        for rk in self._routing_keys:
            self._logger.info('Binding %s to %s with %s',
                              self._exchange, self._amqp_queue, rk)
            self._channel.queue_bind(self.on_bindok, self._amqp_queue,
                                     self._exchange, rk)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self._logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self._logger.info('Consumer was cancelled remotely, shutting down: %r',
                          method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self._logger.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        self._logger.info('Received message # %s from %s: %s',
                          basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)
        self._queue.put_nowait(
            (unused_channel, basic_deliver, properties, body))

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self._logger.info(
            'RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        self.signal_online(False)
        if self._channel:
            self._logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.signal_online(True)

        self._logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self._amqp_queue)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self._bound_keys += 1
        self._logger.info('Queue bound')
        if(self._bound_keys == len(self._routing_keys)):
            self._logger.info('All keys bound. Start consuming.')
            self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self._logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self._logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        try:
            self._connection = self.connect()
            self._connection.ioloop.start()
        except:
            self._logger.error(
                "Unexpected error. Probably RabbitMQ is unavailable. Consumer died.")
            self.signal_online(False)

    def signal_online(self, status):
        """Tell our manager object if we are online or not"""
        if self._control_queue:
            self._control_queue.put_nowait(status)

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        if hasattr(self._connection, 'ioloop'):
            self._connection.ioloop.start()
        self._logger.info('Stopped')


class Publisher(threading.Thread):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """

    def __init__(self, url, queue, exchange, publish_interval=0.01, reconnect_timeout=5, app_id='set_app_id', control_queue=None, log_level=logging.DEBUG):
        super(Publisher, self).__init__()
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param str amqp_url: The URL for connecting to RabbitMQ

        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        self._url = url
        self._queue = queue
        self._exchange = exchange
        self._exchange_type = 'topic'
        self._publish_interval = publish_interval
        self._reconnect_timeout = reconnect_timeout
        self._app_id = app_id
        self._control_queue = control_queue

        self._logger = logging.getLogger('PUBLISHER')
        self._logger.setLevel(log_level)

    def signal_online(self, status):
        """Tell our manager object if we are online or not"""
        if self._control_queue:
            self._control_queue.put_nowait(status)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """
        self._logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     on_open_callback=self.on_connection_open,
                                     on_close_callback=self.on_connection_closed,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self._logger.info('Connection opened')
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        # publisher is now offline and can not send messages
        self.signal_online(False)

        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in %s seconds: (%s) %s',
                                 self._reconnect_timeout, reply_code, reply_text)
            self._connection.add_timeout(
                self._reconnect_timeout, self._connection.ioloop.stop)

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        self._logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self._logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self._logger.warning('Channel was closed: (%s) %s',
                             reply_code, reply_text)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self._logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self._exchange_type)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self._logger.info('Exchange declared')
        # self.setup_queue(self._amqp_queue)
        self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        # publisher is now online and can start publishing messages from the
        # input queue
        self._logger.info('Start publishing')
        self.signal_online(True)

        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        self._logger.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        self._logger.debug('Received %s for delivery tag: %i',
                           confirmation_type,
                           method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._logger.debug('Published %i messages, %i have yet to be confirmed, '
                           '%i were acked and %i were nacked',
                           self._message_number, len(self._deliveries),
                           self._acked, self._nacked)

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        self._logger.debug('Scheduling next message for %0.1f seconds',
                           self._publish_interval)
        self._connection.add_timeout(self._publish_interval,
                                     self.publish_message)

    def publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._channel is None or not self._channel.is_open:
            return

        try:
            payload_obj = self._queue.get_nowait()
            routing_key = payload_obj.routing_key
            self._queue.task_done()
        except Queue.Empty:
            self.schedule_next_message()
            return

        if routing_key is None:
            self._logger.error("Routing key was empty")
            self._logger.info(payload_obj)
            self.schedule_next_message()
            return

        properties = pika.BasicProperties(app_id=self._app_id,
                                          content_type='application/json',
                                          # reply_to='callback_queue' # if using the rpc pattern
                                          # correlation_id='correlation_id_of_caller'
                                          # headers=message # this brakes
                                          # publishing of large messages
                                          )

        self._channel.basic_publish(self._exchange, routing_key,
                                    payload_obj.to_json(),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        self._logger.info('Published message # %i : %s',
                          self._message_number, payload_obj.to_json())

        self.schedule_next_message()

    def run(self):
        try:
            while not self._stopping:
                self._connection = None
                self._deliveries = []
                self._acked = 0
                self._nacked = 0
                self._message_number = 0

                try:
                    self._connection = self.connect()
                    self._connection.ioloop.start()
                except KeyboardInterrupt:
                    self.stop()
                    if (self._connection is not None and
                            not self._connection.is_closed):
                        # Finish closing
                        self._connection.ioloop.start()

            self._logger.info('Stopped')
        except:
            self._logger.error(
                'Unexpected error. Probably RabbitMQ is unavailable. Publisher died.', exc_info=True)
            self.signal_online(False)

    def stop(self):
        self._logger.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None:
            self._logger.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            self._logger.info('Closing connection')
            try:
                self._connection.close()
            except AssertionError:
                self._logger.error(
                    "Could not close connection cleanly. Maybe RabbitMQ is not accessible?")
                self._connection.ioloop.stop()


class OfflineError(Exception):
    """Raised when you try to send a message while the publisher is offline"""

    pass


class Client(threading.Thread):

    def __init__(self, url, to_rabbitmq, from_rabbitmq, exchange, amqp_queue,
                 routing_key, reply_to=None,
                 app_id='app_id__not_set', log_limit=0,
                 logger_name="RABBITMQ_CLIENT",
                 log_level_publisher=logging.WARNING, log_level_consumer=logging.WARNING, log_level=logging.INFO,
                 ):
        """
        Args:
            url:                    Url used to connect to RabbitMQ
                                    Example url:
                                        amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=1&heartbeat=30&retry_delay=10&socket_timeout=3

            to_rabbitmq:            Queue.Queue() instance used to send jobs to the rabbitmq publisher

            from_rabbitmq:          Queue.Queue() instance used to receive messages from the rabbitmq consumer

            exchange:               String. Exchange to connect to

            amqp_queue:             String. Queue to declare

            routing_key:            String or Array of String:
                                    Examples:
                                        "routing_key"
                                        ["rk1", "rk2", "rk3"] - will subscribe to all this ro uting keys in specified channel

            reply_to:               String. Will bind queue to this additional topic. Use this topic when dispatching RPC requests
                                    as the reply_to field. This topic should be unique for the entire exchange so that RPC
                                    replies do not end up beeing delivered to multiple queues

            app_id:                 String. Used in the publish BasicProperties to identify this app

            log_limit:              Integer. If len(body) is greater than this, than we do not log the body to the logs. If 0, disable

            logger_name:            String. The name to give to our internal logger

            log_level_publisher:    log_level for the publisher

            log_level_consumer:     log_level for the consumer

            log_level:              log_level for self

        """
        # reconnect attempts
        self._attempts = 0

        super(Client, self).__init__()
        self._url = url

        self._log_limit = log_limit
        self._reply_to = reply_to

        self._inbound = from_rabbitmq
        self._consumer_control_queue = Queue.Queue()
        self._can_receive = False

        self._outbound = to_rabbitmq
        self._publisher_control_queue = Queue.Queue()
        self._can_send = False

        self._stopping = False
        self._app_id = app_id

        self.exchange = exchange
        self.amqp_queue = amqp_queue

        self._publisher_args = dict(
            url=self._url, queue=self._outbound, exchange=exchange,
            app_id=self._app_id, control_queue=self._publisher_control_queue, log_level=log_level_publisher
        )
        self._publisher = Publisher(**self._publisher_args)
        self._publisher.setDaemon(True)

        self._consumer_args = dict(
            url=self._url, queue=self._inbound,
            exchange=exchange, amqp_queue=amqp_queue,
            routing_key=routing_key, reply_to=reply_to, control_queue=self._consumer_control_queue,
            log_level=log_level_consumer)
        self._consumer = Consumer(**self._consumer_args)
        self._consumer.setDaemon(True)

        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(log_level)

    def _check_threads(self):

        publisher_restarted, consumer_restarted = False, False

        if not self._publisher.isAlive():
            self._logger.info('Publisher is dead')
            self._publisher = Publisher(**self._publisher_args)
            self._publisher.setDaemon(True)
            self._publisher.start()
            publisher_restarted = True

        if not self._consumer.isAlive():
            self._logger.info('Consumer is dead')
            self._consumer = Consumer(**self._consumer_args)
            self._consumer.setDaemon(True)
            self._consumer.start()
            consumer_restarted = True

        to_check = [
            (self._publisher_control_queue, '_can_send', 'Publisher'),
            (self._consumer_control_queue, '_can_receive', 'Consumer'),
        ]

        # check status of publisher and consumer and set local properties
        # self._can_send and self._can_receive according to child thread
        for queue, attrname, name in to_check:
            try:
                # see if any status updates are available from our publisher or consumer
                # do this in a while so we consume all status updates posted to the worker
                # queue
                while True:
                    # get status updates from our worker (publisher and consumer)
                    # and reflect them onto local variables
                    setattr(self, attrname, queue.get_nowait())
                    attrval = getattr(self, attrname)
                    if attrval is True:
                        self._logger.warning("%s is now online" % name)
                    else:
                        self._logger.warning("%s is now offline" % name)
            except Queue.Empty:
                pass

        return publisher_restarted, consumer_restarted

    def run(self):
        while self._stopping is False:
            publisher_restarted, consumer_restarted = self._check_threads()
            reconnect_time = 10
            if publisher_restarted and consumer_restarted:

                if self._attempts > 0:
                    self._logger.error(
                        'Connetion problems. Attempting to reconnect in {}s'.format(reconnect_time))
                    time.sleep(reconnect_time)

                self._attempts += 1
                continue

            # Process received messages
            try:
                unused_channel, basic_deliver, properties, body = self._inbound.get(
                    True, 0.1)
                self._inbound.task_done()

                # log message if it is not too large
                if self._log_limit and (len(body) < self._log_limit):
                    self._logger.info("Received message {}".format(body))

                try:
                    self._on_message(
                        unused_channel, basic_deliver, properties, body)
                except NotImplementedError:
                    self._logger.error("on_message handler not implemented")
                    raise
                except:
                    self._logger.error(
                        "on_message raised unknown exception", exc_info=True)
            except Queue.Empty:
                pass

        self._logger.info("Stopping the subthreads")
        self._publisher.stop()
        self._consumer.stop()
        self._logger.info('Joining subthreads')
        self._publisher.join(3)
        self._consumer.join(3)
        self._logger.info('Subthreads exited')

    def _on_message(self, unused_channel, basic_deliver, properties, body):
        try:
            payload = Payload.from_json(body)
        except Exception as e:
            self._logger.error(str(e))
            self._logger.warning(
                "Could not decode payload object. Going to call 'on_message' instead of 'on_payload'")
            return self.on_message(unused_channel, basic_deliver, properties, body)

        return self.on_payload(unused_channel, basic_deliver, properties, payload)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """This is called when the body can not be decoded into a Payload object"""
        raise NotImplementedError(
            "implement the 'on_message' method that processes raw text messages.")

    def on_payload(self, unused_channel, basic_deliver, properties, payload):
        """This is called when the body can be decoded into a Payload object"""
        raise NotImplementedError(
            "implement the 'on_payload' method. The last argument is a Payload object.")

    def send(self, payload, log_payload=True):
        """
        Args:
            payload:    instance of the Payload class. Do not attempt to send plain string
                        in here because the underlaying publisher thread expects a Payload object

            log_payload: Boolean. If false then the payload will not be logged. Use when sending huge payloads

        Raises:
            OfflineError: if it can not send the message. The message will be discarded in this case
        """
        if log_payload:
            self._logger.info("Sending message %s", payload.to_json())
        else:
            self._logger.info(
                "Sending message *** message body not shown ***")

        if self._can_send:
            payload.published_at = datetime.datetime.now()
            self._outbound.put_nowait(payload)
        else:
            raise OfflineError(
                'publisher is not able to process messages at this time')

    def stop(self):
        self._logger.info('Stopping')
        self._stopping = True


class RPCClient(Client):
    """
    Extend the Client with RPC functionality
    This adds the query method which is used to dispatch a query to rpc servers
    and tracking of the issued and resolved queries
    """

    def __init__(self, *args, **kwargs):
        super(RPCClient, self).__init__(*args, **kwargs)

        # we store the unanswered queries here ( Payload objects )
        self._unanswered_queries = {}

    def on_payload(self, unused_channel, basic_deliver, properties, payload):
        """
        This method matches the reply to our original query and only
        dispatches the on_reply message if the reponse can be matched to a
        query issued by our client
        """
        self._logger.info("received Payload %s" % payload.to_json())

        # if the routing key equals our reply_to
        # it means this is a RPC reply destined for our client
        if payload.routing_key == self._reply_to:

            # get the original query we made for this corellation_id
            query_hint = self._unanswered_queries.get(payload.corellation_id)

            # if we can't find a query issued by us with this correlation_id
            # then there may be a problem
            if query_hint is None:
                self._logger.warning(
                    "Received RPC reply %s does not match an ongoing query" % payload.corellation_id)
                return

            del self._unanswered_queries[payload.corellation_id]

            return self.handle_reply(unused_channel, basic_deliver, properties, payload, query_hint)

        # If we got to this point it means that this was not a RPC reply.
        # either it is an erroneous message that somehow got to us or it is
        # a valid non RPC-reply message
        return self.handle_payload(unused_channel, basic_deliver, properties, payload)

    def handle_payload(self, unused_channel, basic_deliver, properties, payload):
        """
        This is called when we receive a normal Payload that could not be matched
        to a RPC query and we should treat it like a normal Payload message
        """
        raise NotImplementedError(
            "you need to implement the handle_payload method")

    def handle_reply(self, unused_channel, basic_deliver, properties, reply, query_hint):
        """
        This is called when a RPC response arives and if the response could be matched
        to an original query issued by our client

        Args:
            unused_channel:     pika object
            basic_deliver:      pika object
            properties:         pika object
            reply:              Payload object. Represents the RCP server response that
                                has returned to us
            query_hint:         This is the data that we specified as query_hint
                                when we dispatched this query using the rpc client's
                                query() method
        """
        raise NotImplementedError(
            "you need to implement the handle_reply method")

    def query(self, payload, query_hint=True):
        """
        Args:
            payload:            Payload object. this method will automatically add a reply_to and corellation_id to the
                                payload object before dispatching
            query_hint:         Whatever is provided here will be used to log into the _unanswered_queries dict.
                                You may wish to only provide "True" in here so that space is saved and once the
                                reply comes back, the client is able to find the key and confirm that it did issue
                                the initial query.
                                If required you can pass the entire original payload object or whatever is usefull
                                for processing the answer once it comes back from the server

        Use this method to dispatch RPC queries
        The method will automatically set the proper reply_to and create a corellation_id for the
        payload to dispatch.

        Then it will add it to the collection of unanswered queries and
        """
        payload.reply_to = self._reply_to
        payload.corellation_id = str(uuid.uuid4())

        # we save the query hint and the datetime at which we dispatched this
        # message in our ongoing query book
        self._unanswered_queries[payload.corellation_id] = dict(
            dispatched_at=datetime.datetime.now(), hint=query_hint)

        self._logger.debug("Unanswered queries: %s" %
                           len(self._unanswered_queries.keys()))
        self.send(payload)


class PayloadError(Exception):
    pass


class Payload(dict):

    __slots__ = ('routing_key', 'exchange', 'message_type', 'body', 'app_meta',
                 'app', 'host', 'published_at', 'reply_to', 'corellation_id')

    # Dictionary powers activate:
    __getitem__ = dict.get

    # Will use this format when encoding/decoding datetime objects
    _dt_format = "%Y-%m-%dT%H:%M:%S.%f"

    @classmethod
    def from_json(cls, message_json):
        try:
            decoded = json.loads(message_json)
        except ValueError:
            logging.error("Could not decode json")
            raise PayloadError("Could not decode json message")
        except:
            raise PayloadError(
                "Unknown error when trying to build Payload from json")

        # convert the datetime string into an actual datetime
        try:
            decoded['published_at'] = datetime.datetime.strptime(
                decoded['published_at'], cls._dt_format)
        except:
            raise PayloadError(
                "Could not process published_at for this payload")

        return cls(**decoded)

    def __init__(self, **kwargs):

        creation_errors = []

        # routing key on which the message was published
        self.routing_key = kwargs.get('routing_key')
        if self.routing_key is None:
            creation_errors.append('Message payload is missing "routing_key"')

        # the exchange to which this message will be published
        self.exchange = kwargs.get('exchange')
        if self.exchange is None:
            creation_errors.append('Message payload is missing "exchange"')

        # a descriptor for this message. string. consumers can take different
        # actions according to this descriptor
        self.message_type = kwargs.get('message_type')

        # the actual body of the message
        self.body = kwargs.get('body')
        if self.body is None:
            creation_errors.append('Message payload is missing "body"')

        # extra information about the app that publishes this message. For example, if it is a python script it may publish
        # the absolute path to the script
        self.app_meta = kwargs.get('app_meta')
        if self.app_meta is None:
            creation_errors.append(
                'Message payload is missing "app_meta". Detailed information about publishing app.')

        # name of the app that is publishing this message
        self.app = kwargs.get('app')
        if self.app is None:
            creation_errors.append(
                'Message payload is missing "app". Name of the publishing application.')

        # hostname of the publishing node
        self.host = kwargs.get('host')
        if self.host is None:
            creation_errors.append(
                'Message payload is missing "host". IP or hostname of the publishing application.')

        # at what time was this message published. ISO 8601: Example:
        # 2007-04-06T21:35.12.000000
        self.published_at = kwargs.get('published_at')
        # if self.published_at is None:
        #     creation_errors.append(
        #         'Message payload is missing "published_at". ISO 8601 (ex 2007-04-06T21:35.123) datetime representing the moment this message was published.')

        # the queue to which replies are expected in case of a RPC call
        self.reply_to = kwargs.get('reply_to')

        # the corellation id which should be included in the response in case
        # of a RPC call
        self.corellation_id = kwargs.get('corellation_id')

        if len(creation_errors):
            raise PayloadError(creation_errors)

    def to_json(self):
        try:
            return json.dumps({k: self._get_as_string(k) for k in self.__slots__ if getattr(self, k) is not None}, ensure_ascii=False)
        except:
            logging.error("Could not json serialize Payload", exc_info=True)

    def _get_as_string(self, attribute_name):
        attribute_val = getattr(self, attribute_name)
        """convert attributes to their string form"""

        if attribute_name == 'published_at':
            return attribute_val.strftime(self._dt_format)

        return attribute_val
