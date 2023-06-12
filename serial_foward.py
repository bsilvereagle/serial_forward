#!/bin/env python3

"""
serial_fwd - a small utility to forward serial data packets over various network protocols.

At a high level, this utility reads ASCII serial data and stores any lines beginning with $ as JSON in a
multisubscriber queue. Publishers read from the queue and publish the JSON via MQTT, UDP, or websockets.

Additionally a prompt is shown to the user allowing writing ASCII serial data to the device.

"""

import asyncio
import datetime
import functools
import json
import logging
import signal
import socket
import time
import sys

import aioserial
import click

from asyncio_multisubscriber_queue import MultisubscriberQueue

logger = logging.getLogger(__name__)
mq = MultisubscriberQueue()


def make_async(f):
    """ Helper wrapper to run click commands async."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


def configure_logging(verbosity, log, log_file):
    """ Configure logging based on verbosity level.

    Select a logging level (WARNING, INFO, DEBUG based on a numeric indicator typically returned
    from argparse.add_argument(action='count') or click.option(count=True)

    logging.ERROR is the default value for no -v flag being passed

    If `log` is set, then logging is also written to `log_file`.

    """
    levels = [logging.ERROR, logging.WARNING,
              logging.INFO, logging.DEBUG]

    formatter = logging.Formatter(
        '%(asctime)s::%(levelname)s::%(name)s::%(message)s')

    log_level = levels[min(verbosity, len(levels) - 1)]

    if log:
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter('pctime:%(asctime)s,%(message)s'))
        logger.addHandler(fh)
    else:
        logger.setLevel(log_level)

    sh = logging.StreamHandler()
    sh.setLevel(log_level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)


def line_to_dict(line):
    """
    Convert a key:value CSV line in the Arduino Serial Plotter format to a dict.

    For example, "time:10,b:2,c:3" becomes {"time":10, "b":2.0, "c":3.0}

    https://github.com/arduino/Arduino/blob/master/build/shared/ArduinoSerialPlotterProtocol.md
    """

    tree = {}
    for pair in line.strip().split(','):
        t = tree
        key, value = pair.split(':')
        parts = key.split('/')
        for part in parts[:-1]:
            t = t.setdefault(part, {})
        if parts[-1] == "time":
            t[parts[-1]] = int(value)
        else:
            t[parts[-1]] = float(value)
    return tree


async def serial_read_coro(aios):
    """ Reads data from serial device aios and stores as JSON in a multisubscriber queue."""
    logger.info("Serial Read Coro Started")
    show_prompt = True
    while True:
        try:
            data = await aios.readline_async()
            logger.info('%s', data.decode())
            if data.decode().startswith('$'):
                try:
                    data_dict = line_to_dict(data.decode())
                    await mq.put(json.dumps(data_dict))
                    logger.debug(data.decode())
                except ValueError:
                    pass
            else:
                sys.stdout.write(data.decode())
                # await loop.run_in_executor(None, lambda: sys.stdout.write(aios.name + '> '))
                sys.stdout.flush()
        except TypeError:
            pass


async def serial_write_coro(aios):
    """Reads input from stdin and writes to aios serial device."""
    logger.info("Serial Write Coro Started")
    loop = asyncio.get_event_loop()
    while True:
        await loop.run_in_executor(None, lambda: sys.stdout.write(aios.name + '> '))
        sys.stdout.flush()
        data = await loop.run_in_executor(None, sys.stdin.readline)
        await aios.write_async(data.encode())


async def publish_udp_coro(sock, host, udp_port):
    """Publishes a JSON blob from the multisubscriber queue via UDP to host:udp_port."""
    logger.info("Publish UDP Coro Started")
    q = mq.add()  # Generate a new subscriber to the multisubscriber queue
    while True:
        item = await q.get()
        logger.debug('UDP Item: %s', item)
        sock.sendto(item.encode(), (host, udp_port))


async def publish_mqtt_coro(aiomqtt, host, port, client_id, transport, version, topic):
    """Publishes JSON blob as a MQTT message."""
    logger.info("Publish MQTT Coro Started")
    q = mq.add()
    async with aiomqtt.Client(hostname=host,
                              port=port,
                              client_id=client_id,
                              transport=transport,
                              protocol=version) as mqtt_client:
        while True:
            item = await q.get()
            logger.debug('MQTT Item: %s', item)
            await mqtt_client.publish(topic, item)


async def publish_ws_coro(websockets, ws_url):
    """Publish JSON via websocket."""
    q = mq.add()
    async with websockets.connect(ws_url) as websocket:
        while True:
            item = await q.get()
            logger.debug('WS Item: %s', item)
            await websocket.send(item)


CONTEXT_SETTINGS = {"help_option_names": [
    '-h', '--help'], "show_default": True, }

coroutines = set()


def sighup_handler(signum, frame, aios):
    """Sleep 10 seconds after a SIGHUP.

    This is arbitrarily chosen to give avrdude sufficient time to flash the device.
    """
    logger.info("Received HUP signal")
    aios.close()
    logger.info("Serial closed")
    time.sleep(10)
    logger.info("Reopening serial")
    aios.open()
    logger.info("Serial Opened")


def sigterm_handler(signum, frame):
    """Terminate the event loop on SIGTERM."""
    logger.error("SIGTERM")
    loop = asyncio.get_running_loop()
    loop.stop()
    loop.close()


@click.group("cli", chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', count=True,
              help='Verbosity level [WARNING, INFO, DEBUG]')
@click.option('-d', '--delimiter', default=',', help='CSV data delimiter')
@click.option('-s', '--separator', default=':', help='Label:Value separator')
@click.option('-p', '--port', default='/dev/ttyUSB0', help='Serial port')
@click.option('-b', '--baud', default=115200, help='Serial baud rate')
@click.option('-l', '--log', is_flag=True)
@click.option('--log-file', default=datetime.datetime.now().strftime('serial_%Y%m%d-%H%M%S.log'))
@click.version_option(version='0.1.0', message="%(version)s")
def cli(verbose, delimiter, separator, port, baud, log, log_file):
    """Click."""
    configure_logging(verbose, log, log_file)

    try:
        aios = aioserial.AioSerial(port=port, baudrate=baud)
    except aioserial.SerialException as e:
        logger.error(e)
        logger.error("Unable to open serial port %s", port)
        sys.exit(-1)
    coroutines.add(serial_read_coro(aios))
    coroutines.add(serial_write_coro(aios))

    handler = functools.partial(sighup_handler, aios=aios)
    signal.signal(signal.SIGHUP, handler)


@cli.result_callback()
@make_async
async def process_futures(*args, **kwargs):
    """Click callback.

    Exectues all of the coroutines that have been created via various click commands
    """
    logger.debug("Callback reached")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    tasks = [asyncio.create_task(coro) for coro in coroutines]

    await asyncio.gather(*tasks)


@cli.command("mqtt", context_settings=CONTEXT_SETTINGS)
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-P', '--mqtt-port', default=1883)
@click.option('-t', '--transport', type=click.Choice(["tcp", "websockets"]), default="tcp")
@click.option('-V', '--version', type=click.Choice(['3.1', '3.1.1', '5.0']), default='5.0')
@click.option('--topic', default="arduino")
def mqtt(host, mqtt_port, transport, version, topic):
    """Use MQTT as transport.

    This can be tested by using mosquitto as the MQTT broker in an insecure fashion:

    podman run -it -p 1883:1883 -p 9001:9001 -p 8080:8080 docker.io/eclipse-mosquitto sh -c "printf 'protocol websockets\nlistener 8080' >> /mosquitto-no-auth.conf && /usr/sbin/mosquitto -c /mosquitto-no-auth.conf"

    MQTTX can be used as a GUI to view the broadcast messages, or the PlotJuggler MQTT plugin can be used to plot data
    """
    try:
        import asyncio_mqtt as aiomqtt  # pylint: disable=import-outside-toplevel
        import paho.mqtt as pmqtt  # pylint: disable=import-outside-toplevel
    except ImportError:
        logger.error(
            'Python package `asyncio-mqtt` not found. Please install via \n\n\t`python3 -m pip install asyncio-mqtt`\n\nor use a different transport protocol.')

    VERSIONS = {'3.1': pmqtt.client.MQTTv31,
                '3.1.1': pmqtt.client.MQTTv311,
                '5.0': pmqtt.client.MQTTv5, }

    coroutines.add(publish_mqtt_coro(aiomqtt,
                                     host,
                                     mqtt_port,
                                     "serial_fwd",
                                     transport,
                                     VERSIONS[version],
                                     topic))


@cli.command("udp", context_settings=CONTEXT_SETTINGS)
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-P', '--udp-port', default=9870)
# @make_async
def udp(udp_port, host):
    """Use UDP as transport.

    Each line of serial data is broadcast as a single JSON packet over UDP.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logger.info('UDP Socket Created')

    coroutines.add(publish_udp_coro(sock, host, udp_port))


@cli.command("ws", context_settings=CONTEXT_SETTINGS)
@click.option('-u', '--ws-url', default="ws://127.0.0.1:9870")
def ws(ws_url):
    """Use Websockets as transport"""
    try:
        import websockets  # pylint: disable=import-outside-toplevel
    except ImportError:
        logger.error(
            'Python package `websockets` not found. Please install via\n\n\t`python3 -m pip install websockets`\n\nor use a different transport protocol.')
        sys.exit(1)
    coroutines.add(publish_ws_coro(websockets, ws_url))


if __name__ == '__main__':
    if len(sys.argv) == 1:
        cli.main(['--help'])
    else:
        cli()  # pylint: disable=no-value-for-parameter
