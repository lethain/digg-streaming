#!/usr/bin/env python
"""
This file shows an example of listening to the Digg Streaming API
and loading the events into Redis.

## Examples

Running from the command line:

    python stream.py
    python stream.py comment digg
    python stream.py --host 127.0.0.1 --db 4 --port 6379
    python stream.py submission --db 6

Running from Python:

    import digg_stream
    event_handler = digg_stream.make_handle_event("localhost", 6379, 0)
    digg_stream.stream(event_handler, ["digg"])

The easiest way to extend the existing script is to write your own event_handler,
which could do just about anything you want: write it into CouchDB, MongoDB,
MySQL, a flat file. Really, whatever you want.

## Kudos

Implementation is Inspired in [this article][1].

[1]: http://arstechnica.com/open-source/guides/2010/04/tutorial-use-twitters-new-real-time-stream-api-in-python.ars
"""
import pycurl, json, logging, time, redis
from optparse import OptionParser
logging.basicConfig(level=logging.INFO)

DIGG_SERVICE_URL = "services.digg.com"
LEGAL_EVENTS = set(['digg', 'comment', 'submission'])
STREAM_LOGGER = logging.getLogger("digg_stream.stream")
FAILURE_FORGIVENESS = 60 * 60
SMALL_BACKOFF = 15.0
LARGE_BACKOFF = SMALL_BACKOFF * 4
HUGE_BACKOFF = LARGE_BACKOFF * 10
BUFFER = ""

class InvalidEventTypeException(Exception):
    pass

class InvalidEventException(Exception):
    pass

def make_handle_event(redis_host, redis_port, redis_db):
    "Load an event into Redis."
    redis_cli = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    def handle_event(serialized_event):
        STREAM_LOGGER.debug(serialized_event)
        try:
            event = json.loads(serialized_event)
            redis_cli.lpush("%s" % (event['type'],), serialized_event)
        except (ValueError, TypeError), e:
            STREAM_LOGGER.warn("Received invalid event: %s\n%s" % (e, serialized_event))
    return handle_event

def make_event_buffer(event_handler):
    "Generate an event bufferer."
    def event_buffer(data):
        "Parse events from the data stream."
        global BUFFER
        BUFFER += data
        try:
            json.loads(BUFFER)
            event_handler(BUFFER)
            BUFFER = ""
        except ValueError:
            pass
    return event_buffer

def start_streaming(event_handler, event_types=list(LEGAL_EVENTS)):
    "Initialize streaming from the Digg Streaming API."
    for event_type in event_types:
        if event_type not in LEGAL_EVENTS:
            error_msg = "%s is not a legal event type." % (event_type,)
            raise InvalidEventTypeException(error_msg)

    failures = 0
    last_failure = time.time()
    while True:
        try:
            conn = pycurl.Curl()
            conn.setopt(pycurl.URL, "http://%s/2.0/stream?format=json&types=%s" % (DIGG_SERVICE_URL, ",".join(event_types)))
            conn.setopt(pycurl.WRITEFUNCTION, make_event_buffer(event_handler))
            conn.perform()
        except Exception, e:
            STREAM_LOGGER.error(e)
            now = time.time()
            if now - last_failure > FAILURE_FORGIVENESS:
                failures = 0
            last_failure = now
            failures += 1
            if failures > 10:
                time.sleep(HUGE_BACKOFF)
            elif failures > 2:
                time.sleep(LARGE_BACKOFF)
            elif failures == 2:
                time.sleep(SMALL_BACKOFF)
            
            

def main():
    "Initialize consumption of Digg Streaming API."
    p = OptionParser("usage: stream.py digg comment submission  --host 127.0.0.1 --port 6379, --db 0")
    p.add_option('-H', '--host', dest='host', help="HOST for Redis", metavar="HOST", default="localhost")
    p.add_option('-p', '--port', dest='port', help="HOST for Redis", metavar="HOST", type="int", default=6379)
    p.add_option('-d', '--db', dest='db', help="DB for Redis", metavar="DB", type="int", default=0)
    (options, args) = p.parse_args()
    event_types = args or list(LEGAL_EVENTS)
    event_handler = make_handle_event(options.host, options.port, options.db)
    start_streaming(event_handler, event_types)

if __name__ == "__main__":
    main()
