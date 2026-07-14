import eventlet

eventlet.monkey_patch()  # must run before redis/socket imports

import json

import redis
from flask import Flask, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

r = redis.Redis(host='localhost', port=6380, decode_responses=True)


def redis_listener():
    """Forward fraud alerts from Redis pub/sub to the browser."""
    pubsub = r.pubsub()
    pubsub.subscribe('fraud_notifications')
    print("Listening on redis channel 'fraud_notifications'...")

    for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            alert = json.loads(message['data'])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Skipping unparseable alert: {e}")
            continue

        # one event per ring so the graph can draw each loop separately
        for ring in alert.get('rings', []):
            socketio.emit('fraud_alert', {'raw': message['data'], 'accounts': ring})


socketio.start_background_task(redis_listener)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
