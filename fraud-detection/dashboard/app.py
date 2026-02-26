from flask import Flask, render_template
from flask_socketio import SocketIO
import redis
import eventlet  # Import eventlet

# IMPORTANT: Monkey patch to ensure background tasks work with Redis
eventlet.monkey_patch()

app = Flask(__name__)
# Allow CORS just in case
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

r = redis.Redis(host='localhost', port=6380, decode_responses=True)


def redis_listener():
    """Listens to Redis Pub/Sub and pushes to WebSockets"""
    pubsub = r.pubsub()
    pubsub.subscribe('fraud_notifications')

    print("Listening to Redis channel 'fraud_notifications'...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data']
            print(f"Server Received: {data}")  # Confirm Python sees it

            try:
                # Logic to parse "EvilA | EvilB | EvilC"
                accounts_str = data.split(":")[-1].strip()
                accounts = [acc.strip() for acc in accounts_str.split('|')]

                # Emit to all connected clients
                socketio.emit('fraud_alert', {'raw': data, 'accounts': accounts})
                print(" -> Emitted to Browser")  # Confirm emit happened
            except Exception as e:
                print(f"Error parsing alert: {e}")


# START BACKGROUND TASK HERE (Instead of threading.Thread)
socketio.start_background_task(redis_listener)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    # Ensure debug is False in production contexts to prevent double-reloads
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)