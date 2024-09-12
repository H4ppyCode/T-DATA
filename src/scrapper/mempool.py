import websocket
import rel
import json


class MempoolWs:
    def __init__(self, url: str = "wss://mempool.space/api/v1/ws"):
        rel.safe_read()
        self.ws = websocket.WebSocketApp(url,
                              on_open=self.on_open,
                              on_message=self.on_message,
                              on_error=self.on_error,
                              on_close=self.on_close)
        self.ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()

    def on_open(self, ws):
        message = { "action": "init" }
        ws.send(json.dumps(message))
        message = { "action": "want", "data": ['blocks', 'stats', 'mempool-blocks', 'live-2h-chart', 'watch-mempool'] }
        ws.send(json.dumps(message))

    def on_message(self, ws, message):
        print(json.loads(message))

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")


if __name__ == "__main__":
    MempoolWs()
