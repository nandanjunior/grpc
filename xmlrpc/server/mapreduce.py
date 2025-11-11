"""
XML-RPC MapReduce Service (Music)
Counts plays per "Artist - SongID" and forwards to UserBehavior Service
"""
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import os
import time
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# allow importing from project root (adjust if your layout differs)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Config
HOST = os.getenv('MAPREDUCE_HOST_XMLRPC', '0.0.0.0')
PORT = int(os.getenv('MAPREDUCE_PORT_XMLRPC', '8001'))
NEXT_URL = os.getenv('USERBEHAVIOR_URL_XMLRPC', 'http://localhost:8003')

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

class MapReduceXMLHandler:
    def __init__(self, next_service_url):
        self.next_service_url = next_service_url
        print(f"[MapReduce Service] Initialized. Next service: {next_service_url}")

    def process(self, records_data, accumulated_results):
        """
        records_data: list of dicts {user_id, song_id, artist, duration, timestamp}
        accumulated_results: dict (can be empty)
        """
        try:
            n = len(records_data or [])
            print(f"[MapReduce Service] Processing {n} records...")
            start = time.time()

            # Map phase (parallel) — key = "Artist - SongID"
            def map_fn(r):
                key = f"{r.get('artist', '')} - {r.get('song_id', '')}"
                return (key, 1)

            mapped = []
            with ThreadPoolExecutor(max_workers=4) as ex:
                mapped = list(ex.map(map_fn, records_data))

            # Reduce
            counts = defaultdict(int)
            for k, v in mapped:
                counts[k] += v

            processing_time = time.time() - start

            # Serializables
            play_counts = {str(k): int(v) for k, v in counts.items()}

            accumulated_results = accumulated_results or {}
            accumulated_results['mapreduce'] = {
                'play_counts': play_counts,
                'processing_time': processing_time
            }

            # optional per-service metrics dump
            try:
                with open(os.path.join(RESULTS_DIR, 'mapreduce_xmlrpc_metrics.json'), 'w', encoding='utf-8') as f:
                    json.dump({'processing_time': processing_time, 'unique_keys': len(play_counts)}, f, indent=2)
            except Exception:
                pass

            print(f"[MapReduce] Processed {n} records")
            print(f"[MapReduce] Top counts (sample):")
            for i, (k, v) in enumerate(sorted(play_counts.items(), key=lambda x: x[1], reverse=True)[:10], 1):
                print(f"  {i}. {k}: {v} plays")
            print(f"[MapReduce] Processing time: {processing_time:.4f} seconds")
            print(f"[MapReduce Service] Forwarding to UserBehavior Service...")

            # Forward to next service
            next_service = ServerProxy(self.next_service_url, allow_none=True)
            final = next_service.process(records_data, accumulated_results)
            return final

        except Exception as e:
            print(f"[MapReduce Service] Error: {e}")
            raise

def main():
    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True, logRequests=False)
    server.register_introspection_functions()
    handler = MapReduceXMLHandler(NEXT_URL)
    server.register_instance(handler)

    print("=" * 70)
    print(f"MapReduce XML-RPC Service started on {HOST}:{PORT}")
    print(f"Next (chained) service: {NEXT_URL}")
    print("=" * 70)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[MapReduce Service] Shutting down...")

if __name__ == "__main__":
    main()