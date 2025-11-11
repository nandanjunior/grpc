"""
XML-RPC UserBehavior Service (Music)
Analyzes per-user total listening time and top artist, forwards to Recommendation Service.
"""
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import os
import time
import json
from collections import defaultdict, Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

HOST = os.getenv('USERBEHAVIOR_HOST_XMLRPC', '0.0.0.0')
PORT = int(os.getenv('USERBEHAVIOR_PORT_XMLRPC', '8003'))
NEXT_URL = os.getenv('RECOMMENDATION_URL_XMLRPC', 'http://localhost:8005')

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

class UserBehaviorXMLHandler:
    def __init__(self, next_service_url):
        self.next_service_url = next_service_url
        print(f"[UserBehavior Service] Initialized. Next service: {next_service_url}")

    def process(self, records_data, accumulated_results):
        try:
            n = len(records_data or [])
            print(f"[UserBehavior Service] Received {n} records")
            start = time.time()

            user_time = defaultdict(int)
            user_artists = defaultdict(list)

            for r in records_data:
                uid = r.get('user_id')
                dur = int(r.get('duration', 0))
                art = r.get('artist', '')
                user_time[uid] += dur
                user_artists[uid].append(art)

            user_stats = []
            for uid, t in user_time.items():
                top_artist = Counter(user_artists[uid]).most_common(1)[0][0] if user_artists[uid] else ""
                user_stats.append({
                    'user_id': str(uid),
                    'total_time': int(t),
                    'top_artist': str(top_artist)
                })

            # top users by total_time
            top_users_sorted = sorted(user_time.items(), key=lambda x: x[1], reverse=True)[:5]
            top_users = [uid for uid, _ in top_users_sorted]

            processing_time = time.time() - start

            accumulated_results = accumulated_results or {}
            accumulated_results['userbehavior'] = {
                'user_stats': user_stats,
                'top_users': top_users,
                'processing_time': processing_time
            }

            try:
                with open(os.path.join(RESULTS_DIR, 'userbehavior_xmlrpc_metrics.json'), 'w', encoding='utf-8') as f:
                    json.dump({'processing_time': processing_time, 'num_users': len(user_stats)}, f, indent=2)
            except Exception:
                pass

            print(f"[UserBehavior] Processed {len(user_stats)} users")
            for s in user_stats[:10]:
                print(f"  • User {s['user_id']}: {s['total_time']}s listened, Top Artist: {s['top_artist']}")
            print(f"[UserBehavior] Processing time: {processing_time:.4f} seconds")
            print("[UserBehavior Service] Forwarding to Recommendation Service...")

            next_svc = ServerProxy(self.next_service_url, allow_none=True)
            final = next_svc.process(records_data, accumulated_results)
            return final

        except Exception as e:
            print(f"[UserBehavior Service] Error: {e}")
            raise

def main():
    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True, logRequests=False)
    server.register_introspection_functions()
    handler = UserBehaviorXMLHandler(NEXT_URL)
    server.register_instance(handler)

    print("=" * 70)
    print(f"UserBehavior XML-RPC Service started on {HOST}:{PORT}")
    print(f"Next (chained) service: {NEXT_URL}")
    print("=" * 70)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[UserBehavior Service] Shutting down...")

if __name__ == "__main__":
    main()
