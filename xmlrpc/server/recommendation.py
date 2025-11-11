"""
XML-RPC Recommendation Service (Music)
Terminal service: compute trending songs and user recommendations, return final accumulated results.
"""
from xmlrpc.server import SimpleXMLRPCServer
import sys
import os
import time
import json
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

HOST = os.getenv('RECOMMENDATION_HOST_XMLRPC', '0.0.0.0')
PORT = int(os.getenv('RECOMMENDATION_PORT_XMLRPC', '8005'))

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

class RecommendationXMLHandler:
    def __init__(self):
        print("[Recommendation Service] Initialized (terminal service)")

    def process(self, records_data, accumulated_results):
        try:
            print("[Recommendation Service] Received chain input. Generating recommendations...")
            start = time.time()

            play_counts = accumulated_results.get('mapreduce', {}).get('play_counts', {})
            user_stats = accumulated_results.get('userbehavior', {}).get('user_stats', [])

            # compute trending (top 5)
            counter = Counter(play_counts)
            top5 = [k for k, _ in counter.most_common(5)]

            # recommendations per user: exclude songs containing their top_artist
            recommendations = {}
            for u in user_stats:
                uid = u.get('user_id')
                fav = u.get('top_artist', '')
                recs = [s for s in top5 if fav and fav not in s]
                # if none left, just return top5
                if not recs:
                    recs = top5.copy()
                recommendations[str(uid)] = recs

            processing_time = time.time() - start

            accumulated_results['recommendation'] = {
                'trending_songs': top5,
                'recommendations': recommendations,
                'processing_time': processing_time
            }

            try:
                with open(os.path.join(RESULTS_DIR, 'recommendation_xmlrpc_metrics.json'), 'w', encoding='utf-8') as f:
                    json.dump({'processing_time': processing_time, 'num_trending': len(top5)}, f, indent=2)
            except Exception:
                pass

            print(f"[Recommendation] Trending (top {len(top5)}): {top5}")
            print(f"[Recommendation] Processing time: {processing_time:.4f} seconds")
            print("[Recommendation Service] Returning final results to client.")
            return accumulated_results

        except Exception as e:
            print(f"[Recommendation Service] Error: {e}")
            raise

def main():
    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True, logRequests=False)
    server.register_introspection_functions()
    handler = RecommendationXMLHandler()
    server.register_instance(handler)

    print("=" * 70)
    print(f"Recommendation XML-RPC Service started on {HOST}:{PORT}")
    print("Terminal service - returns full accumulated results")
    print("=" * 70)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Recommendation Service] Shutting down...")

if __name__ == "__main__":
    main()
