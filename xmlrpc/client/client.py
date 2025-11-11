"""
XML-RPC Client for Music Streaming chained services:
Client -> MapReduce(XML-RPC) -> UserBehavior -> Recommendation -> Client
Saves detailed metrics similar to gRPC JSON (for fair comparison).
"""
from xmlrpc.client import ServerProxy
import os
import sys
import time
import json
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
CSV_PATH = os.getenv('CSV_PATH', os.path.join(PROJECT_ROOT, 'data', 'stream_data.csv'))
MAPREDUCE_URL = os.getenv('MAPREDUCE_URL_XMLRPC', 'http://localhost:8001')
OUTPUT_FILE = os.getenv('OUTPUT_FILE_XMLRPC', os.path.join(PROJECT_ROOT, 'results', 'run_xmlrpc_metrics.json'))

def load_stream_csv(csv_path):
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()
        if not lines:
            return []
        for line in lines[1:]:
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 5:
                records.append({
                    'user_id': parts[0],
                    'song_id': parts[1],
                    'artist': parts[2],
                    'duration': int(parts[3]),
                    'timestamp': parts[4]
                })
    return records

class ChainedXMLRPCClient:
    def __init__(self, mapreduce_url):
        self.mapreduce_url = mapreduce_url
        self.proxy = None

    def connect(self):
        self.proxy = ServerProxy(self.mapreduce_url, allow_none=True)
        # validate
        self.proxy.system.listMethods()
        print(f"[Client] Connected to MapReduce at {self.mapreduce_url}")

    def disconnect(self):
        self.proxy = None
        print("[Client] Disconnected")

    def start_workflow(self, records):
        accumulated = {}
        start = time.time()
        final = self.proxy.process(records, accumulated)
        end = time.time()
        return final, end - start

def main():
    print("=" * 70)
    print("XML-RPC MUSIC STREAMING CLIENT")
    print("=" * 70)
    print(f"MapReduce entry: {MAPREDUCE_URL}")
    print("=" * 70)

    records = load_stream_csv(CSV_PATH)
    if not records:
        print("[Client] No records found. Exiting.")
        return

    client = ChainedXMLRPCClient(MAPREDUCE_URL)
    try:
        client.connect()
    except Exception as e:
        print(f"[Client] Connect failed: {e}")
        return

    try:
        print(f"[Client] Launching chained workflow with {len(records)} records...")
        final_results, workflow_time = client.start_workflow(records)
        print(f"[Client] Workflow finished in {workflow_time:.4f}s")

        mapreduce = final_results.get('mapreduce', {})
        userbehavior = final_results.get('userbehavior', {})
        recommendation = final_results.get('recommendation', {})

        map_time = float(mapreduce.get('processing_time', 0.0))
        user_time = float(userbehavior.get('processing_time', 0.0))
        rec_time = float(recommendation.get('processing_time', 0.0))
        total_processing = map_time + user_time + rec_time
        network_overhead = workflow_time - total_processing
        avg_service_time = total_processing / 3 if total_processing > 0 else 0
        overhead_pct = (network_overhead / workflow_time * 100) if workflow_time > 0 else 0

        # Pretty print (similar to gRPC client)
        print("\n" + "="*70)
        print("WORKFLOW RESULTS")
        print("="*70)
        print("\n[MapReduce] Top songs (sample):")
        for idx, (k, v) in enumerate(sorted(mapreduce.get('play_counts', {}).items(), key=lambda x: x[1], reverse=True)[:10], 1):
            print(f"  {idx}. {k}: {v} plays")

        print("\n[UserBehavior] Top users (sample):")
        for u in userbehavior.get('top_users', [])[:10]:
            print(f"  - {u}")

        print("\n[Recommendation] Trending songs (sample):")
        for idx, s in enumerate(recommendation.get('trending_songs', [])[:10], 1):
            print(f"  {idx}. {s}")

        print("\n" + "="*70)
        print("PERFORMANCE SUMMARY")
        print("="*70)
        print(f"MapReduce Time:      {map_time:.4f}s")
        print(f"UserBehavior Time:   {user_time:.4f}s")
        print(f"Recommendation Time: {rec_time:.4f}s")
        print(f"Total Processing:    {total_processing:.4f}s")
        print(f"End-to-End Time:     {workflow_time:.4f}s")
        print(f"Network Overhead:    {network_overhead:.4f}s")
        print("="*70)

        # Save detailed JSON (schema similar to gRPC output)
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "protocol": "XML-RPC",
            "workflow_time": workflow_time,
            "mapreduce_time": map_time,
            "userbehavior_time": user_time,
            "recommendation_time": rec_time,
            "total_processing_time": total_processing,
            "network_overhead": network_overhead,
            "summary": {
                "total_services": 3,
                "avg_service_time": avg_service_time,
                "overhead_percentage": overhead_pct
            },
            "detailed_results": final_results
        }

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)
        print(f"[Client] Saved XML-RPC metrics to {OUTPUT_FILE}")

    except Exception as e:
        print(f"[Client] Workflow error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
