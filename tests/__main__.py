"""
Main entry point for running tests as a module.
Enables: python -m tests (from pybeansack directory)
"""
import os
import sys

# Set up path for running from pybeansack directory
parent_of_pybeansack = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if parent_of_pybeansack not in sys.path:
    sys.path.insert(0, parent_of_pybeansack)

if __name__ == "__main__":
    import argparse
    from pybeansack.tests.test import *
    
    parser = argparse.ArgumentParser(description="Run warehouse tests")
    parser.add_argument('--cores', action='store_true', help='Run store cores test')
    parser.add_argument('--embeddings', action='store_true', help='Run store embeddings test')
    parser.add_argument('--gists', action='store_true', help='Run store gists test')
    parser.add_argument('--chatters', action='store_true', help='Run store chatters test')
    parser.add_argument('--sources', action='store_true', help='Run store sources test')
    parser.add_argument('--maintenance', action='store_true', help='Run maintenance test')
    parser.add_argument('--unprocessed', action='store_true', help='Run unprocessed beans query test')
    parser.add_argument('--processed', action='store_true', help='Run processed beans query test')
    parser.add_argument('--setup', action='store_true', help='Run register test')
    parser.add_argument('--random', action='store_true', help='Run random query test')
    parser.add_argument('--lancedb', action='store_true', help='Run lancesack test')
    parser.add_argument('--pgsack', action='store_true', help='Run lancesack test')
    
    args = parser.parse_args()
    if args.cores: test_store_cores()
    if args.embeddings: test_store_embeddings()
    if args.gists: test_store_gists()
    if args.chatters: test_store_chatters()
    if args.sources: test_store_sources()
    if args.maintenance: test_maintenance()
    if args.unprocessed: test_unprocessed_beans()
    if args.processed: test_processed_beans()
    if args.setup: test_setup()
    if args.random: test_random_query()
    if args.lancedb: test_lancesack()
    if args.pgsack: test_pgsack()
