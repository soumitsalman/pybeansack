from concurrent.futures import ThreadPoolExecutor
import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from models import *
from mongosack import *
from warehouse import *

BATCH_SIZE = 128

get_test_beansack = lambda: Beansack(
    os.getenv("MONGODB_CONN_STR", "mongodb://localhost:27017"), "test"
)

get_test_warehouse = lambda: BeanWarehouse(
    "/workspaces/beansack/pycoffeemaker/coffeemaker/pybeansack/warehouse.sql", 
    storage_config={
        's3_region': os.getenv('S3_REGION'),
        's3_access_key_id': os.getenv('S3_ACCESS_KEY_ID'),
        's3_secret_access_key': os.getenv('S3_SECRET_ACCESS_KEY'),
        's3_endpoint': os.getenv('S3_ENDPOINT'),
        's3_use_ssl': True
    }
)

def _run_test_func(test_func, total=1000):
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(test_func, range(0, total, BATCH_SIZE))
    # for offset in range(0, total, batch_size):
    #     test_func(offset, batch_size)

def test_store_cores():
    """Test storing BeanCore data in warehouse"""
    logger.info("Testing store_cores...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack()

    def get_and_store(offset):
        cursor = list(beansack.beanstore.find(
            filter={"title": {"$exists": True}},
            skip=offset,
            limit=BATCH_SIZE,
            projection={"_id": 0, "gist": 0, "embedding": 0}
        ))
        for item in cursor:
            item['restricted_content'] = bool(item.get('is_scraped'))
        found = [BeanCore(**item) for item in cursor]
        logger.info(f"offset {offset}, Found {len(found)} cores")
        inserted = warehouse.store_cores(found)
        logger.info("offset %d, Found %d, Stored %d", offset, len(found), len(inserted))
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=500000)
    warehouse.close()
    logger.info("core store test completed")
    

def test_store_embeddings():
    """Test storing BeanEmbedding data in warehouse"""
    logger.info("Testing store_embeddings...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack()

    def get_and_store(offset):
        cursor = beansack.beanstore.find(
            filter={"embedding": {"$exists": True}},
            skip=offset,
            limit=BATCH_SIZE,
            projection={"url": 1,  "embedding": 1}
        )
        found = [BeanEmbedding(**item) for item in cursor]
        logger.info(f"offset {offset}, Found {len(found)} embeddings")
        inserted = warehouse.store_embeddings(found)
        logger.info("offset %d, Found %d, Stored %d", offset, len(found), len(inserted) if inserted else 0)
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=120000)
    warehouse.close()
    logger.info("embeddings store test completed")

def test_store_gists():
    """Test storing BeanGist data in warehouse"""
    logger.info("Testing store_gists...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack()

    def get_and_store(offset):
        cursor = beansack.beanstore.find(
            filter={"gist": {"$exists": True}},
            skip=offset,
            limit=BATCH_SIZE,
            projection={"url": 1, "gist": 1, "entities": 1, "regions": 1}
        )
        
        found = [BeanGist(**item) for item in cursor]
        logger.info(f"offset {offset}, Found {len(found)} gists")
        inserted = warehouse.store_gists(found)
        logger.info("offset %d, Found %d, Stored %d", offset, len(found), len(inserted))
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=102400)
    warehouse.close()
    logger.info("gists store test completed")

def test_store_chatters():
    """Test storing Chatter data in warehouse"""
    logger.info("Testing store_chatters...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack()

    def get_and_store(offset):
        cursor = beansack.chatterstore.find(
            skip=offset,
            limit=BATCH_SIZE
        )
        found = [Chatter(**item) for item in cursor]
        logger.info(f"offset {offset}, Found {len(found)} chatters")
        inserted = warehouse.store_chatters(found)
        logger.info("offset %d, Found %d, Stored %d", offset, len(found), len(inserted))
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=400000)
    warehouse.close()
    logger.info("Chatter store test completed")

def test_store_sources():
    """Test storing Source data in warehouse"""
    logger.info("Testing store_sources...")

    # Initialize warehouse
    warehouse = BeanWarehouse("./warehouse.sql")

    # Get test data from MongoDB
    beansack = get_test_beansack()
    sources_data = list(beansack.sourcestore.find(
        projection={"_id": 0},
        limit=10
    ))

    if not sources_data:
        logger.warning("No test sources found, skipping store_sources test")
        return

    # Convert to Source models
    sources = [Source(**source) for source in sources_data if source]

    if not sources:
        logger.warning("No valid Source data after conversion")
        return

    # Store the data
    stored_count = warehouse.store_sources(sources)
    logger.info(f"Stored {stored_count} sources")

    warehouse.close()
    assert stored_count == len(sources), "Not all sources were stored successfully"

def run_all_store_tests(cores=True, embeddings=False, gists=False, chatters=False, sources=False):
    """Run all store method tests"""
    logger.info("Running all warehouse store tests...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        if cores: executor.submit(test_store_cores)
        if embeddings: executor.submit(test_store_embeddings)
        if gists: executor.submit(test_store_gists)
        if chatters: executor.submit(test_store_chatters)
        if sources: executor.submit(test_store_sources)

    # if cores: test_store_cores()
    # if embeddings: test_store_embeddings()
    # if gists: test_store_gists()
    # if chatters: test_store_chatters()
    # if sources: test_store_sources()
    logger.info("All warehouse store tests completed")

if __name__ == "__main__":
    run_all_store_tests(
        cores = True,
        embeddings = True,
        gists = True,
        chatters = True,
        sources = False
    )
