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

BATCH_SIZE = 1024
LOG_MSG_FOUND = "[%s] offset %d, Found %d"
LOG_MSG_STORED = "[%s] offset %d, Found %d, Stored %d"

get_test_beansack = lambda dbname="test": Beansack(
    os.getenv("MONGODB_CONN_STR", "mongodb://localhost:27017"), dbname
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
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(test_func, range(0, total, BATCH_SIZE))
    # list(map(test_func, range(0, total, BATCH_SIZE)))

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
        logger.info(LOG_MSG_FOUND, "cores", offset, len(found))
        inserted = warehouse.store_cores(found)
        logger.info(LOG_MSG_STORED, "cores", offset, len(found), len(inserted) if inserted else 0)
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
        logger.info(LOG_MSG_FOUND, "embeddings", offset, len(found))
        inserted = warehouse.store_embeddings(found)
        logger.info(LOG_MSG_STORED, "embeddings", offset, len(found), len(inserted) if inserted else 0)
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=307200)
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
        logger.info(LOG_MSG_FOUND, "gists", offset, len(found))
        inserted = warehouse.store_gists(found)
        logger.info(LOG_MSG_STORED, "gists", offset, len(found), len(inserted) if inserted else 0)
        # assert len(found) == len(inserted), "Not all beans were stored successfully"

    _run_test_func(get_and_store, total=204800)
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
            # filter = {
            #     "$or": [
            #         { "likes": { "$exists": True } },
            #         { "comments": { "$exists": True } },
            #         { "subscribers": { "$exists": True } }
            #     ]
            # },
            skip=offset,
            limit=BATCH_SIZE
        )
        found = [Chatter(**item) for item in cursor]
        logger.info(LOG_MSG_FOUND, "chatters", offset, len(found))
        inserted = warehouse.store_chatters(found)

        logger.info(LOG_MSG_STORED, "chatters", offset, len(found), len(inserted) if inserted else 0)

    _run_test_func(get_and_store, total=409600)
    # list(map(get_and_store, range(0, 409600, BATCH_SIZE)))
    warehouse.close()
    logger.info("Chatter store test completed")

def test_store_sources():
    """Test storing Source data in warehouse"""
    logger.info("Testing store_sources...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack("espresso")

    def get_and_store(offset):
        cursor = beansack.sourcestore.find(skip=offset, limit=BATCH_SIZE)
        found = []
        for item in cursor:
            found.append(Source(
                source=item.get('source'),
                base_url=item.get('site_base_url'),
                title=item.get('site_name'),
                description=item.get('site_description'),
                favicon=item.get('site_favicon'),
                rss_feed=item.get('site_rss_feed')
            ))
        logger.info(LOG_MSG_FOUND, "sources", offset, len(found))
        try:
            inserted = warehouse.store_sources(found)
            logger.info(LOG_MSG_STORED, "sources", 0, len(found), len(inserted) if inserted else 0)
        except Exception as e:
            logger.error(f"Error storing sources at offset\nError: {e}\nData: {found}")

    _run_test_func(get_and_store, total=12400)

    warehouse.close()
    logger.info("Sources store test completed")

def test_maintenance():
    """Test warehouse maintenance tasks"""
    logger.info("Testing warehouse maintenance...")

    # Initialize warehouse
    warehouse = get_test_warehouse()

    # warehouse.register_datafile('categories', "/workspaces/beansack/pycoffeemaker/coffeemaker/pybeansack/tests/categories.parquet")
    # warehouse.register_datafile('sentiments', "/workspaces/beansack/pycoffeemaker/coffeemaker/pybeansack/tests/sentiments.parquet")
    warehouse.compact()

    warehouse.close()
    logger.info("Warehouse maintenance test completed")

def run_all_store_tests(cores=False, embeddings=False, gists=False, chatters=False, sources=False, maintenance=False):
    """Run all store method tests"""
    logger.info("Running all warehouse store tests...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        if cores: executor.submit(test_store_cores)
        if embeddings: executor.submit(test_store_embeddings)
        if gists: executor.submit(test_store_gists)
        if chatters: executor.submit(test_store_chatters)
        if sources: executor.submit(test_store_sources)
        if maintenance: executor.submit(test_maintenance)
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
        sources = True,
        maintenance = True
    )
