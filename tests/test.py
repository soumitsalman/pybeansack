from dotenv import load_dotenv
load_dotenv()

import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from icecream import ic

# Import from package using relative imports
from .. import lakehouse, mongosack, lancesack
from ..models import *

import argparse
import numpy as np
import random
from datetime import datetime
from faker import Faker
from slugify import slugify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
faker = Faker()

DATA_DIR = os.getenv("DATA_DIR", ".coffeemaker")
BATCH_SIZE = 128
LOG_MSG_FOUND = "[%s] offset %d, Found %d"
LOG_MSG_STORED = "[%s] offset %d, Found %d, Stored %d"

get_test_beansack = lambda dbname="test": mongosack.Beansack(
    os.getenv("MONGODB_CONN_STR", "mongodb://localhost:27017"), dbname
)

get_test_warehouse = lambda: lakehouse.Beansack()

def _run_test_func(test_func, total=1000):
    # with ThreadPoolExecutor(max_workers=8) as executor:
    #     executor.map(test_func, range(0, total, BATCH_SIZE))
    list(map(test_func, range(0, total, BATCH_SIZE)))

def test_store_cores():
    """Test storing BeanCore data in warehouse"""
    logger.info("Testing store_cores...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    # Get test data from MongoDB
    beansack = get_test_beansack()

    def get_and_store(offset):
        found = list(beansack.beanstore.find(
            filter={"title": {"$exists": True}},
            skip=offset,
            limit=BATCH_SIZE,
            projection={"_id": 0, "gist": 0, "embedding": 0}
        ))
        found = list(filter(lambda x: x.get('source'), found))
        for item in found:
            item['restricted_content'] = bool(item.get('is_scraped'))            
            # item['summary'] = item.get('summary') or None
            # item['author'] = item.get('author') or None
            # item['image_url'] = item.get('image_url') or None
        found = [BeanCore(**item) for item in found]
        logger.info(LOG_MSG_FOUND, "cores", offset, len(found))
        inserted = warehouse.store_cores(found)
        logger.info(LOG_MSG_STORED, "cores", offset, len(found), len(inserted) if inserted else 0)

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
        found = beansack.chatterstore.find(
            skip=offset,
            limit=BATCH_SIZE
        )
        found = list(filter(lambda x: x.get('source') and x.get('chatter_url') , found))
        found = [Chatter(**item) for item in found]
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
        cursor = beansack.publisherstore.find(skip=offset, limit=BATCH_SIZE)
        found = []
        for item in cursor:
            found.append(Publisher(
                source=item.get('source'),
                base_url=item.get('site_base_url'),
                site_name=item.get('site_name'),
                description=item.get('site_description'),
                favicon=item.get('site_favicon'),
                rss_feed=item.get('site_rss_feed')
            ))
        logger.info(LOG_MSG_FOUND, "sources", offset, len(found))        
        inserted = warehouse.store_publishers(found)
        logger.info(LOG_MSG_STORED, "sources", 0, len(found), len(inserted) if inserted else 0)    

    _run_test_func(get_and_store, total=12400)

    warehouse.close()
    logger.info("Sources store test completed")

def test_maintenance():
    """Test warehouse maintenance tasks"""
    logger.info("Testing warehouse maintenance...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    warehouse.recompute()
    warehouse.close()
    logger.info("Warehouse maintenance test completed")


def test_setup():
    """Test warehouse maintenance tasks"""
    logger.info("Testing warehouse maintenance...")

    # Initialize warehouse
    warehouse = get_test_warehouse()
    warehouse.setup()
    warehouse.close()
    logger.info("Warehouse maintenance test completed")

def test_unprocessed_beans():
    """Test querying unprocessed beans"""
    logger.info("Testing query_unprocessed_beans...")

    # Initialize warehouse
    warehouse = get_test_warehouse()

    content_length = 100
    created = datetime.now() - timedelta(days=2)
    created_str = created.strftime("%Y-%m-%d")

    beans = warehouse.query_contents_with_missing_embeddings(
        conditions=[
            f"content_length >= {content_length}", 
            f"created >= '{created_str}'"
        ], 
        limit=10
    )
    for bean in beans:
        print(bean.url, bean.content[:50])

    beans = warehouse.query_contents_with_missing_gists(
        conditions=[
            f"content_length >= {content_length}", 
            f"created >= '{created_str}'"
        ], 
        limit=10
    )
    for bean in beans:
        print(bean.url, bean.content[:50])

    warehouse.close()
    logger.info("Query unprocessed beans test completed")

def test_processed_beans():
    """Test querying processed beans"""
    logger.info("Testing query_processed_beans...")

    # Initialize warehouse
    warehouse = get_test_warehouse()

    for offset in range(0, 200, 10):
        beans = warehouse.query_processed_beans(
            kind="news",
            created=datetime.now() - timedelta(days=14),        
            # categories=["Machine Learning and AI Applications", "Cloud Computing"], 
            # entities=["South Korea"],
            embedding=[
                -0.03653450682759285,
                0.01646137423813343,
                0.07098295539617538,
                -0.019134635105729103,
                0.06962728500366211,
                -0.000977593706920743,
                0.004345798399299383,
                0.008926904760301113,
                0.04817322641611099,
                -0.020556693896651268,
                0.0053676036186516285,
                -0.05440796539187431,
                0.031144559383392334,
                0.00027641808264888823,
                0.024522384628653526,
                0.010199358686804771,
                0.03682781010866165,
                -0.031229397282004356,
                0.013300992548465729,
                0.002872086362913251,
                0.03121226653456688,
                -0.014550581574440002,
                -0.03947068005800247,
                -0.08067464083433151,
                -0.029869984835386276,
                0.013059777207672596,
                -0.001797103090211749,
                -0.016643892973661423,
                -0.0028305102605372667,
                -0.20514798164367676,
                -0.0004374505952000618,
                -0.004210734274238348,
                0.08483289927244186,
                0.025231365114450455,
                -0.01256607472896576,
                -0.0062669189646840096,
                -0.04229079186916351,
                0.018565639853477478,
                -0.06145390123128891,
                0.005014173220843077,
                -0.02401565946638584,
                -0.044255249202251434,
                0.013397790491580963,
                -0.05811101943254471,
                0.06352726370096207,
                -0.029428016394376755,
                0.009081961587071419,
                -0.019334450364112854,
                -0.0022559105418622494,
                -0.018832329660654068,
                0.0017992754001170397,
                -0.0440349355340004,
                0.006270613521337509,
                0.04227504879236221,
                -0.0012202853104099631,
                0.03531260043382645,
                0.02089134231209755,
                0.06279315799474716,
                0.07494078576564789,
                0.00537154171615839,
                0.022118087857961655,
                0.01667952910065651,
                -0.20079651474952698,
                0.03960835188627243,
                -0.014994338154792786,
                0.0009719543159008026,
                -0.015597065910696983,
                -0.025723004713654518,
                0.014544978737831116,
                0.04402732104063034,
                8.717281889403239e-05,
                0.011648843996226788,
                0.038879573345184326,
                0.035746339708566666,
                0.005841305945068598,
                0.026262741535902023,
                0.0041028414852917194,
                -0.0286540687084198,
                -0.018613385036587715,
                -0.016850396990776062,
                0.0035444176755845547,
                0.011594930663704872,
                -0.06191517785191536,
                -0.03855779021978378,
                -0.06721461564302444,
                0.032819248735904694,
                -0.02023923769593239,
                -0.04958716779947281,
                0.03805490583181381,
                0.025179121643304825,
                -0.012703439220786095,
                0.017656132578849792,
                -0.026811698451638222,
                0.023079771548509598,
                -0.028016548603773117,
                -0.008448893204331398,
                -0.03268922120332718,
                0.02143745683133602,
                -0.043171998113393784,
                0.41510090231895447,
                -0.03277428820729256,
                0.0021139427553862333,
                0.042443372309207916,
                -0.037628185003995895,
                0.02579648792743683,
                -0.044945258647203445,
                -0.03534793108701706,
                -0.0364445261657238,
                -0.02465297281742096,
                0.016790257766842842,
                -0.04643489047884941,
                0.007791223470121622,
                0.022956395521759987,
                0.008134914562106133,
                -0.01301395334303379,
                0.007589398417621851,
                0.050320424139499664,
                0.023033088073134422,
                0.014348847791552544,
                -0.017470944672822952,
                0.022135719656944275,
                0.02560976706445217,
                0.02335328795015812,
                0.000243630405748263,
                -0.026071494445204735,
                -0.10901127010583878,
                0.024471305310726166,
                0.03296619653701782,
                -0.0024577779695391655,
                0.014484557323157787,
                0.07127542793750763,
                -0.018544876947999,
                -0.03762564808130264,
                -0.0035008881241083145,
                0.014726502820849419,
                0.010553720407187939,
                0.0022574185859411955,
                0.026602551341056824,
                -0.007012611720710993,
                0.012730782851576805,
                -0.027720805257558823,
                -0.03441806510090828,
                0.02583182230591774,
                -0.11067676544189453,
                -0.0051114908419549465,
                0.07906712591648102,
                -0.028275316581130028,
                -0.001303909346461296,
                -0.005132146645337343,
                0.018272291868925095,
                0.010329751297831535,
                -0.018873605877161026,
                -0.0007723228773102164,
                0.017342936247587204,
                0.022367309778928757,
                -0.029958480969071388,
                0.03125648945569992,
                0.04776865243911743,
                -0.06388591974973679,
                -0.007560486905276775,
                0.006735004484653473,
                -0.035939645022153854,
                -0.07220878452062607,
                0.08051285892724991,
                0.001530233770608902,
                -0.0921080932021141,
                -0.005948538891971111,
                0.019647875800728798,
                0.009531518444418907,
                -0.00037280545802786946,
                0.04035034403204918,
                0.03652779012918472,
                -0.05940026044845581,
                0.019732479006052017,
                0.07590927183628082,
                0.01285380870103836,
                -0.007875352166593075,
                0.006760657764971256,
                -0.0008449786109849811,
                0.010102001018822193,
                0.053610511124134064,
                -0.015084299258887768,
                -0.004079613834619522,
                0.024247631430625916,
                0.03931742534041405,
                -0.05864088982343674,
                0.02059110440313816,
                -0.022997824475169182,
                0.02076622284948826,
                0.01188302505761385,
                -0.03782268613576889,
                0.04523741081357002,
                -0.008091804571449757,
                0.01990700140595436,
                -0.014471495524048805,
                -0.021431270986795425,
                -0.010666840709745884,
                -0.014697028324007988,
                -0.015252916142344475,
                -0.05363208055496216,
                0.04221845418214798,
                0.011050546541810036,
                -0.034525830298662186,
                -0.02536907233297825,
                -0.02244887314736843,
                0.004521300550550222,
                0.02929331734776497,
                -0.003948342055082321,
                0.03578367084264755,
                0.0587347038090229,
                -0.03115019202232361,
                0.011598811484873295,
                0.022862421348690987,
                -0.0011405956465750933,
                0.01847270503640175,
                0.006731242872774601,
                0.005404390394687653,
                0.02950724959373474,
                0.0007808312657289207,
                0.020162295550107956,
                -0.022654736414551735,
                0.008497973904013634,
                -0.06288128346204758,
                -0.2921091914176941,
                0.0207488052546978,
                -0.002090162131935358,
                0.004323030821979046,
                0.004704570863395929,
                -0.038230132311582565,
                0.0006564190844073892,
                -0.02160060964524746,
                0.09522227942943573,
                0.031949955970048904,
                0.08716068416833878,
                0.007287697400897741,
                -0.014191566035151482,
                0.01627354323863983,
                -0.011112702079117298,
                0.021120617166161537,
                0.05818699672818184,
                0.027878548949956894,
                -0.02595416270196438,
                -0.012681606225669384,
                -0.0016910841222852468,
                0.021742235869169235,
                0.019183214753866196,
                -0.024097803980112076,
                0.006716770585626364,
                0.013095265254378319,
                0.11786229908466339,
                -0.02632337436079979,
                -5.8243051171302795e-05,
                0.002474952256307006,
                0.011880211532115936,
                0.030118465423583984,
                -0.0038122546393424273,
                -0.07028194516897202,
                0.06298240274190903,
                0.019256597384810448,
                0.03768070414662361,
                -0.056756168603897095,
                -0.06962524354457855,
                -0.0333956740796566,
                -0.04917221516370773,
                0.03257376700639725,
                -0.004305668640881777,
                -0.040137216448783875,
                -0.024112803861498833,
                -0.027360472828149796,
                -0.03879598528146744,
                0.0062406957149505615,
                -0.07590490579605103,
                0.008858476765453815,
                -0.011367416009306908,
                0.012853177264332771,
                0.02730141207575798,
                -0.01234340202063322,
                -0.012833714485168457,
                0.0035239122807979584,
                -0.052127350121736526,
                0.038317542523145676,
                0.0006334370118565857,
                0.003228971268981695,
                -0.0004540806694421917,
                -0.04403005167841911,
                -0.00572125893086195,
                -0.03211730718612671,
                0.03865348547697067,
                -0.025710975751280785,
                0.0006156658055260777,
                0.052391696721315384,
                0.013413742184638977,
                0.03251910209655762,
                -0.03483860567212105,
                0.08043880015611649,
                0.030118782073259354,
                -0.005071363877505064,
                0.08185403048992157,
                0.0035829097032546997,
                0.01889922469854355,
                -0.012644904665648937,
                0.015058891847729683,
                0.004498633556067944,
                0.05809963494539261,
                -0.0041658515110611916,
                0.024457693099975586,
                0.01913974992930889,
                0.0251231137663126,
                0.011493260972201824,
                0.019362086430191994,
                -0.02718287706375122,
                0.026450414210557938,
                -0.020978903397917747,
                -0.028019875288009644,
                -0.003437524661421776,
                -0.02534833550453186,
                -0.038265928626060486,
                0.04404642432928085,
                0.014317670837044716,
                -0.34631556272506714,
                0.04588356614112854,
                -0.03417114168405533,
                0.08290223777294159,
                -0.05224096029996872,
                -0.014805048704147339,
                -0.029744509607553482,
                -0.015230509452521801,
                -0.013426626101136208,
                0.007742748595774174,
                -0.008144699037075043,
                0.02150360681116581,
                0.002091742819175124,
                -0.009040328674018383,
                0.006048449780791998,
                -0.04022228717803955,
                0.0370451882481575,
                -0.01944432221353054,
                0.04323193430900574,
                -0.017713673412799835,
                0.024380359798669815,
                0.053504981100559235,
                0.22166737914085388,
                0.0014125159941613674,
                0.008910120464861393,
                -0.009693468920886517,
                -0.002346586901694536,
                -0.0231839157640934,
                0.0017985627055168152,
                -0.031056230887770653,
                -0.03132088854908943,
                0.010065145790576935,
                0.08131194114685059,
                0.00934496521949768,
                0.019971519708633423,
                0.005027606151998043,
                -0.0421869158744812,
                2.2217374862520956e-05,
                -0.004645271692425013,
                0.022963901981711388,
                0.004419128410518169,
                0.011112590320408344,
                -0.04034733027219772,
                0.014240147545933723,
                0.04011970013380051,
                0.019018791615962982,
                -0.04114062711596489,
                -0.028664711862802505,
                -0.02301774173974991,
                0.014269331470131874,
                -0.00775457639247179,
                -0.043565571308135986,
                -0.04001067951321602,
                -0.00665354123339057,
                0.003981081303209066,
                0.04826711490750313,
                -0.004488990642130375,
                -0.028742650523781776,
                -0.031116336584091187,
                -0.03655574470758438,
                0.01053606066852808,
                -0.041158512234687805,
                0.04479822888970375,
                0.024826742708683014,
                0.02787953056395054
            ],
            distance=0.3,
            limit=10,
            offset=offset
        )
        [print(f"[{bean['created']}] Categories: {bean['categories']} | Title: {bean['title']}") for bean in beans]

    warehouse.close()
    logger.info("Query processed beans test completed")

def test_random_query():
    """Test random query"""
    logger.info("Testing random_query...")

    # Initialize warehouse
    warehouse = get_test_warehouse()

    # warehouse.execute(SQL_CLEANUP)  # SQL_CLEANUP is local to warehouse.cleanup()
#     query_expr = f"""
#     SELECT mcl.url as url, e.url as related, array_distance(mcl.embedding::FLOAT[{VECTOR_LEN}], e.embedding::FLOAT[{VECTOR_LEN}]) as distance 
# FROM warehouse.missing_clusters_view mcl
# CROSS JOIN warehouse.bean_embeddings e
# WHERE distance BETWEEN 0.01 AND {CLUSTER_EPS}
# LIMIT 5
#     """
#     beans = warehouse.query(query_expr)
#     [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT * FROM warehouse.bean_categories LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT * FROM warehouse.bean_sentiments LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT * FROM warehouse.bean_gists LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT url FROM warehouse.bean_embeddings LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT * FROM warehouse.processed_beans_view LIMIT 10
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT url, title, content, created FROM bean_cores b
    # WHERE url NOT IN (SELECT url FROM bean_embeddings)
    # ORDER BY created DESC
    # LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT url, title, content, created FROM bean_cores b
    # WHERE url NOT IN (SELECT url FROM bean_gists)
    # ORDER BY created DESC
    # LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    # query_expr = """
    # SELECT url, title, content, created FROM unprocessed_beans_view
    # WHERE gist IS NULL AND content_length > 100
    # ORDER BY created DESC
    # LIMIT 5
    # """
    # params = None
    # beans = warehouse.query(query_expr, params=params)
    # [ic(bean) for bean in beans]

    warehouse.close()
    logger.info("Random query test completed")


def generate_fake_beans(ai_fields = True, limit = 30):
    generate = lambda: Bean(
        url=faker.url(),
        kind=faker.random_element(elements=("news", "blog")),
        source=faker.domain_name(),
        title=faker.sentence(nb_words=6),
        summary=faker.paragraph(nb_sentences=5),
        content=faker.paragraph(nb_sentences=10),
        image_url=faker.image_url(),
        author=faker.name(),
        created=faker.date_time_this_year(),
        collected=faker.date_time_this_month(),
        # ai fields
        embedding=np.random.random(VECTOR_LEN).tolist() if ai_fields else None,
        gist=faker.text(max_nb_chars=200) if ai_fields else None,
        categories=[faker.word() for _ in range(3)] if ai_fields else None,
        sentiments=[faker.word() for _ in range(3)] if ai_fields else None,
        entities=[faker.name() for _ in range(3)] if ai_fields else None,
        regions=[faker.country(), faker.city()] if ai_fields else None
    )
    return [generate() for _ in range(random.randrange(10, limit))]

def generate_fake_embeddings(urls = None):
    generate = lambda url: Bean(
        url=url or faker.url(),        
        embedding=np.random.random(VECTOR_LEN).tolist()
    )
    if urls: return [generate(url) for url in urls]
    return [generate(None) for _ in range(random.randrange(3, 30))]

def generate_fake_digests(urls = None):
    generate = lambda url: Bean(
        url=url or faker.url(),        
        source=faker.domain_name(),        
        gist="[UPDATED]" + faker.text(max_nb_chars=200),        
        entities=["[UPDATED]" + faker.name() for _ in range(3)],
        regions=["[UPDATED]" + faker.country(), "[UPDATED]" + faker.city()]
    )
    if urls: return [generate(url) for url in urls]
    return [generate(None) for _ in range(random.randrange(3, 30))]

def generate_fake_publishers(sources = None, update = False):
    generate = lambda source: Publisher(
        source=source or faker.domain_name(),
        base_url=faker.url(),
        site_name=("[UPDATED]" if update else "")+faker.company() if random.random() > 0.5 else None,
        description=("[UPDATED]" if update else "")+faker.sentence(nb_words=10) if random.random() > 0.5 else None,
        favicon=("[UPDATED]" if update else "")+faker.image_url() if random.random() > 0.5 else None,
        rss_feed=("[UPDATED]" if update else "")+faker.url() if random.random() > 0.5 else None
    )
    if sources: return [generate(source) for source in sources]
    return [generate(None) for _ in range(random.randrange(5, 15))]

def generate_fake_chatters():
    generate = lambda: Chatter(
        chatter_url=faker.url(),
        url=faker.url(),
        source=faker.domain_name(),
        forum=faker.random_element(elements=("r/technology", "r/programming", "HackerNews", "Lobsters")),
        collected=faker.date_time_this_month(),
        updated=faker.date_time_this_month(),
        likes=faker.random_int(min=0, max=1000),
        comments=faker.random_int(min=0, max=500),
        subscribers=faker.random_int(min=0, max=10000)
    )
    return [generate() for _ in range(random.randrange(10, 30))]

def generate_fake_mugs():
    generate = lambda: Mug(
        id=slugify(faker.sentence(nb_words=3)),
        title=faker.sentence(nb_words=6),
        content=faker.paragraph(nb_sentences=8),
        embedding=np.random.random(VECTOR_LEN).tolist(),
        created=faker.date_time_this_year(),
        updated=faker.date_time_this_month(),
        sips=[slugify(faker.sentence(nb_words=2)) for _ in range(random.randrange(2, 6))],
        highlights=[faker.sentence(nb_words=5) for _ in range(random.randrange(1, 4))],
        tags=[faker.word() for _ in range(random.randrange(2, 5))]
    )
    return [generate() for _ in range(random.randrange(5, 15))]

def generate_fake_sips():
    generate = lambda: Sip(
        id=slugify(faker.sentence(nb_words=3)),
        title=faker.sentence(nb_words=5),
        content=faker.paragraph(nb_sentences=6),
        embedding=np.random.random(VECTOR_LEN).tolist(),
        created=faker.date_time_this_year(),
        updated=faker.date_time_this_month(),
        mug=slugify(faker.sentence(nb_words=2)),
        related=[slugify(faker.sentence(nb_words=3)) for _ in range(random.randrange(0, 3))],
        beans=[faker.url() for _ in range(random.randrange(1, 5))]
    )
    return [generate() for _ in range(random.randrange(10, 30))]

def test_lancesack():
    db = lancesack.Beansack(f"{os.getenv('TEST_STORAGE')}/{datetime.now().strftime('%Y-%m-%d')}-lancedb/v2")
    
    ic(db.allbeans.count_rows())
    ic(db.store_beans(generate_fake_beans(ai_fields=False)))
    ic(db.allbeans.count_rows())

    ic(db.allpublishers.count_rows())
    ic(db.store_publishers(generate_fake_publishers()))
    ic(db.allpublishers.count_rows())   

    ic(db.allchatters.count_rows())
    ic(db.store_chatters(generate_fake_chatters()))
    ic(db.allchatters.count_rows())

    ic(db.allmugs.count_rows())
    ic(db.store_mugs(generate_fake_mugs()))
    ic(db.allmugs.count_rows())

    ic(db.allsips.count_rows())
    ic(db.store_sips(generate_fake_sips()))
    ic(db.allsips.count_rows())

def test_pgsack():
    from .. import pgsack
    db = pgsack.Beansack(os.getenv('PG_CONNECTION_STRING'))
    
    if False:
        print("=== INSERT BEANS ===")
        ic(db.count_rows(BEANS))
        beans = generate_fake_beans(ai_fields=False)
        ic(len(beans), db.store_beans(beans))
        ic(db.count_rows(BEANS))

    if False:
        print("=== DEDUPLICATE BEANS ===")
        beans = generate_fake_beans()
        beans[0].url = "https://choi.com/"
        beans[1].url = "https://chase.org/"
        beans[2].url = "https://www.murphy.biz/"
        ic(len(beans), len(db.deduplicate(BEANS, beans)))

    if False:
        print("=== INSERT PUBLISHERS ===")
        ic(db.count_rows(PUBLISHERS))
        publishers = generate_fake_publishers()
        ic(len(publishers), db.store_publishers(publishers))
        ic(db.count_rows(PUBLISHERS))

    if False:
        print("=== INSERT CHATTERS ===")
        ic(db.count_rows(CHATTERS))
        chatters = generate_fake_chatters()
        chatters[0].chatter_url = "http://williams.com/"
        chatters[1].chatter_url = "http://morrow.com/"
        ic(len(chatters), db.store_chatters(chatters))
        ic(db.count_rows(CHATTERS))

    if False:
        print("=== UPDATE DIGESTS ===")
        beans = ic(db.query_latest_beans(limit=2, columns=[K_URL, K_ENTITIES, K_REGIONS]))
        urls = [bean.url for bean in beans]
        print("+++++++++")
        ic(db.update_beans(generate_fake_digests(urls), columns=[K_GIST, K_ENTITIES, K_REGIONS]))
        print("+++++++++")
        ic(db._fetch_all(table=BEANS, urls=urls, columns=[K_URL, K_ENTITIES, K_REGIONS]))

    if False:
        print("=== UPDATE PUBLISHERS ===")
        publishers = db.query_publishers(limit=5)
        sources = [pub.source for pub in publishers]
        print("+++++++++")
        ic(len(publishers), db.update_publishers(generate_fake_publishers(sources, update=True)))
        print("+++++++++")
        new_publishers = ic(db.query_publishers(sources=sources))


    if False:
        print("=== UPDATE EMBEDDINGS ===")
        beans = ic(db.query_latest_beans(conditions = ["embedding IS NULL"], columns=[K_URL, K_CATEGORIES, K_SENTIMENTS], limit=5))
        urls = [bean.url for bean in beans]
        print("+++++++++")
        ic(db.update_embeddings(generate_fake_embeddings(urls)))
        print("+++++++++")
        ic(db._fetch_all(table=BEANS, urls=urls, columns=[K_URL, K_CATEGORIES, K_SENTIMENTS, K_EMBEDDING]))

    if True:
        print("=== REFRESH MATERIALIZED VIEWS ===")
        ic(db.optimize())

    if True:
        print("=== QUERY BEANS ===")
        ic(db.query_latest_beans(created=ndays_ago(360), categories=['Algorithm and Computation', 'Human Biology and Physiology'], columns=[K_URL, K_CREATED, K_TITLE, K_CATEGORIES]))
        ic(db.query_aggregated_beans(categories=['Algorithm and Computation', 'Human Biology and Physiology'], columns=[K_URL, K_TITLE, K_CATEGORIES, K_LIKES, K_COMMENTS, K_CLUSTER_SIZE, K_CLUSTER_ID]))
        ic(db.query_aggregated_beans(kind="news", embedding=QUERY_EMBEDDING, distance=10, columns=[K_URL, K_KIND, K_TITLE, K_CATEGORIES, K_LIKES, K_COMMENTS, K_CLUSTER_SIZE, K_CLUSTER_ID], limit=5))



QUERY_EMBEDDING = [0.2342979609966278, 0.6434040069580078, 0.7778109908103943, 0.47109219431877136, 0.43694204092025757, 0.3831805884838104, 0.25522226095199585, 0.603622555732727, 0.20320194959640503, 0.9400644898414612, 0.636284589767456, 0.3467329442501068, 0.24820339679718018, 0.054701656103134155, 0.6445680856704712, 0.36837542057037354, 0.8175750970840454, 0.873748242855072, 0.5009404420852661, 0.8217029571533203, 0.6393482089042664, 0.2898009717464447, 0.5819682478904724, 0.21001796424388885, 0.44594308733940125, 0.11428400874137878, 0.4681902825832367, 0.8701942563056946, 0.7539563775062561, 0.9986602067947388, 0.8228234648704529, 0.9520212411880493, 0.19817838072776794, 0.2675766348838806, 0.40467163920402527, 0.43434441089630127, 0.22141768038272858, 0.8530570864677429, 0.9348291158676147, 0.6538299322128296, 0.14671768248081207, 0.7768648862838745, 0.8836519122123718, 0.23998595774173737, 0.9404140710830688, 0.7549768090248108, 0.3498823046684265, 0.2079925835132599, 0.666752278804779, 0.9516422748565674, 0.3448159396648407, 0.2424265593290329, 0.3656389117240906, 0.5235873460769653, 0.5219000577926636, 0.7850387096405029, 0.3356684446334839, 0.5172552466392517, 0.5861392617225647, 0.45056259632110596, 0.0940907895565033, 0.3920106887817383, 0.8758504986763, 0.08634267002344131, 0.8671706914901733, 0.030458932742476463, 0.05180145800113678, 0.5383043885231018, 0.7853085398674011, 0.49641847610473633, 0.13392360508441925, 0.8909914493560791, 0.465706467628479, 0.8920135498046875, 0.7361955642700195, 0.7325476408004761, 0.023298947140574455, 0.29600757360458374, 0.017329419031739235, 0.8219354152679443, 0.7143113613128662, 0.8497188687324524, 0.7127368450164795, 0.7406572699546814, 0.6923353672027588, 0.06624365597963333, 0.010696967132389545, 0.7349875569343567, 0.034329503774642944, 0.9856587052345276, 0.7460044622421265, 0.1405796855688095, 0.08879464864730835, 0.020354559645056725, 0.7064446806907654, 0.06628287583589554, 0.0041625602170825005, 0.34515151381492615, 0.5265820622444153, 0.2515864670276642, 0.6650121212005615, 0.7966241836547852, 0.35590487718582153, 0.15967559814453125, 0.9329755902290344, 0.10623885691165924, 0.3659462630748749, 0.1280892938375473, 0.23953129351139069, 0.948611319065094, 0.007157099433243275, 0.26199519634246826, 0.32975488901138306, 0.9632959365844727, 0.3404500484466553, 0.11751426756381989, 0.22007638216018677, 0.23725572228431702, 0.6298142075538635, 0.5260857343673706, 0.35334497690200806, 0.8112795948982239, 0.13452087342739105, 0.1572822779417038, 0.8455383777618408, 0.5062063336372375, 0.5375779867172241, 0.7047982215881348, 0.8445231914520264, 0.8399357795715332, 0.4678848683834076, 0.6296774744987488, 0.49796921014785767, 0.7019550204277039, 0.6376791596412659, 0.21721749007701874, 0.20946665108203888, 0.7368452548980713, 0.9817544221878052, 0.48438823223114014, 0.8657391667366028, 0.19794413447380066, 0.37892356514930725, 0.7262741923332214, 0.9081265330314636, 0.409427285194397, 0.5276758670806885, 0.30446964502334595, 0.09329353272914886, 0.9349226355552673, 0.21359951794147491, 0.5346940159797668, 0.7522802352905273, 0.3888498544692993, 0.1322525441646576, 0.6737202405929565, 0.6561363339424133, 0.053257737308740616, 0.3897663950920105, 0.8594342470169067, 0.9773942828178406, 0.6915591955184937, 0.5968119502067566, 0.7805699706077576, 0.12145873159170151, 0.3269588351249695, 0.5235165357589722, 0.2564924359321594, 0.00443405844271183, 0.7098672986030579, 0.21324177086353302, 0.20806008577346802, 0.09881781786680222, 0.611947238445282, 0.512050211429596, 0.26404812932014465, 0.8047134280204773, 0.9289073348045349, 0.4708915650844574, 0.9868159294128418, 0.6532613635063171, 0.7382963299751282, 0.9319079518318176, 0.7170007228851318, 0.06377249956130981, 0.1798255443572998, 0.14676278829574585, 0.16462178528308868, 0.8903360962867737, 0.15303069353103638, 0.49605098366737366, 0.8916160464286804, 0.3927208483219147, 0.9691510200500488, 0.9461489319801331, 0.9564616084098816, 0.06669040024280548, 0.1480129510164261, 0.04983286187052727, 0.6629021763801575, 0.5688089728355408, 0.1517796516418457, 0.6586180329322815, 0.8725050687789917, 0.7480769753456116, 0.7389931678771973, 0.8424069881439209, 0.4124656319618225, 0.4659503102302551, 0.8688803911209106, 0.7219874262809753, 0.9997146725654602, 0.8036301732063293, 0.18013647198677063, 0.020064448937773705, 0.24868664145469666, 0.896973729133606, 0.6689051389694214, 0.7999356389045715, 0.523453950881958, 0.2533937990665436, 0.801508903503418, 0.09236855059862137, 0.4506012499332428, 0.6164650321006775, 0.6263551115989685, 0.5074870586395264, 0.40662580728530884, 0.2351270616054535, 0.7275728583335876, 0.4680725932121277, 0.5242088437080383, 0.20332926511764526, 0.7247822880744934, 0.5759570598602295, 0.9412417411804199, 0.6203413009643555, 0.9172621965408325, 0.09459082782268524, 0.9037403464317322, 0.2029256373643875, 0.1594650149345398, 0.4784926772117615, 0.7260258197784424, 0.6335464715957642, 0.9153514504432678, 0.16347238421440125, 0.17860189080238342, 0.6070381999015808, 0.006581870838999748, 0.3747204542160034, 0.9510931372642517, 0.15078458189964294, 0.32104483246803284, 0.03185172379016876, 0.8382594585418701, 0.09587518125772476, 0.27623116970062256, 0.9727802276611328, 0.9296330809593201, 0.7592703700065613, 0.4436655044555664, 0.10701251775026321, 0.7728281617164612, 0.5011996626853943, 0.7656568884849548, 0.9184994697570801, 0.8716582655906677, 0.9172300100326538, 0.5167863965034485, 0.3078855574131012, 0.8491021394729614, 0.8986914753913879, 0.5931780338287354, 0.9972102642059326, 0.13297224044799805, 0.18418972194194794, 0.3351830840110779, 0.5038394331932068, 0.014777778647840023, 0.09261386096477509, 0.9325172901153564, 0.18826374411582947, 0.948046088218689, 0.9369212985038757, 0.685977578163147, 0.8084359169006348, 0.9753568768501282, 0.1698966920375824, 0.2755703032016754, 0.24068056046962738, 0.5094568729400635, 0.17218102514743805, 0.5860380530357361, 0.6285922527313232, 0.44665291905403137, 0.6472154855728149, 0.3402685225009918, 0.05582307279109955, 0.3353240489959717, 0.40196484327316284, 0.6670607328414917, 0.03609346225857735, 0.24106119573116302, 0.4155484437942505, 0.8180393576622009, 0.3115175664424896, 0.6081684827804565, 0.38863474130630493, 0.16698218882083893, 0.26993221044540405, 0.3727966845035553, 0.9710465669631958, 0.37461626529693604, 0.6874709129333496, 0.007001467514783144, 0.3329027593135834, 0.21744143962860107, 0.9906328320503235, 0.08153041452169418, 0.6539635062217712, 0.6259394288063049, 0.7843993306159973, 0.021987149491906166, 0.3634469211101532, 0.6710318326950073, 0.2154671996831894, 0.5129680037498474, 0.14786376059055328, 0.9589179158210754, 0.9687715768814087, 0.8751899600028992, 0.5524784922599792, 0.07694286108016968, 0.7411525249481201, 0.3752460777759552, 0.007299432530999184, 0.16318002343177795, 0.8893817067146301, 0.08048597723245621, 0.48283642530441284, 0.7579646706581116, 0.12527117133140564, 0.6651806831359863, 0.4237237572669983, 0.10778270661830902, 0.41499143838882446, 0.7091203331947327, 0.5869641900062561, 0.12671005725860596, 0.05611551180481911, 0.2728385329246521, 0.8275564312934875, 0.09607762098312378, 0.01576598919928074, 0.28877928853034973, 0.1623099446296692, 0.4239787757396698, 0.1646648496389389, 0.8278176188468933, 0.9743233919143677, 0.5218372941017151, 0.42087823152542114, 0.9528033137321472, 0.6254134178161621, 0.6282122731208801, 0.3151690661907196, 0.8579148650169373, 0.40704119205474854, 0.24768151342868805, 0.3728760778903961, 0.3872365653514862, 0.24365180730819702, 0.6849273443222046, 0.802613377571106, 0.9347347617149353, 0.8723124265670776, 0.33460676670074463, 0.02049080841243267, 0.49225980043411255, 0.5385429263114929, 0.04310426115989685, 0.29661494493484497, 0.050668030977249146]