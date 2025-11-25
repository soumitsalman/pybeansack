from dotenv import load_dotenv
load_dotenv()

import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from icecream import ic

# Import from package using relative imports
from .. import mongosack, lancesack, warehouse
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

get_test_warehouse = lambda: warehouse.Beansack()

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
        cursor = beansack.sourcestore.find(skip=offset, limit=BATCH_SIZE)
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
    db = pgsack.create_db(os.getenv('PG_CONNECTION_STRING'), os.getenv('FACTORY_DIR'))
    
    if True:
        ic(db.count_rows(BEANS))
        ic(db.store_beans(generate_fake_beans(ai_fields=False, limit=512)))
        ic(db.count_rows(BEANS))

    if True:
        beans = generate_fake_beans()
        beans[0].url = "https://wilson.biz/"
        beans[1].url = "http://clark-evans.com/"
        beans[2].url = "https://www.murphy.biz/"
        ic(len(beans), len(db.deduplicate(BEANS, beans)))

    if True:
        ic(db.count_rows(PUBLISHERS))
        ic(db.store_publishers(ic(generate_fake_publishers())))
        ic(db.count_rows(PUBLISHERS))

    if True:
        ic(db.count_rows(CHATTERS))
        chatters = generate_fake_chatters()
        chatters[0].chatter_url = "http://williams.com/"
        chatters[1].chatter_url = "http://morrow.com/"
        ic(len(chatters), db.store_chatters(chatters))
        ic(db.count_rows(CHATTERS))

    if True:
        beans = ic(db.query_latest_beans(limit=5, columns=[K_URL, K_ENTITIES, K_REGIONS]))
        urls = [bean.url for bean in beans]
        print("+++++++++")
        ic(db.update_beans(generate_fake_digests(urls), columns=[K_GIST, K_ENTITIES, K_REGIONS]))
        print("+++++++++")
        ic(db._query_beans(table=BEANS, urls=urls, columns=[K_URL, K_ENTITIES, K_REGIONS]))

    if True:
        publishers = db.query_publishers(limit=5)
        sources = [pub.source for pub in publishers]
        print("+++++++++")
        ic(db.update_publishers(ic(generate_fake_publishers(sources, update=True))))
        print("+++++++++")
        ic(db.query_publishers(sources=sources))


    if True:
        beans = ic(db.query_latest_beans(conditions = ["embedding IS NULL"], columns=[K_URL, K_CATEGORIES, K_SENTIMENTS]))
        urls = [bean.url for bean in beans]
        print("+++++++++")
        ic(db.update_embeddings(generate_fake_embeddings(urls)))
        print("+++++++++")
        ic(db._query_beans(table=BEANS, urls=urls, columns=[K_URL, K_CATEGORIES, K_SENTIMENTS]))

    if True:
        ic(db.refresh())

    if True:
        ic(db.query_latest_beans(categories=['Algorithm and Computation', 'Human Biology and Physiology'], columns=[K_URL, K_TITLE, K_CATEGORIES]))
        ic(db.query_aggregated_beans(categories=['Algorithm and Computation', 'Human Biology and Physiology'], columns=[K_URL, K_TITLE, K_CATEGORIES, K_LIKES, K_COMMENTS, K_CLUSTER_SIZE, K_CLUSTER_ID]))
