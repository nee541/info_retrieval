import argparse
try:
    from search.retrieval.core.utils import *
except ModuleNotFoundError:
    from core.utils import *
from functools import reduce

BJ_NEWS_FLODER = os.path.join(os.path.dirname(__file__), 'spider/bj_news')

def inverted_index(pool, folder: str=BJ_NEWS_FLODER):
    r = redis.Redis(connection_pool=pool)
    indies = {}
    art_cnt = 0
    for name, lines in get_articles(folder):
        art_cnt += 1
        seg_cnt = 0
        r.sadd('__docs__', name)
        for seg, pos in get_segs(lines):
            if pos != 'wp':
                # 非标点符号
                r.sadd(f'{name}:set', base64.b64encode(seg.encode()).decode())
                seg_cnt += 1
                if seg not in indies:
                    indies[seg] = {
                        name: [seg_cnt]
                    }
                else:
                    tmp = indies[seg]
                    if name in tmp:
                        tmp[name].append(seg_cnt)
                    else:
                        tmp[name] = [seg_cnt]
        r.hset(name, '__len__', seg_cnt)
        r.hset(name, '__url__', name_to_url(name))

    r.set('__total_docs__', art_cnt)
    for item, ind in indies.items():
        b64item = base64.b64encode(item.encode()).decode()
        r.sadd('__terms__', b64item)
        b64_hash = f'{b64item}:hash'
        r.hset(b64_hash, '__len__', len(ind))
        for doc, i in ind.items():
            r.hset(b64_hash, doc, len(i))
        r.set(f'{b64item}:str', json.dumps(ind))

def tf_idf(pool, docs:dict=None, scheme='dt'):
    assert scheme in ['dt', 'qt'], 'scheme must be one of [dt, qt], more details at https://en.wikipedia.org/wiki/Tf%E2%80%93idf#Term_frequency%E2%80%93inverse_document_frequency'
    r = redis.Redis(connection_pool=pool)
    if scheme == 'dt':
        for doc in r.sscan_iter('__docs__'):
            doc = doc.decode()
            sum = 0
            for b64term in r.sscan_iter(f'{doc}:set'):
                b64term = b64term.decode()
                val = TF(b64term, doc, pool, scheme='tf') * IDF(b64term, pool, scheme='idf')
                if not math.isclose(val, 0):
                    r.hset(doc, b64term, val)
                    sum += val * val
            r.hset(doc, '__norm__', math.sqrt(sum))
    elif scheme == 'qt':
        rets = {}
        for term in docs:
            b64term = base64.b64encode(term.encode()).decode()
            rets[term] = TF(term, docs, pool, scheme='dnp5') * IDF(b64term, pool, scheme='idf')
        return rets

def clear(pool):
    r = redis.Redis(connection_pool=pool)
    r.flushdb()

def get_relevances(query: dict, pool):
    r = redis.Redis(connection_pool=pool)
    query_weight = tf_idf(pool, query, scheme='qt')
    query_norm = math.sqrt(reduce(lambda x, y: x * x + y * y, query_weight.values()))
    if math.isclose(query_norm, 0):
        return {}
    relevances = {}
    for doc in r.sscan_iter('__docs__'):
        doc = doc.decode()
        dot_mult = 0
        for term, weight in query_weight.items():
            b64term = base64.b64encode(term.encode()).decode()
            if r.hexists(doc, b64term):
                dot_mult += weight * float(r.hget(doc, b64term))
        relevances[doc] = dot_mult / (query_norm * float(r.hget(doc, '__norm__')))
    return relevances

def main(pool, max_display=5):
    text = input('请输入查询词：')
    r = redis.Redis(connection_pool=pool)
    while text != 'exit':
        docs = {}
        for seg, pos in get_segs([f'{text}']):
            if pos != 'wb':
                if seg not in docs:
                    docs[seg] = 1
                else:
                    docs[seg] += 1
        relevances = get_relevances(docs, pool)
        relevances = dict(sorted(relevances.items(), key=lambda x: x[1], reverse=True))
        cnt = 0
        for doc, rel in relevances.items():
            if cnt >= max_display or math.isclose(rel, 0):
                break
            print(f'{doc} \t{rel} \t{r.hget(doc, "__url__").decode()}')
            cnt += 1
        text = input('请输入查询词：')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Information Retrieval')
    parser.add_argument('-f', '--folder', default=BJ_NEWS_FLODER, help='folder to crawl')
    parser.add_argument('-o', '--host', default='localhost', help='redis host')
    parser.add_argument('-p', '--port', default=6379, help='redis port')
    parser.add_argument('-d', '--db', default=0, help='redis db')
    parser.add_argument('-r', '--reInvert', action='store_true', help='re-invert index')
    args = parser.parse_args()
    pool = redis.ConnectionPool(host=args.host, port=args.port, db=args.db)
    r = redis.Redis(connection_pool=pool)
    if args.reInvert:
        clear(pool)
        inverted_index(pool, args.folder)
        tf_idf(pool)
        print('inverted index done')
    else:
        main(pool)