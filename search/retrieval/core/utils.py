import re
import os
import json
import redis
import math
import base64
from pathlib import Path
from ltp import LTP

USER_DICT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'user_dict.txt')

ltp = LTP()
ltp.init_dict(path=USER_DICT, max_window=4)

# https://blog.csdn.net/qq_41895003/article/details/105743077
POS_WEIGHT = {
    'r':    0.5 ,
    'n':    1   ,
    'ns':   1   ,
    'wp':   0.1 ,
    'k':    0.5 ,
    'h':    0.5 ,
    'u':    0.5 ,
    'c':    0.5 ,
    'v':    1   ,
    'p':    0.5 ,
    'd':    0.5 ,
    'q':    0.5 ,
    'nh':   1   ,
    'm':    0.5 ,
    'e':    0.5 ,
    'b':    0.5 ,
    'a':    1   ,
    'nd':   1   ,
    'nl':   1   ,
    'o':    0.5 ,
    'nt':   1   ,
    'nz':   1   ,
    'ni':   1   ,
    'i':    1   ,
    'j':    1   ,
    'ws':   1   ,
    'g':    1   ,
    'x':    1   ,
    'z':    1   ,
    '%':    1   ,
}

BJ_NEWS_NAME = re.compile(r'(\d+)-(\d+)-([\w\d-]+)(\.txt|)')

def name_to_url(name: str):
    m = re.match(BJ_NEWS_NAME, name)
    return f'http://bj.people.com.cn/n2/{m[1]}/{m[2]}/{m[3]}.html'

def get_segs(sentences: list):
    sents = ltp.sent_split(sentences)
    step = 5
    for i in range(0, len(sents), step):
        segment, hidden = ltp.seg(sents[i:i+step])
        pos = ltp.pos(hidden)
        for segs, ps in zip(segment, pos):
            for seg, p in zip(segs, ps):
                yield seg, p

def get_weight(seg_pos: str):
    return POS_WEIGHT.get(seg_pos, 1)

def get_articles(folder: str, name: str='.*\.txt'):
    # yield name and list of line
    name = re.compile(name)
    for file_name in os.listdir(folder):
        if re.match(name, file_name):
            with open(os.path.join(folder, file_name), 'r', encoding='utf-8') as f:
                yield Path(file_name).stem, list(filter(lambda x : (x != '\r\n' and x != '\n'), f.readlines()))

def get_article_title(name, folder: str):
    with open(os.path.join(folder, name), 'r', encoding='utf-8') as f:
        line = f.readline().strip()
        while len(line) == 0:
            line = f.readline().strip()
        return line

def get_article_text(keys: list, name, folder: str):
    with open(os.path.join(folder, name), 'r', encoding='utf-8') as f:
        keywords = []
        max_cnt = 0
        result = ''
        for line in f.readlines():
            cnt = 0
            cur_keywords = []
            line = line.strip()
            for key in keys:
                if key in line:
                    cnt += 1
                    cur_keywords.append(key)
            if cnt > max_cnt:
                max_cnt = cnt
                result = line
                keywords = cur_keywords
    return result, keywords

def TF(term, docs, conn_pool, scheme='tf') -> float:
    assert scheme in ['b', 'rc', 'tf', 'ln', 'dnp5', 'dnk'], 'scheme must be one of [b, rc, tf, ln, dnp4, dnk], more details at https://en.wikipedia.org/wiki/Tf%E2%80%93idf#Term_frequency_2'
    r = redis.Redis(connection_pool=conn_pool)
    if scheme == 'tf':
        # term 是b64编码encode后的
        b64_hash_term = f'{term}:hash'
        if r.hexists(b64_hash_term, docs):
            return int(r.hget(b64_hash_term, docs)) / int(r.hget(docs, '__len__'))
        else:
            return 0
    elif scheme == 'dnp5':
        # docs保存键值对
        return 0.5 + 0.5 * docs[term] / docs[max(docs, key=docs.get)]


def IDF(term, conn_pool, scheme='idf') -> float:
    assert scheme in ['u', 'idf', 'idfs', 'idfm', 'pidf'], 'scheme must be one of [u, idf, idfs, idfm, pidf], more details at https://en.wikipedia.org/wiki/Tf%E2%80%93idf#Inverse_document_frequency_2'
    r = redis.Redis(connection_pool=conn_pool)
    if scheme == 'idf':
        # term 是b64编码encode后的
        try:
            total_docs = int(r.get('__total_docs__'))
            num_having_term = int(r.hget(f'{term}:hash', '__len__'))
        except TypeError as e:
            # print(e)
            # print(f'IDF: {term} does not have __len__')
            # exit(1)
            return 0
        return math.log(total_docs / num_having_term)

if __name__ == '__main__':
    # for sentences in get_articles():
    #     for seg, pos in get_segs(sentences):
    #         print(seg, pos)
    # with open(os.path.join('spider/bj_news', '2021-0719-c14540-34826580.txt'), 'r', encoding='utf-8') as f:
    #     print(list(get_segs([f.readline()])))
    print(name_to_url('2021-0706-c82841-34808290'))
    print(name_to_url('2021-0706-c82841-34808290.txt'))