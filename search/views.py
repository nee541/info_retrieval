from django.shortcuts import render
from search.retrieval.info_retrieval import *
import time

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

MIN_REL = 0.00000

def index(request):
    return render(request, 'index.html')

def search(request):
    if request.method == 'POST':
        start_t = time.time()
        r = redis.Redis(connection_pool=pool)
        search = request.POST['search']
        final_result = []

        query = {}
        for seg, pos in get_segs([f'{search}']):
            if pos != 'wb':
                if seg not in query:
                    query[seg] = 1
                else:
                    query[seg] += 1
        relevances = get_relevances(query, pool)
        relevances = dict(sorted(relevances.items(), key=lambda x: x[1], reverse=True))
        
        for doc, rel in relevances.items():
            if rel < MIN_REL:
                break
            url = r.hget(doc, "__url__").decode()
            line, keywords = get_article_text(list(query.keys()), f'{doc}.txt', BJ_NEWS_FLODER)
            final_result.append((url, line, f'相关度：{rel:.3f} 关键词：' + ','.join(keywords)))
        
        timecost = time.time() - start_t
        context = {
            'final_result': final_result,
            'query': search,
            'timecost': f'{timecost:.3f}',
            'number': len(final_result)
        }
        return render(request, 'search.html', context)
    else:
        return render(request, 'search.html')
