import requests
import time

def fetch_url(url):
    try:
        start = time.time()
        r = requests.get(url)
        end = time.time()
        diff = end - start
        try:
            resp = r.content
            print(diff, url)
            return diff, url
        except ValueError:
            # print 'Junk response'
            return None, url
    except requests.exceptions.Timeout as e:
        print('Request timeout %s' % url)
        return None, url
    except requests.exceptions.ConnectionError as e:
        print('Connection reset: %s' % url)
        return None, url

def do_committees():
    base_url = 'http://sunshine.datamade.us'

    pool = Pool(processes=16)
    
    ids = engine.execute('select id from committees')

    all_resp = []

    for d in range(32):
        
        resps = pool.map(fetch_url, ['%s/committees/%s/' % (base_url, c[0]) for c in ids])
        all_resp.extend(resps)
    
    return all_resp

def do_candidates():
    base_url = 'http://illinoissunshine.org'

    pool = Pool(processes=16)
    
    ids = engine.execute('select first_name, last_name from candidates')

    all_resp = []

    for d in range(32):
        
        resps = pool.map(fetch_url, ['%s/api/advanced-search/?term=%s %s' % \
                            (base_url, c.first_name, c.last_name) for c in ids])
        all_resp.extend(resps)
    
    return all_resp

def do_receipts():
    base_url = 'http://sunshine.datamade.us'

    pool = Pool(processes=16)
    
    ids = engine.execute('select id from condensed_receipts order by RANDOM() limit 20000')

    all_resp = []

    for d in range(32):
        
        resps = pool.map(fetch_url, ['%s/contributions/%s/' % (base_url, c[0]) for c in ids])
        all_resp.extend(resps)
    
    return all_resp

def do_expenditures():
    base_url = 'http://sunshine.datamade.us'

    pool = Pool(processes=16)
    
    ids = engine.execute('select id from condensed_expenditures order by RANDOM() limit 20000')

    all_resp = []

    for d in range(32):
        
        resps = pool.map(fetch_url, ['%s/expenditures/%s/' % (base_url, c[0]) for c in ids])
        all_resp.extend(resps)
    
    return all_resp

if __name__ == "__main__":
    from multiprocessing import Pool
    from itertools import groupby
    from operator import itemgetter
    from sunshine.database import engine
    
    import sys
    
    all_resp = []

    if sys.argv[1] == 'committees':
        all_resp = do_committees()
    
    elif sys.argv[1] == 'candidates':
        all_resp = do_candidates()
    
    elif sys.argv[1] == 'receipts':
        all_resp = do_receipts()

    elif sys.argv[1] == 'expenditures':
        all_resp = do_expenditures()

    if all_resp:
        all_resp = sorted(all_resp, key=itemgetter(1))
        all_times = {}
        best = 180
        worst = 0
        fastest_url = ''
        slowest_url = ''
        for k,g in groupby(all_resp, key=itemgetter(1)):
            good_times = [i[0] for i in list(g) if i]
            bad_times = len([i[0] for i in list(g) if i is None])
            best_time, worst_time = good_times[0], good_times[-1]
            if best_time < best:
                best = best_time
                fastest_url = k
            if worst_time > worst:
                worst = worst_time
                slowest_url = k
        print('Fastest: %s (%s)' % (fastest_url, best))
        print('Slowest: %s (%s)' % (slowest_url, worst))
