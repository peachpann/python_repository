# encoding=utf-8

import urllib
from bs4 import BeautifulSoup
import chardet
import pymongo
import threading
import re
import pdbx
import time
import urllib2
import md5

pdbx.enable_pystack()
get_time_total = 0


def get_info(url):  # each_url,get_extract return dict_info
    info = {}

    if urllib.urlopen(url).code != 200:
        return {}

    get_t1 = time.time()
    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html;q=0.9,*/*;q=0.8',
    }
    req_timeout = 0.5
    try:
        req = urllib2.Request(url, None, req_header)
        resp = urllib2.urlopen(req, None, req_timeout)
        content = resp.read()
    except:
        logBad(url)
        return {}

    get_t2 = time.time()
    global get_time_total
    get_time_total += (get_t2 - get_t1)

    extract_t1 = time.time()
    det_t1 = time.time()

    encoding = det_encoding(content)
    content = content.decode(encoding=encoding, errors='ignore')

    det_t2 = time.time()
    soup = BeautifulSoup(content, "html.parser")
    soup_des = soup.find("meta", attrs={"name": re.compile('description.*', re.I)})
    soup_kw = soup.find("meta", attrs={"name": re.compile('keyword.*', re.I)})
    soup_tags = soup.find("meta", attrs={"name": re.compile('tag.*', re.I)})

    try:
        info['title'] = soup.title.text
    except:

        logBad(url)
        print 'no title'

    if soup_des != None:

        try:
            info['description'] = soup_des['content'].strip()
        except:
            print 'no desc'

    if (soup_kw != None):
        try:
            info['keywords'] = soup_kw['content'].strip()
        except:
            print 'no keywords'

    if (soup_des != None):
        try:
            info['tags'] = soup_tags['content'].strip()
        except:
            print 'no tags'

    extract_t2 = time.time()

    print 'url:' + url + '   get:' + str(get_t2 - get_t1) + '   extract:' + str(
        extract_t2 - extract_t1) + '   det:' + str(det_t2 - det_t1)

    return info


crowler_count = 0;


def list_extract(f, fout):  # list_url filter and save2mongo
    global crowler_count
    global get_time_total

    sites = f
    count = 0
    count_saved = 0

    for j in sites:
        count += 1
        crowler_count += 1

        if crowler_count % 1000 == 0:
            print 'crowler_count=', str(crowler_count), 'total=', str(crowler_total), 'get_time=', str(get_time_total)
            get_time_total = 0

        if isExist(j):
            write_to_file(fout, j)
            continue

        try:
            write_to_file(fout, j)
            info = get_info(j)
            # record has run

            if info.has_key('title') and info['title'] != '':

                m1 = md5.new()
                m1.update(j)
                info['_id'] = m1.hexdigest()
                info['url'] = j

                c = pymongo.MongoClient("10.10.23.101", 50001)
                db = c.recbrain
                db['user_behavior0816'].insert(info)
                c.close()

                print 'inserted: ', j, info['_id']
                count_saved += 1

            else:
                print 'no title'
        except:
            print 'can not save'


def logBad(url):
    with open('/home/firedata/pantao/bad_url_0819', 'a') as f:
        f.writelines(urllib2.urlparse.urlparse(url).netloc + '\n')


def splist(l, n):
    s = len(l) / (n - 1)

    return [l[i:i + s] for i in range(len(l)) if i % s == 0]


filter_hosts = [r"[\d\.]*",
                r"msg\.71\.am",
                r"msg\.iqiyi\.com",
                r".*\.pdl\.wow\.battlenet\.com\.cn",
                r"\.rcd\.iqiyi\.com",
                r".*log.*",
                r".*cdn.*",
                r".*cnzz\.com",
                r".*baidupcs\.com",
                r".*\.akamaihd\.net",
                r"ubiz\.yy\.com",
                r"olime\.baidu\.com",
                r"image\.yy\.com",
                r"api\.pallas\.tgp\.qq\.com",
                r".*\.ykimg\.com",
                r"stat\.funshion\.net",
                r"btrace\.video\.qq\.com",
                r"t.*\.baidu\.com",
                r"hb\.crm2\.qq\.com",
                r"t7z\.cupid\.iqiyi\.com",
                r"p\.tanx\.com",
                r".*\.img4399\.com",
                r".*\.qpic\.cn",
                r"trackercdn\.kugou\.com",
                r"pos\.baidu\.com",
                r"wn\.pos\.baidu\.com",
                r"eclick\.baidu\.com",
                r"im-x\.jd\.com",
                r"down\.qq\.com",
                r"tce\.alicdn\.com",
                r"log\.houyi\.baofeng\.net",
                r"isdspeed\.qq\.com",
                r"log\.v2\.hunantv\.com",
                r".*\.taobaocdn\.com",
                r"dq\.baidu\.com",
                r"res\.mir\.37wan\.com",
                r"js\.189\.cn",
                r"pcweb\.v1\.mgtv\.com",
                r"images\.sohu\.com",
                r".*\.sogoucdn\.com",
                r".*\.cdn\.baidupcs\.com",
                r"auth\.wifi\.com",
                r"uestat\.video\.qiyi\.com/"
                r".*\.cnzz\.com"
                r"cdn\.tgp\.qq\.com",
                r"apple\.www\.letv\.com|ope/.tanx/.com|uestat/.video/.qiyi/.com|dingshi4pc/.qiumibao/.com|pingfore/.qq/.com/",   #0819 add filter rules
                r"chat.chushou.tv"
                r"l\.rcd\.iqiyi\.com",
                r"show\.re\.taobao\.com|s\.360\.cn|.*synacast\.com|dan\.zhibo8\.cc|googleads\.g\.doubleclick\.net|afp\.csbew\.com|bifen4pc\.qiumibao\.com"]


def acceptUrl(url):
    u = urllib2.urlparse.urlparse(url)
    return re.match(r"^(" + ("|".join(filter_hosts)) + ")$", u.netloc) is None


def det_encoding(content):
    m = re.search(r'(utf-8|gb2312|gbk|windows-1252|ISO-8859-8)', content, re.I)
    if m is not None:
        return m.group().lower();

    return 'GB18030'


def isExist(url):
    c = pymongo.MongoClient("10.10.23.101", 50001);
    db = c.recbrain;
    e = db['user_behavior0816'].find({'_id': md5.md5(url).hexdigest()}).count()
    c.close();
    return e != 0


lock = threading.Lock()


def write_to_file(f, text):
    lock.acquire()  # thread blocks at this line until it can obtain lock

    # in this section, only one thread can be present at a time.
    print >> f, text

    lock.release()


f = open('/home/firedata/pantao/url_0819_top')

sites = []
has_run = []

with open('/home/firedata/pantao/list_url_finish', 'r') as f1:
    for i in f1:
        has_run.append(i[:-1])

set_has_run = set(has_run)

for i in f:
    i = i[:-1]
    if acceptUrl(i) and i not in set_has_run:
        sites.append(i)

crowler_total = len(sites)

sitess = splist(sites, 1000)

threads = []

f_list_url_finish = open('/home/firedata/pantao/list_url_finish', 'a')

for i in range(1000):
    print i
    t = threading.Thread(target=list_extract, args=(sitess[i], f_list_url_finish))
    threads.append(t)

for t in threads:
    t.setDaemon(True)
    t.start()

for t in threads:
    t.join()

f_list_url_finish.close()

print 'end'
