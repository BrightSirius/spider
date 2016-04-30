#encoding=utf-8
import os, sys, json, logging, copy
import urllib2, threading
from bs4 import BeautifulSoup as BS

from easy_tool import EasyTool as ET

reload(sys)
sys.setdefaultencoding('utf-8')

linfo = logging.info
ldebug = logging.debug
lexcept = logging.exception

local_lock = threading.Lock()
task_lock = threading.Lock()

total_tasks = set() 
record_tasks = set()

def write_records(path, mode, txt):
    local_lock.acquire()
    ET.write_file(path, mode, txt)
    local_lock.release()

def modify_tasks(new_tasks=set()):
    task_lock.acquire()
    global total_tasks
    total_tasks |= new_tasks
    task_lock.release()

class SingleThreadSpider(threading.Thread):
    def __init__(self, tid, local_path, tasks, sub_url=None):
        threading.Thread.__init__(self)

        self.thread_id = tid
        self._local_path = local_path
        self.tasks=tasks
        self.sub_url = sub_url
    
    def run(self):
        ldebug('thread_id %s start' % self.thread_id)
        if not self.tasks:
            return
        new_urls = set()
        for url in self.tasks:
            ret = self._parse_page(url)
            if ret:
                new_urls |= ret
        modify_tasks(new_urls)

    def _parse_page(self, url):
        global record_tasks
        ldebug('parse url: %s' % url)
        try:
            response = urllib2.urlopen(url, timeout=3)
            page = response.read()
            page = self._encode(url, page)
            if not page:
                return None
            soup = BS(page, 'html.parser')
            #people news.
            #title_tags = soup.find_all(id="p_title")

            #cankaoxiaoxi news.
            title_tags = soup.find_all('h1')
            title_tags = filter(lambda x:  x.get('class') and isinstance(x.get('class'), list) and ' '.join(x.get('class')) == 'h2 fz-25 YH', title_tags)

            if title_tags:
                #linfo('title: %s' % title_tags[0].string)
                for tag in title_tags:
                    title = tag.string
                    if title:
                        #ET.write_file(self._local_path, 'a', '%s,%s\n' % (title.replace(',','，'), url))
                        write_records(self._local_path, 'a', '%s,%s\n' % (title.replace(',','，').strip(), url.strip()))
                        ldebug('title: %s found' % title)
                return None
            link_tags = soup.find_all('a')
            link_tags = filter(lambda x: x.get('href'), link_tags)
            if link_tags:
                new_urls=set()
                for tag in link_tags:
                    new_url = tag['href']
                    if 'http' not in new_url:
                        end = url[8:].find('/')
                        root_url = url if end == -1 else url[:end+8]
                        new_url = '%s%s'%(root_url, new_url) if new_url[0] == '/' else '%s/%s' % (root_url, new_url)
                    if self.sub_url not in new_url:
                        continue
                    if new_url not in record_tasks:
                        new_urls.add(new_url)
                ldebug('%s new urls found' % len(new_urls))
                return new_urls
            return None
        except Exception as e:
            lexcept("Exception: %s" % e)
            return None

    def _encode(self, url, txt, try_decode=['utf-8', 'gbk']):
        for code in try_decode:
            try:
                txt = txt.decode(code).encode('utf-8')
                return txt
            except UnicodeDecodeError:
                pass
                #ldebug('DecodeError!')
        linfo('Decode Fail. page from url:%s' % url)
        return None


class Spider(object):
    def __init__(self, src_url='None', local_path='None', depth=None, keyword=''):
        if not src_url or not local_path or not depth:
            raise Exception('INVALID PARAMETER')
        #self._src_url = 'http://www.people.com.cn/'
        self._src_url =  src_url
        self._keyword = keyword

        self._depth = depth
        self._local_path = local_path
        global total_tasks
        total_tasks.add(self._src_url)
       
    def run(self, worker=100, debug=False):
        global record_tasks
        if not debug:
            record_tasks = self._load_local()
        dep = 0
        while dep < self._depth:
            linfo('%s depth spider begin...tasks cnt: %s' % (dep, len(total_tasks)))
            self.dispatch_tasks(worker)
            linfo('%s depth spider end.' % (dep))
            dep += 1
    
    def dispatch_tasks(self, workers_cnt):
        if workers_cnt < 1:
            raise Exception('INVALID WORKER GIVEN')
        global total_tasks, record_tasks
        tasks = filter(lambda x: x not in record_tasks, total_tasks)
        total_tasks = set() 
        worker2tasks = [[] for i in range(workers_cnt)]
        for i, task in enumerate(tasks):
            sd = i % workers_cnt
            worker2tasks[sd].append(task)
            record_tasks.add(task)

        workers = []
        for i, task in enumerate(worker2tasks):
            worker = SingleThreadSpider(i, self._local_path, task, self._keyword)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()

    def _load_local(self):
        linfo('begin load local records')
        urls = set()
        if not os.path.exists(self._local_path):
            return urls
        with open(self._local_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                tokens = line.split(',')
                if len(tokens) != 2:
                    lexcept('NOT VALID LOCAL RECORD: %s' % line)
                    raise Exception('NOT VALID LOCAL RECORD: %s' % line)
                urls.add(tokens[1])
        linfo('end load local records. %s found.' % len(urls))
        return urls
    
    def test(self):
        res = urllib2.urlopen(self._src_url)
        print type(res)
        print type(res.headers)
        print res.info()
        print res.headers
        print res.headers['content-type']
        print res.headers.getparam('charset')
        

def main():
    #src_url = 'http://www.cankaoxiaoxi.com/'
    #keyword ='people.com'

    src_url = 'http://www.cankaoxiaoxi.com/'
    keyword = 'cankaoxiaoxi.com'

    #src_url = 'http://www.xinhuanet.com/'
    #keyword = 'xinhuanet.com'


    config={"depth":5, 'src_url':src_url, 'local_path':'data/cankao_records', 'keyword':keyword}
    spider = Spider(**config)
    spider.run(worker=200)
    #spider.test()


if __name__ == '__main__':
    logging.basicConfig(filename='/home/lizhitao/log/spider.log',format='%(asctime)s %(levelname)s %(message)s',level=logging.DEBUG)
    linfo('-----------------')
    logging.info('begin supervise')
    main()
    logging.info('end')
