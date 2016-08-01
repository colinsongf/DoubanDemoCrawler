# coding:utf-8

"""
redis (2.10.5)
requests (2.9.1)
python (2.7.10)

"""

import requests
import re
import redis
import logging
from Queue import Queue
from Queue import Full
import pdb
import json
from general import *

# 日志的配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(threadName)s %(name)s %(levelname)s  %(message)s')
logger = logging.getLogger(__name__)

# 简单的agent 还是要用的,防止被封
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/51.0.2704.106 Safari/537.36'
}

# 本机redis实例配置
redis_client = redis.Redis(host='localhost', port=6379)

# 已经抓取过的种子列表
visited_key_ids = set()
# 添加到set的锁,防止竞争
add_lock = threading.Lock()

# 推送到redis 队列, 支持100万
tmp_queue = Queue(100 * 10000)

# 豆瓣正则扣取配置
key_id_pattern = re.compile('[\/|"|\s](?P<key_id>\d{8})[\/|"|\s]')

# 最大爬取深度
MAX_DEPTH = 10

DEBUG = False


class MovieItem(object):
    """一部电影的基本信息描述"""

    def __init__(self, arg):
        super(MovieItem, self).__init__()
        self.arg = arg


class CrawlTask(object):
    """爬虫任务描述"""

    DOUBAN_ID = 1  # 由于暂时只考虑豆瓣抓取, 所以固定了部分抓取参数值
    DOUBAN_HOST = 'www.douban.com'
    DOUBAN_SEED_FORMAT = 'https://movie.douban.com/subject/%s'  # 豆瓣电影链接源格式

    def __init__(self, site_id, site_host, site_seed_format, key_id, crawl_depth, source_url):
        super(CrawlTask, self).__init__()
        self.site_id = site_id if site_id else self.DOUBAN_ID
        self.site_host = site_host if site_host else self.DOUBAN_HOST
        self.site_seed_format = site_seed_format if site_seed_format \
            else self.DOUBAN_SEED_FORMAT
        self.crawl_depth = crawl_depth  # 记录在多少层才抓取到这个链接的
        self.key_id = key_id  # 种子Id
        self.source_url = source_url  # 来自于哪个页面
        self._crawl_time = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime())  # 记录抓取时间

    @property
    def crawl_time(self):
        return self._crawl_time

    @crawl_time.setter
    def crawl_time(self, value):
        self._crawl_time = value

    # 获取要访问的种子链接
    def get_url(self):
        return self.site_seed_format % self.key_id

    def __str__(self):
        return '网站:%s; 种子地址:%s; 抓取时间: %s; 深度:%s; 来源于:%s' % \
               (self.site_host, self.get_url(),
                str(self._crawl_time), self.crawl_depth, self.source_url)

    # 转成json
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    # json 转成对应的对象object
    @staticmethod
    def json_decoder(obj):
        # 直接反序列化
        # if '__type__' in obj and obj['__type__'] == 'CrawlTask':
        task = CrawlTask(None, None, None, str(obj['key_id']),
                         int(obj['crawl_depth']), str(obj['source_url']))
        task.crawl_time = obj['_crawl_time']
        return task

    # 怎么样可以判断为同一个任务; 对应网站Id一样,且 种子Id一样就可以认为是同一个任务
    def __eq__(self, other):
        if self.site_id == other.site_Id and self.key_id == other.key_id:
            return True
        return False


# 抓取进程， 主要负责访问种子链接，生成种子内容
def crawl():
    while True:
        crawl_task_str = fetch_task()
        if crawl_task_str is None:
            logger.info('[抓取页面] 暂时没有抓取任务')
            time.sleep(0.1)  # sleep 100毫秒
            continue
        else:
            crawl_task = json.loads(
                crawl_task_str, object_hook=CrawlTask.json_decoder)
            if DEBUG:
                pdb.set_trace()
            url = crawl_task.get_url()
            current_crawl_depth = crawl_task.crawl_depth  # 当前的深度
            # 达到最大深度, 不予抓取
            if current_crawl_depth >= MAX_DEPTH:
                logger.info('[抓取页面] 任务:%s 达到最大深度; 放弃抓取', url)
                continue
            try:
                while rate_limit(40, 1, 'M'):
                    # 请求过于频繁,超过每分钟40次了
                    logger.info('[抓取页面]请求过于频繁,超过每分钟40次了')
                    time.sleep(0.1)
                logger.info('[抓取页面] 开始抓取网址:%s', url)
                res = requests.get(url=url, headers=HEADERS)
                status_code = res.status_code
                if status_code == 200:
                    # 提取页面上所有的key_id
                    key_id_match = key_id_pattern.finditer(res.content)
                    if key_id_match:
                        key_id_list = map(lambda x: x.group('key_id'),
                                          [key_id_single_match for key_id_single_match in key_id_match])
                        logger.info('[抓取页面] 在页面[%s] 抓取到种子数:%s 个; ID列表为:%s', url, len(
                            key_id_list), ','.join(key_id_list))
                        effective_task_ids = duplicate_key_id(
                            key_id_list=key_id_list)
                        logger.info('[抓取页面] 在页面:[%s] 抓取到[有效]任务种子数:%s 个; 任务ID为:%s', url,
                                    effective_task_ids.__len__(),
                                    ','.join(effective_task_ids))
                        for effective_task_id in effective_task_ids:
                            # 生成任务实体,同时记录爬取深度及源地址
                            tmp_crawl_task = CrawlTask(None, None, None, effective_task_id,
                                                       crawl_task.crawl_depth + 1,
                                                       url)
                            retry_times = 0
                            while retry_times <= 5:
                                try:
                                    tmp_queue.put(tmp_crawl_task, block=True)
                                    break
                                except Full:
                                    retry_times += 1
                                    logger.error(
                                        '[抓取页面] 推任务:%s到临时队列失败, 队列已满; 第%s次尝试;', tmp_crawl_task, retry_times)
                                    time.sleep(0.1)  # sleep 100毫秒, 重试五次
                else:
                    logger.warn('[抓取页面] 抓取网址:%s  响应码:[%s] ', url, status_code)
            except requests.exceptions.RequestException as e:
                logger.exception('[抓取页面] 抓取网址:%s  失败;', url, e)


# 获取抓取任务(先只考虑每次只pop一个任务)
def fetch_task():
    try:
        start = time.time()
        crawl_task = redis_client.rpop('crawler:douban:tasks')
        # if DEBUG:
        #     pdb.set_trace()
        end = time.time()
        logger.info('[获取任务] 获取抓取任务耗时%s毫秒',
                    format_time(end - start))  # 埋点用作以后的优化
        return crawl_task
    except (redis.ConnectionError, redis.TimeoutError) as e:
        logger.exception('[获取任务] 失败;', e)
    return None


# key_id去重
def duplicate_key_id(key_id_list):
    # 去重过的种子列表
    task_id_list = []
    if key_id_list is None or len(key_id_list) == 0:
        return task_id_list
    with add_lock:
        for key_id in key_id_list:
            if key_id not in visited_key_ids:
                visited_key_ids.add(key_id)
                logger.info('[抓取页面] 目前已经抓取到[%s] 个种子了', len(visited_key_ids))
                task_id_list.append(key_id)
    return task_id_list


#  从内存queue中取任务推到redis中
def push_to_queue():
    while True:
        # 木有推送任务, sleep 1000ms
        if tmp_queue.empty():
            logger.info('[推送任务] 暂时没有推送任务')
            time.sleep(1)
        else:
            tmp_crawl_task = None
            try:
                tmp_crawl_task = tmp_queue.get()
                redis_client.lpush('crawler:douban:tasks', tmp_crawl_task.to_json())
                logger.info('[推送任务] 推到redis成功; 任务信息:%s', tmp_crawl_task)
            except (Exception) as e:
                logger.exception(
                    '[推送任务] 写入redis失败; 任务信息:%s', tmp_crawl_task, e)


# 初始化一个任务种子
def init_crawl_task():
    crawl_task = CrawlTask(None, None, None, '25850122', 0, '')
    visited_key_ids.add('25850122')  # 第一个页面访问过了,也不用访问
    redis_client.lpush('crawler:douban:tasks', crawl_task.to_json())


# 初始化所有的worker线程
def init_work_threads(crawl_thread_num, push_thread_num):
    all_threads = []
    # 初始化抓取线程
    for i in range(crawl_thread_num):
        t = threading.Thread(target=crawl)  # 参数以元组形式传递给线程
        all_threads.append(t)
    # 初始化推送线程
    for i in range(push_thread_num):
        push_thread = threading.Thread(target=push_to_queue)
        all_threads.append(push_thread)
    for t in all_threads:
        t.start()
    for t in all_threads:
        t.join()


if __name__ == '__main__':
    logger.info('[豆瓣]爬取start')
    init_crawl_task()
    init_work_threads(1, 1)
