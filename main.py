# coding:utf-8

"""
redis (2.10.5)
requests (2.9.1)
python (2.7.10)

"""

import json
import pdb
import re
from Queue import Full
from Queue import Queue

import requests

from CrawlTaskItem import CrawlTaskItem
from anti_duplicate import *
from configs.logger_config import *
from configs.redis_config import *
from general import *

# 日志
logger = get_logger(__name__)
# 简单的agent 还是要用的,防止被封
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/51.0.2704.106 Safari/537.36'
}

# 推送到redis 队列, 支持100万
tmp_queue = Queue(100 * 10000)

# 豆瓣正则扣取配置
# key_id_pattern = re.compile('[\/|"|\s](?P<key_id>\d{8})[\/|"|\s]')
key_id_pattern = re.compile('subject\/(?P<key_id>\d{8})\/')

# 最大爬取深度
MAX_DEPTH = 10

DEBUG = False


# 抓取进程， 主要负责访问种子链接，生成种子内容
@thread_pool(1)
def crawl():
    while True:
        crawl_task_item_str = fetch_task_item()
        if crawl_task_item_str is None:
            logger.info('[抓取页面] 暂时没有抓取任务')
            time.sleep(0.1)  # sleep 100毫秒
            continue
        else:
            crawl_task_item = json.loads(
                crawl_task_item_str, object_hook=CrawlTaskItem.json_decoder)
            if DEBUG:
                pdb.set_trace()
            url = crawl_task_item.get_url()
            source_url = crawl_task_item.source_url
            current_crawl_depth = crawl_task_item.crawl_depth  # 当前的深度
            # 达到最大深度, 不予抓取
            if current_crawl_depth >= MAX_DEPTH:
                logger.info('[抓取页面] 任务:%s 达到最大深度; 放弃抓取', url)
                continue
            try:
                while rate_limit(40, 1, 'M'):
                    # 请求过于频繁,超过每分钟40次了
                    logger.info('[抓取页面]请求过于频繁,超过每分钟40次了')
                    time.sleep(0.1)
                logger.info('[抓取页面] 开始抓取网址:%s; 源地址:%s', url, source_url)
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
                        effective_task_ids = duplicate_key_id_with_set(
                            key_id_list=key_id_list)
                        logger.info('[抓取页面] 在页面:[%s] 抓取到[有效]任务种子数:%s 个; 任务ID为:%s', url,
                                    effective_task_ids.__len__(),
                                    ','.join(effective_task_ids))
                        for effective_task_id in effective_task_ids:
                            # 生成任务实体,同时记录爬取深度及源地址
                            tmp_crawl_task_item = CrawlTaskItem(None, None, None, effective_task_id,
                                                                crawl_task_item.crawl_depth + 1,
                                                                url)
                            retry_times = 0
                            while retry_times <= 5:
                                try:
                                    tmp_queue.put(tmp_crawl_task_item, block=True)
                                    break
                                except Full:
                                    retry_times += 1
                                    logger.error(
                                        '[抓取页面] 推任务:%s到临时队列失败, 队列已满; 第%s次尝试;', tmp_crawl_task_item, retry_times)
                                    time.sleep(0.1)  # sleep 100毫秒, 重试五次
                else:
                    logger.warn('[抓取页面] 抓取网址:%s  响应码:[%s]; 源地址:%s ', url, status_code, source_url)
            except requests.exceptions.RequestException as e:
                logger.exception('[抓取页面] 抓取网址:%s  失败;', url, e)


# 获取抓取任务(先只考虑每次只pop一个任务)
def fetch_task_item():
    try:
        start = time.time()
        crawl_task_item = redis_client.rpop('crawler:douban:tasks')
        # if DEBUG:
        #     pdb.set_trace()
        end = time.time()
        logger.info('[获取任务] 获取抓取任务耗时%s毫秒',
                    format_time(end - start))  # 埋点用作以后的优化
        return crawl_task_item
    except (redis.ConnectionError, redis.TimeoutError) as e:
        logger.exception('[获取任务] 失败;', e)
    return None


#  从内存queue中取任务推到redis中
@thread_pool(1)
def push_to_queue():
    while True:
        # 木有推送任务, sleep 1000ms
        if tmp_queue.empty():
            logger.info('[推送任务] 暂时没有推送任务')
            time.sleep(1)
        else:
            tmp_crawl_task_item = None
            try:
                tmp_crawl_task_item = tmp_queue.get()
                redis_client.lpush('crawler:douban:tasks', tmp_crawl_task_item.to_json())
                logger.info('[推送任务] 推到redis成功; 任务信息:%s', tmp_crawl_task_item)
            except (Exception) as e:
                logger.exception(
                    '[推送任务] 写入redis失败; 任务信息:%s', tmp_crawl_task_item, e)


# 初始化一个任务种子
def init_crawl_task():
    crawl_task_item = CrawlTaskItem(None, None, None, '25850122', 0, '')
    visited_key_ids.add('25850122')  # 第一个页面访问过了,也不用访问
    redis_client.lpush('crawler:douban:tasks', crawl_task_item.to_json())


if __name__ == '__main__':
    logger.info('[豆瓣]爬取start')
    init_crawl_task()
    crawl()
    push_to_queue()
