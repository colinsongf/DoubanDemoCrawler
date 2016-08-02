# coding:utf-8

import json
import time


class CrawlTaskItem(object):
    """爬虫任务描述"""

    DOUBAN_ID = 1  # 由于暂时只考虑豆瓣抓取, 所以固定了部分抓取参数值
    DOUBAN_HOST = 'www.douban.com'
    DOUBAN_SEED_FORMAT = 'https://movie.douban.com/subject/%s'  # 豆瓣电影链接源格式

    def __init__(self, site_id, site_host, site_seed_format, key_id, crawl_depth, source_url):
        super(CrawlTaskItem, self).__init__()
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
        # if '__type__' in obj and obj['__type__'] == 'CrawlTaskItem':
        task = CrawlTaskItem(None, None, None, str(obj['key_id']),
                             int(obj['crawl_depth']), str(obj['source_url']))
        task.crawl_time = obj['_crawl_time']
        return task

    # 怎么样可以判断为同一个任务; 对应网站Id一样,且 种子Id一样就可以认为是同一个任务
    def __eq__(self, other):
        if self.site_id == other.site_Id and self.key_id == other.key_id:
            return True
        return False
