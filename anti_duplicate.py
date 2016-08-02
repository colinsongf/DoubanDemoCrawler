# coding:utf-8

import threading
from configs.logger_config import *

# 日志
logger = get_logger(__name__)
# 已经抓取过的种子列表
visited_key_ids = set()
# 添加到set的锁,防止竞争
add_lock = threading.Lock()


# key_id去重(用传统的set方法来去重)
def duplicate_key_id_with_set(key_id_list):
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
