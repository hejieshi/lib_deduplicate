# 生成ID
from generate_id import IdWorker
# worker = IdWorker(1,2)
parent = {}
rank = {}
import redis
# r = redis.Redis('localhost', port=6379, db=1)

# # 设置logging
# import datetime
# import logging
# logging.basicConfig(level = logging.INFO,format = '%(asctime)s %(levelname)s: %(message)s')
# logger = logging.getLogger()
# f_handler = logging.FileHandler('dedupWithColumn_log'+datetime.datetime.now().strftime('%Y_%m_%d')+'.log', mode='a')
# f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
# logger.addHandler(f_handler)


def find(x):
    #父节点为本身，直接返回。
    if parent[x]==x:
        return x
    # 类似记忆化储存.
    parent[x]=find(parent[x])
    return parent[x]


def merge(i, j):
    # 先找到两个根节点
    x = find(i)
    y = find(j) 
    if rank[x] <= rank[y]:
        parent[x] = y
    else:
        parent[y] = x
    if rank[x] == rank[y] and x != y:
        # 如果深度相同且根节点不同，则新的根节点的深度+1
        rank[y] += 1;                   

        
def mergeColumnAndTitle(h5file, column_dic, title_list):
    """
    路径优化版本的并查集，接收三个参数：
    id_set:全部id的set，由pdata.id获得
    （注：这里为了加速计算，由pdata.id获得，实际应该检查以下id_set ?= column_dic.values+title_list.values）
    column_dic:全部有column的字典，结构为{'doi':[属于这个doi的所有id...],'doi':[属于这个doi的所有id...]...}
    title_list:title相似的列表，结构为{[title相似的所有id...],[title相似的所有id...]...}
    返回：{'new_id':[所有重复的id...]}
    """
    # 初始化
    global parent
    global rank
    parent = {}
    rank = {}
    id_set = set(h5file.pdata.id.unique())
    for id_ in id_set:
        parent[id_] = id_
        rank[id_] = 1
        
    # 合并，（1，2，3）拆分为（1，2）（1，3）来合并
    try:
        for value in column_dic.values():
            if len(value) > 1:
                for dup_id in value[1:]:
                    merge(value[0],dup_id)

        for value in title_list:
            if len(value) > 1:
                for dup_id in value[1:]:
                    merge(value[0],dup_id)
    except:
        print("错误：column_dic, title_list中出现了id_set中不存在的id！将返回空字典！")
        return {}
    # 找出重复项(按根节点分类){'根结点id':[重复的id...]}
    dup_dic = {}
    for id_ in id_set:
        root_id = find(id_)
        if root_id not in dup_dic:
            dup_dic[root_id] = [id_]
        else:
            dup_dic[root_id].append(id_)
    
    return dup_dic
#     # 创建新id，插入redis
#     print_count = 0
#     repeat_count = 0
#     for old_dup_id_list in dup_dic.values():
#         # 存redis
#         new_id = worker.get_id()
#         for old_id in old_dup_id_list:
#             r.rpush(new_id, old_id)   # {new_id: [dup_old_id, , ]}
#             r.set(old_id, new_id)   # {old_id : new_id}
          
#         # 统计
#         if len(old_dup_id_list) > 1:
#             repeat_count += (len(old_dup_id_list)-1)
        
#     r.set(h5file.file_name + '|repeat_count', repeat_count)
#     r.set(h5file.file_name + '|ori_count', len(h5file.pdata))
#     r.connection_pool.disconnect()
#     print('{}/{} data have been deleted'.format(repeat_count,len(h5file.pdata)))
#     # logger.info('{}: {}/{} data of have been deleted'.format(h5file.file_name,repeat_count,len(h5file.pdata)))
#     # logger.shutdown()
#     # return final_dup_dic