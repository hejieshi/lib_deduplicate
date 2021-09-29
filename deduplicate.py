import re
import json

from tqdm import tqdm
import numpy as np
import pandas as pd
import Levenshtein
from fuzzywuzzy import fuzz
import redis
r = redis.Redis('localhost', port=6379, db=3)

# 中文标题的hashcode不能全置0，要改
from tool import hashString,hashStringOdd,hashStringEven,hashString3,containChinese
from H5File import H5File
from generate_id import IdWorker
worker = IdWorker(1,2)
from merge import mergeColumnAndTitle

# 设置logging
import datetime
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger()
f_handler = logging.FileHandler('deduplicate_log'+datetime.datetime.now().strftime('%Y_%m_%d')+'.log', mode='w')
f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(f_handler)

max_bucket_size = 0
repeat_count_judge = 0

columns_list = {}
with open('column_config.json','r') as f:
    columns_list = json.loads(f.read())
        
# 1. 先hash再辅助判断 部分的代码
def hashTitleTri(h5file):
    global repeat_count_judge
    repeat_count_judge = 0
    hash_code_map = {}
    for index,row in h5file.pdata.iterrows():
        # 获得hashcode
        hash_code = hashString(row['title_code'])
        # 将（hashcode， index）加入字典
        if hash_code in hash_code_map:
            hash_code_map[hash_code].append(index)
        else:
            hash_code_map[hash_code] = [index]
            
    # f = open('test.txt', 'w', encoding='utf-8')
    # f = open('/Users/jas/Desktop/test.txt', 'w')

    hash_code_map_final_list = []

    for hash_code,index_list in tqdm(hash_code_map.items(),desc='hash_code_map'): 
        # if(len(index_list)>1):
        # print('Hash_code:\t{}'.format(hash_code))
        # 对奇数位再哈希，两次相同则判断为同一字符 
        reHashDicOdd = {}
        for index in index_list:
            # 1111_2222
            hash_code_odd = str(hash_code) + '_' + str(hashStringOdd(h5file.pdata['title_code'][index]))
            # hash_code_odd = hashStringOdd(pdata['title_code'][index])
            if hash_code_odd in reHashDicOdd:
                reHashDicOdd[hash_code_odd].append(index)
            else:
                reHashDicOdd[hash_code_odd] = [index]

            # postPoccess(f, reHashDicOdd, pdata)

            # """
            # 再采用间隔哈希(每三个)
            # 再hash后max_bucket_size由600变为400，速度变为一半，召回个数减少约200个，约1%
        for hash_code,index_list in reHashDicOdd.items(): 
            reHashDic3 = {}
            for index in index_list:
                # hash_code_3 = hashString3(pdata['title_code'][index])
                # 1111_2222_3333
                hash_code_3 = hash_code + '_' + str(hashString3(h5file.pdata['title_code'][index]))
                if hash_code_3 in reHashDic3:
                    reHashDic3[hash_code_3].append(index)
                else:
                    reHashDic3[hash_code_3] = [index]
                # 精筛，注意这个代码要放在粗筛的最后一层，否则前面的筛选失效
            postPoccess(h5file, reHashDic3)
    # f.close() 
    r.set(h5file.file_name + '|repeat_count', repeat_count_judge)
    r.set(h5file.file_name + '|ori_count', len(h5file.pdata))
    # print('{}/{} data have been deleted'.format(repeat_count_judge,len(h5file.pdata)))
    logger.info('{}: {}/{} data of have been deleted'.format(h5file.file_name,repeat_count_judge,len(h5file.pdata)))
    
    
def postPoccess(h5file, hashDic):
    """
    hash + 辅助判
    断
    对最终的hash表处理，找出相同的，写入文件
    """ 
    # print("postPoccessing, length of hasdDic:{}".format(len(hashDic)))
    global max_bucket_size
    global repeat_count_judge
    
    for hash_code,index_list in hashDic.items(): 
        r.hset(h5file.file_name + '|title_hash', hash_code, json.dumps({hash_code: h5file.pdata.loc[index_list]['id'].to_list()}))
        length = len(index_list)
        # 不需要用length删选了，因为如果length==1,那么以下的语句执行1次就会结束
        # if length > 1:
        max_bucket_size = max(max_bucket_size, length)
        # 使用numpy加快处理速度(维度大于10000时提速显著)
        visited = np.zeros([length+1], dtype = np.int)
        hash_code_map_final = {}
        for i in range(length):
            """
            # visited[i]:
            # 注意：如果该title已经并入其他title，则跳过（每个title只被匹配一次）
            # 如果不跳过，可以增加召回率
            # pdata['id_mark'][index_list[i]].endswith('a')):
            # 使hash_code_map_final[i]的i标题不是通过alt复制出来的新行，三种情况：
            # 1. 原标题（i）与alt（j）相似，alt被append到hash_code_map_final[原标题index]中
            # 2. alt（i）与原标题（j）相似，被1中情况覆盖，跳过
            # 3. alt（i）与alt（j）相似，一般alt对应的原标题也会相似，也跳过
            """
            if visited[i] :
                continue
            # 初始化
            hash_code_map_final[i] = [index_list[i]]
            visited[i] = 1
            for j in range(i+1, length):
                if visited[j] != 1:
                    # 提高效率
                    temp_i = h5file.pdata['title_code'][index_list[i]]
                    temp_j = h5file.pdata['title_code'][index_list[j]]
                    if containChinese(temp_i) and len(temp_i) < 15:
                        if fuzz.ratio(temp_i, temp_j) > 95:
                            # 需要辅助判断
                            if h5file.type_id in [1, 4, 8, 9, 12, 14]:
                                column = columns_list[str(h5file.type_id)][0]
                                if h5file.pdata[column][index_list[i]] == h5file.pdata[column][index_list[j]]:
                                    hash_code_map_final[i].append(index_list[j])
                                    visited[j] = 1
                                    repeat_count_judge += 1
                            else: # 不需要辅助判断
                                hash_code_map_final[i].append(index_list[j])
                                visited[j] = 1
                                repeat_count_judge += 1
                    else:
                        if fuzz.ratio(temp_i, temp_j) > 85:
                            # 需要辅助判断
                            if h5file.type_id in [1, 4, 8, 9, 12, 14]:
                                column = columns_list[str(h5file.type_id)][0]
                                if h5file.pdata[column][index_list[i]] == h5file.pdata[column][index_list[j]]:
                                    hash_code_map_final[i].append(index_list[j])
                                    visited[j] = 1
                                    repeat_count_judge += 1
                            else: # 不需要辅助判断
                                hash_code_map_final[i].append(index_list[j])
                                visited[j] = 1
                                repeat_count_judge += 1
                    """
                    这样会出现这种情况：
                    有一条记录：title：中文1（index：2），alt：英文a（index：4），分别与另外两条记录相似
                    title：中文2（index：1） 相似于 中文1
                    title：英文b（index：3） 相似于 英文a
                    处理后他们被归入两个字典：
                    {1:[1, 2]}
                    {3:[3, 4]}
                    他们都属于相似的标题，需要在最后，使用并查集操作使他们归并到同一个集合
                    """
            
            # redis for j循环结束
            # 如果length==1那么不会进入j循环，直接存redis
            new_id_for_key = worker.get_id()
            # 重复列表中的index索引id
            old_dup_id_list = h5file.pdata.loc[hash_code_map_final[i]]['id'].to_list()
            # print(new_id_for_key,':',old_dup_id_list)

            for old_id in old_dup_id_list:
                r.rpush(new_id_for_key, old_id)   # {new_id: [dup_old_id, , ]}
                r.set(old_id, new_id_for_key)   # {old_id : new_id}            

#             # 打印
#             if len(hash_code_map_final[i]) > 1:
#                 print(['*********************************************Same ReHash*********************************************\n'])
#                 for index in hash_code_map_final[i]:
#                     print('Id:\t', h5file.pdata['id'][index])
#                     print(h5file.pdata['title'][index])                

# 2. doi先去重再hash 部分的代码
def dedupWithColumn(h5file):
    """
    修改self.pdata，删除column重复项，保留column为空项，供后面title_hash，此处未重置索引
    返回三个变量：
    column_dic：存储所有数据中column不为空的数据，格式为{'doi':[属于这个doi的所有id...]}
    empty_pdata_index：column为空项的布尔索引
    distinct_pdata_index：column为空的数据中，删除column重复项后剩下的数据的布尔索引
    """
    column = columns_list[str(h5file.type_id)][0]
    empty_pdata_index = (h5file.pdata[column] == '')
    # 去重，保留第一次出现的数据，反转是为了使不重复的index为True
    distinct_pdata_index = (h5file.pdata.duplicated(subset=column,keep='first') == False) 
    # 存储所有数据中column不为空的数据，格式为{'doi':[属于这个doi的所有id...]}
    # column_dic = {value:df.id.tolist() for value,df in pdata.groupby(pdata[column]) if value != ''}
    column_dic = {}
    for value,df in tqdm(h5file.pdata.groupby(h5file.pdata[column]),desc='deduplicating with {}...'.format(column)):
        # 因为不重复的在id_set初始化时就有，而且也不需要进行并查集union操作
        # 所以column_dic和title_list只需要记录重复的id
        if value != '':
            old_id_list = df.id.tolist()
            r.hset(h5file.file_name + '|' + column, value, json.dumps({value: old_id_list}))
            if len(old_id_list) > 1:
                column_dic[value] = old_id_list
                # break
    
    # 修改self.pdata，删除column重复项，保留column为空项，供后面title_hash，此处重置索引
    # 谨慎修改self.pdata，因为post_proccess中可以会需要索引
    # h5file.pdata = h5file.pdata[empty_pdata_index|distinct_pdata_index]   
    return column_dic, empty_pdata_index, distinct_pdata_index



def hashTitleHex(h5file, empty_pdata_index, distinct_pdata_index):
    """
    分别对empty_pdata_index，distinct_pdata_index计算hash
    由于distinct_pdata_index中已经不可能再重复
    所以distinct_pdata_index中的values仅做匹配用
    """
    # 先将distinct_pdata_index的所有hash_code计算出来
    hash_code_map = {} # 用于存放临时数据
    distinct_hash_code_map = {}
    for index,row in h5file.pdata[distinct_pdata_index].iterrows():
        # 获得hashcode
        hash_code = hashString(row['title_code'])
        # 将（hashcode， index）加入字典
        if hash_code in hash_code_map:
            hash_code_map[hash_code].append(index)
        else:
            hash_code_map[hash_code] = [index]
    # 对奇数位再哈希
    for hash_code,index_list in tqdm(hash_code_map.items(),desc='generate_distinct_hash_code_map...'): 
        reHashDicOdd = {}
        for index in index_list:
            # 1111_2222
            hash_code_odd = str(hash_code) + '_' + str(hashStringOdd(h5file.pdata['title_code'][index]))
            # hash_code_odd = hashStringOdd(pdata['title_code'][index])
            if hash_code_odd in reHashDicOdd:
                reHashDicOdd[hash_code_odd].append(index)
            else:
                reHashDicOdd[hash_code_odd] = [index]
            # postPoccess(f, reHashDicOdd, pdata)
            # 再采用间隔哈希(每三个)
            # 再hash后max_bucket_size由600变为400，速度变为一半，召回个数减少约200个，约1%
        for hash_code,index_list in reHashDicOdd.items(): 
            # reHashDic3 = {}
            for index in index_list:
                # hash_code_3 = hashString3(pdata['title_code'][index])
                # 1111_2222_3333
                hash_code_3 = hash_code + '_' + str(hashString3(h5file.pdata['title_code'][index]))
                if hash_code_3 in distinct_hash_code_map:
                    distinct_hash_code_map[hash_code_3].append(index)
                else:
                    distinct_hash_code_map[hash_code_3] = [index]  
    
    # 对empty_pdata_index部分去重
    title_list = []
    hash_code_map = {} # 重置hash_code_map
    for index,row in h5file.pdata[empty_pdata_index].iterrows():
        # 获得hashcode
        hash_code = hashString(row['title_code'])
        # 将（hashcode， index）加入字典
        if hash_code in hash_code_map:
            hash_code_map[hash_code].append(index)
        else:
            hash_code_map[hash_code] = [index]
            
    # f = open('test.txt', 'w', encoding='utf-8')
    # f = open('/Users/jas/Desktop/test.txt', 'w')
    # hash_code_map_final_list = []

    for hash_code,index_list in tqdm(hash_code_map.items(),desc='deduplicating pdata without {}...'.format(columns_list[str(h5file.type_id)][0])): 
        # 对奇数位再哈希，两次相同则判断为同一字符 
        reHashDicOdd = {}
        for index in index_list:
            # 1111_2222
            hash_code_odd = str(hash_code) + '_' + str(hashStringOdd(h5file.pdata['title_code'][index]))
            # hash_code_odd = hashStringOdd(pdata['title_code'][index])
            if hash_code_odd in reHashDicOdd:
                reHashDicOdd[hash_code_odd].append(index)
            else:
                reHashDicOdd[hash_code_odd] = [index]
            # postPoccess(f, reHashDicOdd, pdata)
            # 再采用间隔哈希(每三个)
            # 再hash后max_bucket_size由600变为400，速度变为一半，召回个数减少约200个，约1%
        for hash_code,index_list in reHashDicOdd.items(): 
            # reHashDic3 = {}
            # empty_hash_code_map的初始化没有和distinct_hash_code_map一样放在上层是因为每次进入循环都要置空
            empty_hash_code_map = {}
            for index in index_list:
                # hash_code_3 = hashString3(pdata['title_code'][index])
                # 1111_2222_3333
                hash_code_3 = hash_code + '_' + str(hashString3(h5file.pdata['title_code'][index]))
                if hash_code_3 in empty_hash_code_map:
                    empty_hash_code_map[hash_code_3].append(index)
                else:
                    empty_hash_code_map[hash_code_3] = [index]
                # 精筛，注意这个代码要放在粗筛的最后一层，否则前面的筛选失效
            postPoccessWithColumn(h5file, distinct_hash_code_map, empty_hash_code_map, title_list)
    # f.close() 
    return title_list
    
    
def postPoccessWithColumn(h5file, distinct_hash_code_map, empty_hash_code_map, title_list):
    """
    1. 找出empty_hash_code_map中的重复项目
    2. 找出empty_hash_code_map中与distinct_hash_code_map重复的项
    3. 把结果放到title_list中
    """ 
    # print("postPoccessing, length of hasdDic:{}".format(len(hashDic)))
    global max_bucket_size
    
    for hash_code,index_list in empty_hash_code_map.items(): 
        # r.hset(h5file.file_name + 'title_hash', hash_code, json.dumps({hash_code: h5file.pdata.loc[index_list]['id'].to_list()}))
        length = len(index_list)
        # 如果length==1,也需要去distinct_hash_code_map中去匹配
        max_bucket_size = max(max_bucket_size, length)
        # 使用numpy加快处理速度(维度大于10000时提速显著)
        visited = np.zeros([length], dtype = np.int)
        # hash_code_map_final = {}
        # 若distinct_hash_code_map中没有这个hash_code，则不用再去distinct_hash_code_map中查找
        if hash_code in distinct_hash_code_map:
            need_refind = 1
            distinct_index_list = distinct_hash_code_map[hash_code]
            distinct_length =  len(distinct_index_list)
            distinct_visited = np.zeros([distinct_length], dtype = np.int)
        else:
            need_refind = 0
        for i in range(length):
            """
            # visited[i]:
            # 注意：如果该title已经并入其他title，则跳过（每个title只被匹配一次）
            # 如果不跳过，可以增加召回率
            # pdata['id_mark'][index_list[i]].endswith('a')):
            # 使hash_code_map_final[i]的i标题不是通过alt复制出来的新行，三种情况：
            # 1. 原标题（i）与alt（j）相似，alt被append到hash_code_map_final[原标题index]中
            # 2. alt（i）与原标题（j）相似，被1中情况覆盖，跳过
            # 3. alt（i）与alt（j）相似，一般alt对应的原标题也会相似，也跳过
            """
            if visited[i] :
                continue
            # 初始化，把自己加入重复列表
            # hash_code_map_final[i] = [index_list[i]]
            tmp_list = [index_list[i]]
            visited[i] = 1
            # 先在empty_hash_code_map中查找
            for j in range(i+1, length):
                if visited[j] != 1:
                    # 提高效率
                    temp_i = h5file.pdata['title_code'][index_list[i]]
                    temp_j = h5file.pdata['title_code'][index_list[j]]
                    if containChinese(temp_i) and len(temp_i) < 15:
                        if fuzz.ratio(temp_i, temp_j) > 95:
                            tmp_list.append(index_list[j])
                            # hash_code_map_final[i].append(index_list[j])
                            visited[j] = 1
                    else:
                        if fuzz.ratio(temp_i, temp_j) > 85:
                            tmp_list.append(index_list[j])             
                            # hash_code_map_final[i].append(index_list[j])
                            visited[j] = 1
                    """
                    这样会出现这种情况：
                    有一条记录：title：中文1（index：2），alt：英文a（index：4），分别与另外两条记录相似
                    title：中文2（index：1） 相似于 中文1
                    title：英文b（index：3） 相似于 英文a
                    处理后他们被归入两个字典：
                    {1:[1, 2]}
                    {3:[3, 4]}
                    他们都属于相似的标题，需要在最后，使用并查集操作使他们归并到同一个集合
                    """
            # 然后在distinct_hash_code_map中查找与index_list[i]的hashcode相同的数据中有没有重复的
            if need_refind:
                # distinct_length至少为1
                for k in range(distinct_length):
                    if distinct_visited[k] != 1:
                        # 提高效率
                        temp_i = h5file.pdata['title_code'][index_list[i]]
                        temp_k = h5file.pdata['title_code'][distinct_index_list[k]]
                        if containChinese(temp_i) and len(temp_k) < 15:
                            if fuzz.ratio(temp_i, temp_k) > 95:
                                tmp_list.append(distinct_index_list[k])
                                # hash_code_map_final[i].append(index_list[j])
                                distinct_visited[k] = 1
                                # 由于distinct_index_list中不可能有重复的，所以只要找到一次即可跳出循环
                                break
                        else:
                            if fuzz.ratio(temp_i, temp_k) > 85:
                                tmp_list.append(distinct_index_list[k])             
                                # hash_code_map_final[i].append(index_list[j])
                                distinct_visited[k] = 1
                                # 由于distinct_index_list中不可能有重复的，所以只要找到一次即可跳出循环
                                break
                        
            # for j循环结束
            # 如果length==1，不会进入j循环，但进入k循环
            # 因为不重复的在id_set初始化时就有，而且也不需要进行并查集union操作
            # 所以column_dic和title_list只需要记录重复的id
            # 将重复列表中的index转换为索引id
            old_dup_id_list = h5file.pdata.loc[tmp_list]['id'].to_list()
            title_list.append(old_dup_id_list)
            # print(new_id_for_key,':',old_dup_id_list)                 


def taskDispatch(h5file):
    # 1. 先hash再辅助判断
    if h5file.type_id in [1, 4, 8, 9, 12, 14]:
        hashTitleTri(h5file)
    # 2. doi先去重再hash
    elif h5file.type_id in [3, 5, 6, 7, 11]:
        column_dic, empty_pdata_index, distinct_pdata_index = dedupWithColumn(h5file)
        title_list = hashTitleHex(h5file, empty_pdata_index, distinct_pdata_index)
        dup_dic = mergeColumnAndTitle(h5file, column_dic, title_list)
        # 创建新id，插入redis
        repeat_count = 0
        for old_dup_id_list in dup_dic.values():
            # 存redis
            new_id = worker.get_id()
            for old_id in old_dup_id_list:
                r.rpush(new_id, old_id)   # {new_id: [dup_old_id, , ]}
                r.set(old_id, new_id)   # {old_id : new_id}
            # 统计
            if len(old_dup_id_list) > 1:
                repeat_count += (len(old_dup_id_list)-1)
                
        r.set(h5file.file_name + '|repeat_count', repeat_count)
        r.set(h5file.file_name + '|ori_count', len(h5file.pdata))
        # r.connection_pool.disconnect()
        # print('{}/{} data have been deleted'.format(repeat_count,len(h5file.pdata)))
        logger.info('{}: {}/{} data of have been deleted'.format(h5file.file_name,repeat_count,len(h5file.pdata)))
        # logger.shutdown()
    # 直接hash:0,2,10,13
    else:
        hashTitleTri(h5file)