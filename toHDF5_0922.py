# coding=utf-8
import os
import sys
import time
from fastavro import reader
import pandas as pd
import numpy as np
from tqdm import tqdm
import re

# 生成ID
from generate_id import IdWorker
import datetime
CURRENT_YEAR = datetime.datetime.now().year
os.environ['NUMEXPR_MAX_THREADS'] = '64'

# 设置logging
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger()
f_handler = logging.FileHandler('error'+datetime.datetime.now().strftime('%Y_%m_%d')+'.log', mode='w')
f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(f_handler)


class Avro():
    r"""
    按行读取 avro
    智图的 avro 里面 key/value 均为 str
    数值类字段值在 avro 里面也已 str 存放
    """
    def __init__(self, file):
        self.file = file
        """
        self.columns_list = {'0':[],\
                             '1':['identifier_pisbn'],\
                             '2':['identifier_pisbn'],\
                             '3':['identifier_doi'],\
                             '4':['creator', 'creator_institution'],\
                             '5':['identifier_standard'],\
                             '6':['identifier_doi'],\
                             '7':['identifier_standard'],\
                             '8':['provider_subject'],\
                             '9':['creator_institution'],\
                             '10':[],\
                             '11':['identifier_doi'],\
                             '12':['source'],\
                             '13':[],\
                             '14':['date_created']}
        string_size_dic = {'0':{'title':255,'language':2},\
                             '1':{'title':255,'language':2,'identifier_pisbn':20},\
                             '2':{'title':255,'language':2,'identifier_pisbn':20},\
                             '3':{'title':255,'language':2,'identifier_doi':70},\
                             '4':{'title':255,'language':2,'creator':100,'creator_institution':100},\
                             '5':{'title':255,'language':2,'identifier_standard':100},\
                             '6':{'title':255,'language':2,'identifier_doi':70},\
                             '7':{'title':255,'language':2,'identifier_standard':100},\
                             '8':{'title':255,'language':2,'provider_subject':100},\
                             '9':{'title':255,'language':2,'creator_institution':100},\
                             '10':{'title':255,'language':2},\
                             '11':{'title':255,'language':2,'identifier_doi':70},\
                             '12':{'title':255,'language':2,'source':100},\
                             '13':{'title':255,'language':2},\
                             '14':{'title':255,'language':2}}
        """
        # 类型为标准的identifier_standard需要后处理（把两个标准号的去成1个）
        self.columns_list = {'0':[],\
                             '1':['identifier_pisbn'],\
                             '2':['identifier_pisbn'],\
                             '3':['identifier_doi'],\
                             '4':['creator', 'creator_institution'],\
                             '5':['identifier_standard'],\
                             '6':['identifier_doi'],\
                             '7':['identifier_standard'],\
                             '8':['provider_subject'],\
                             '9':['creator_institution'],\
                             '10':[],\
                             '11':['identifier_doi'],\
                             '12':['source'],\
                             '13':[],\
                             '14':['date_created']}
        self.string_size_dic = {'0':{'title':400,'language':2},\
                             '1':{'title':400,'language':2,'identifier_pisbn':50},\
                             '2':{'title':400,'language':2,'identifier_pisbn':50},\
                             '3':{'title':400,'language':2,'identifier_doi':50},\
                             '4':{'title':400,'language':2,'creator':50,'creator_institution':80},\
                             '5':{'title':400,'language':2,'identifier_standard':50},\
                             '6':{'title':400,'language':2,'identifier_doi':50},\
                             '7':{'title':400,'language':2,'identifier_standard':50},\
                             '8':{'title':400,'language':2,'provider_subject':50},\
                             '9':{'title':400,'language':2,'creator_institution':80},\
                             '10':{'title':400,'language':2},\
                             '11':{'title':400,'language':2,'identifier_doi':50},\
                             '12':{'title':400,'language':2,'source':150},\
                             '13':{'title':400,'language':2},\
                             '14':{'title':400,'language':2}}
        self.source_file_dtype = pd.api.types.CategoricalDtype(categories=list(range(1000)))
        # ''空字符串不是np.nan
        self.language_dtype = pd.api.types.CategoricalDtype(categories=['ZH','EN','JA','DE','SP','ID',\
                                                                        'FR','MS','HU','ES','PT','IT',\
                                                                        'PL','SR','RO','HR','SW','HI',\
                                                                        'RU','NL','AR','EL','FA','LT',\
                                                                        'UN','SL','TR','NO','CS','KO',\
                                                                        'SH','SK','DA','BS','SV','UK',\
                                                                        'IS','ET','CA','BG','AF','FI',\
                                                                        'HE','MO','EO','EU','LA','TH',\
                                                                        'LV','MK','CH',''])
        
  
    
    def ReadAvro(self):
        with open(self.file, 'rb') as fo, open('text.txt','w') as f_w:
            for record in tqdm(reader(fo), desc="ReadAvro"):
                # f_w.writelines(json.dumps(record, ensure_ascii=False))
                # f_w.write('\n')
                if record['value']['title'] == '[untitled]':
                    print(record)
                    time.sleep(1)


    def ExtractDic(self, record_dic, type_):
        new_dic = {}
        # id
        new_dic['id'] = record_dic['id']
        # title
        if 'title' not in record_dic:
            # title = '[untitled]'代表元数据中title缺失
            new_dic['title'] = '[untitled]'
        else:
            new_dic['title'] = record_dic['title'][:200]
        """
        # 由于枚举类型必须提请规定好取值范围，所以title_alternative用枚举类型存储不太合适
        # 用object存储则太费空间，所以本次暂时跳过
        # title_alternative
        if type_ in ['3','6']:
            if 'title_alternative' not in record_dic:
                # title_alternative = ''代表元数据中title缺失
                new_dic['title_alternative'] = ''
            else:
                new_dic['title_alternative'] = record_dic['title_alternative'][:200]
        """
        # 11报纸没有language，为了数据统一，给它加上language
        if 'language' not in record_dic:
            # 默认zh
            new_dic['language'] = 'ZH'
        else:
            new_dic['language'] = record_dic['language'][:2]
        # 其他字段
        for column in self.columns_list[type_]:
            if column not in record_dic:
                # 其他字段空的时候用空字符代替
                new_dic[column] = ''
            else:
                new_dic[column] = record_dic[column][:50]
        # 记录来源文件
        # new_dic['source_file'] = self.file[-18:]
        new_dic['source_file'] = int(re.search(r'\d+',self.file).group())
        return new_dic
            
    
    def AvroToHdf5(self):
        try:
            logger.info("Poccessing File: {}".format(self.file))    
            # 建立一个空的“总表”avro_dic为{'type':[特定type、date的元数据]}
            # 先按照type分类
            avro_dic = {}
            # type_id=0~14，0代表元数据中类型缺失
            for type_id in range(15):
                avro_dic[str(type_id)] = []
                
            # 读数据
            count = 0
            with open(self.file, 'rb') as fo:
                for line_id, record in enumerate(reader(fo)):
                    count += 1
                    # 只要2021的数据
                    if 'date' not in record['value']:
                        continue
                    elif record['value']['date'] != '2021':
                        continue
                    # 获取type  
                    if 'type' not in record['value']:
                        # type = '0'代表元数据中date缺失
                        type_ = '0'
                    else:
                        type_ = record['value']['type']
                    # 提取需要的字段存入tmp_dic   
                    tmp_dic = self.ExtractDic(record['value'], type_) 
                    # 存入“总表”
                    avro_dic[type_].append(tmp_dic)       

            # 存入HDF5文件，key为date年份，value为对应的字典列表
            for type_id, record in avro_dic.items():
                file_name = './data/' + type_id + '_' + '2021' + '.h5'
                if record != []:
                    tmp_data = pd.DataFrame(record)
                    # 转化为枚举类别category
                    # 枚举类别在to_hdf(append=True)时要保证取值范围完全相同
                    tmp_data[['source_file']] = tmp_data[['source_file']].astype(self.source_file_dtype)
                    # 报纸没有language，但是前面已经加上了
                    tmp_data[['language']] = tmp_data[['language']].astype(self.language_dtype)
                    """
                    # 包含titile_alternative的类型
                    if type_id in ['3','6']:
                        tmp_data[['titile_alternative']] = tmp_data[['titile_alternative']].astype('category')
                    # 包含creator_institution的类型
                    elif type_id in ['4','9']:
                        tmp_data[['creator_institution']] = tmp_data[['creator_institution']].astype('category')
                    # 法律法规
                    elif type_id == '8':
                        tmp_data[['provider_subject']] = tmp_data[['provider_subject']].astype('category')
                    """
                    # 资讯
                    if type_id == '14':
                        try:
                            tmp_data[['date_created']] = tmp_data[['date_created']].astype('datetime64')
                        except:
                            logger.error("{} IN POCCESSING {}! FOUND TYPE14 DATE_CREATED ERROR! SKIP!".format(e, self.file))
                            continue
                    # 使用'table'模式时，DataFrame不能为空否则报错，而使用‘fixed’时可以为空
                    # 原数据title最长有2千多，一般超过255的title是无意义的，设置为255
                    # 注意min_itemsize是字节，前面截断是按字符长度
                    # 如同样是len为10的字符串，存储时全部英文字母has a limit of [10]
                    # 全部中文has a limit of [30]
                    # Map column names to minimum string sizes for columns.
                    pd.DataFrame(tmp_data).to_hdf(file_name, 'obj', format='table', \
                                                  append=True, min_itemsize=self.string_size_dic[type_id])

            logger.info("Length of File {} : {}.".format(self.file, count))
        except BaseException as e:
            logger.error("{} IN POCCESSING {}! SKIP!".format(e, self.file))
            
        
    def GetYear(self, date:str):
        if date == '':
            return "unknown"
        year = int(date)
        if year < 1500 or year > CURRENT_YEAR:
            return "unknown"
        elif year >= 1500 and year < 1980:
            return 'old'
        elif year >= 1980 and year <= CURRENT_YEAR:
            return date
        else:
            return "unknown"       
        
        
if __name__ == '__main__':
    if os.path.exists('./data'):
        os.system('rm -rf data')
    os.makedirs('./data')
    
    # 生成ID
    worker = IdWorker(1,2)
    for file in os.listdir('./ori'):
        if file.find('.avro') != -1:
            weipu = Avro(os.path.join(os.getcwd(), 'ori/'+file))
            weipu.AvroToHdf5()



