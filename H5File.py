import os
import pandas as pd
import re
from tool import punc_table

class H5File:
    """
    将HDF5文件读入DataFrame，并处理
    注意H5文件太大时应使用start/stop参数分批读入
    """
    def __init__(self, type_id, year):
        self.type_id = type_id
        self.year = year
        self.file_name = str(self.type_id) + "_" + str(self.year) + ".h5"
        self.path = os.getcwd() + '/data/' + self.file_name
        self.pdata = pd.DataFrame()
    
    
    def del_symbol(self, s):
        # return s.upper().translate(punc_table)
        # 加大“（第一期）”及“第一期”的权重
        pattern = re.compile(r'[\(（\<《\[【\{「〔]第*[\u96f6\u3007\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\d]{1,5}[\u4e00-\u9fa5]*[\)）\>》\]】\}」〕]')
        result = re.findall(pattern,s)
        if result == []:
            pattern = re.compile(r'第[\u96f6\u3007\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\d]{1,5}[次|批|期|号|卷|届|部]')
            result = re.findall(pattern,s)
        for token in result:
            s = s.replace(token,token*10)
        # 加大 年月日 的权重
        tmp_s = s.translate(punc_table).upper()
        if tmp_s == '':
            tmp_s = '[untitled]' 
        
        return tmp_s
    
    
    def generate_DataFrame(self):
        # self.pdata = pd.read_hdf(self.path,mode='r',start=0,stop=100000)
        self.pdata = pd.read_hdf(self.path,mode='r')
        # self.pdata = self.pdata[self.pdata.title != '']
        
    def add_title_alter(self):
        """
        pre poccess title alternative
        将'title_alternative'复制新的一行，并用id+‘a’标示，append在最后
        """
        tem_pdata = self.pdata.loc[self.pdata['title_alternative']!=''].copy()
        tem_pdata['title'] = tem_pdata['title_alternative']
        tem_pdata['id'] = tem_pdata['id'] + 'a'
        # 此处需要增加语言、source_file等信息
        assert 0
        self.pdata = self.pdata.append(tem_pdata, ignore_index=True)
        
    
    def add_trans_title(self):
        # # english[中文标题] TODO
        # df = pdata
        # filtered = df[pdata['title'].str.contains('.\[')]
        # tem_filtered = filtered.loc[contain_chinese(filtered['title'].str)]
        # filtered = filtered.append(tem_filtered, ignore_index = True)
        # filtered.tail()
        pass
    
    
    def add_id_mark(self):
        """
        上下用'id_mark'标示： 'p' 's'
        """
        self.pdata['id_mark'] = self.pdata['id']
        self.pdata.loc[self.pdata['title'].str.endswith("（上）"), ['id_mark']] += 'p'
        self.pdata.loc[self.pdata['title'].str.endswith("(上)"), ['id_mark']] += 'p'
        self.pdata.loc[self.pdata['title'].str.endswith("（下）"), ['id_mark']] += 's'
        self.pdata.loc[self.pdata['title'].str.endswith("(下)"), ['id_mark']] += 's'
        
    
    def add_title_code(self):
        """
        新增'title_code'列（预处理后的str）
        """
        self.pdata['title_code'] = self.pdata['title']
        self.pdata.title_code = self.pdata.title.map(lambda x: self.del_symbol(str(x)))
        # 由于h5文件分多次写入，index不唯一，所以需要充值索引
        # 后续可以利用新id重建索引
        self.pdata = self.pdata.reset_index(drop=True)
        
        
    def classify_with_column(self,column):
        """
        使用column分类，返回同一分类的布尔索引，格式为{'column_value':布尔索引}
        """
        classified_dic = {}
        for value in self.pdata[column].unique():
            classified_dic[value] = (self.pdata[column] == value)
        return classified_dic
        
    
        
    