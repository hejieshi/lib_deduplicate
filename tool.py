import pandas as pd
from zhon.hanzi import punctuation as ch_punctuation
from string import punctuation


def punctPreprocess():
    # 预处理，去掉符号，替换希腊字母
    common_replace = pd.read_excel('./source/字母对照表.xlsx', header=None, engine='openpyxl')
    all_punctuation = punctuation + ch_punctuation
    for char in [' ','\t','\u3000','\u00A0','\u2002']:
        all_punctuation += char
    dicts={i:'' for i in all_punctuation}
    for index,row in common_replace.iterrows():
        dicts[row[0]] = row[1]
    return str.maketrans(dicts)


punc_table = punctPreprocess()


def isLetter(c):
    # 因为已经upper变为了大写
    if ord(c) > 64 and ord(c) < 91:
        return True
    else:
        return False


def isEnglish(c):
    # 数字+大写字母
    if ord(c) > 47 and ord(c) < 91:
        return True
    else:
        return False
    
def hashString(string:str):
    hashCode = 0
    # 如果是英文title，则自动忽略所有的非英文单词
    # 中文标题的hashcode全置0（实际上不能置0，应该把precision设为小数）
    """
    
    中文标题后续为了将（上）（下）区别，需要额外处理，此代码暂时未写
    
    """
    if isLetter(string[0]):
        # 计算均值
        for char in string:
            if isEnglish(char):
                hashCode += ord(char)
            else:
                hashCode += 77
   
    # 保留精度为0.5（5越大，筛选越严格）
    
    """
    precision最好根据句长动态调整，如：
    s1 = 'Temperature sensor based Temperature sensor based on light scattering spectrum of microcavity-coupled nanoparticles'
    s2 = 'Temperature sensor based Temperature sensor based on light scattering spectrum coupled nanoparticles'
    句长够长时，可以hash到同一个桶，太短则不行
    """
    
    precision = 5
    hashCode = (hashCode*precision)//len(string)
    # 为防止区间过于密集，将hashCode按照string长度分桶
    # 根据数据重复规则，一般重复title长度差在10以内，diff = 2*10
    diff = 20
    # 最终hashcode的范围：47～91*length
    # +1 防止长度小于20的结果为0
    # 0915修改，为了加快测试临时修改了precision和10*
    # return hashCode*((len(string)//diff+1)*diff)
    return hashCode*((10*len(string)//diff+1)*diff)

def hashStringOdd(string:str):
    hashCode = 0
    for index, char in enumerate(string):
        if(index%2):
            try:
                hashCode += ord(char)
            except:
                print(string)
                print(char)
                assert 0
   
    # 再哈希时，精度设置低一些
    # 0915修改，为了加快测试临时修改了precision和10*
    # precision = 1
    precision = 5
    hashCode = (hashCode*precision)//len(string)
    diff = 20
    # return hashCode*((10*len(string)//diff+1)*diff)
    return hashCode*((10*len(string)//diff+1)*diff)

def hashStringEven(string:str):
    hashCode = 0
    for index, char in enumerate(string):
        if(1-index%2):
            hashCode += ord(char)
   
    precision = 1
    hashCode = (hashCode*precision)//len(string)
    diff = 20
    return hashCode*((len(string)//diff+1)*diff)

def hashString3(string:str):
    hashCode = 0
    for index, char in enumerate(string):
        if(index%3):
            hashCode += ord(char)
   
    # 0915修改，为了加快测试临时修改了precision和10*
    # precision = 1
    precision = 5
    hashCode = (hashCode*precision)//len(string)
    diff = 20
    # return hashCode*((len(string)//diff+1)*diff)
    return hashCode*((10*len(string)//diff+1)*diff)

def deal_stantard(s):
    # pdata1.identifier_standard = pdata1.identifier_standard.map(lambda x: fun(str(x)))
    is_list = s.strip().split(';')
    if len(is_list) == 1:
        if len(is_list[0]) > 12:
            return s[:2]+s[6:]
        else:
            return s
    else:
        a,b = is_list
        if len(a) > len(b):
            return b
        else:
            return a

def containChinese(check_str):
    for ch in check_str:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False