import os
os.environ['NUMEXPR_MAX_THREADS'] = '64'
import re
import sys
import json

import pymysql
from sqlalchemy import create_engine

from H5File import H5File
from deduplicate import taskDispatch


def main():
    # 没有13
    for type_id in [10,5,12,4,1,2,3,6,7,8,9,11,14]:
        try:
            h5file = H5File(type_id, 2021)
            h5file.generate_DataFrame()
            # h5file.add_title_alter()
            # h5file.add_id_mark()
            h5file.add_title_code()

            taskDispatch(h5file)
        except:
            pass

if __name__ == '__main__':
    main()   
