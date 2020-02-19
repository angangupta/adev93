import xml.dom.minidom as minidom
import re
import pandas as pd
from collections import OrderedDict
import sys
import json
import subprocess, json
import itertools
from functools import reduce
import numpy as np
from xlwt import Workbook

###input_file = "C:/Users/kanish/Desktop/UNDIAL FI/m_STG_TO_HOPM_S_TXN_CPPH_HIST_LOAD_MONTHLY_REWARD.xml"
#outfile = "C:/Users/kanish/Desktop/UNDIAL FI/m_STG_TO_HOPM_S_TXN_CPPH_HIST_LOAD_MONTHLY_REWARD_testrun_route_3.csv"

Flag='Y'
def router_transformation_extract(a):
    input_file =a

    DOMTree = minidom.parse(input_file)
    FL1='Y'
    FL2='Y'
    final_list = []
    IM_list1=[]
    IM_list2=[]
    transformation_dict = {}

    main_tag_list = DOMTree.getElementsByTagName("TRANSFORMATION")
    for Type in main_tag_list:
        transformation_TYPE = Type.getAttribute('TYPE')
        instance_TYPE = Type.getAttribute('NAME')

        #print(transformation_TYPE)
        if transformation_TYPE == "Router":
            transformation_list = Type.getElementsByTagName("GROUP")
            filter_list = Type.getElementsByTagName("TRANSFORMFIELD")


            for transformation in transformation_list:
                row = []
                row.append(instance_TYPE)
                ###field_transformation_name = transformation.getAttribute('NAME')
                ###row.append(str(field_transformation_name))
                row.append(transformation_TYPE)
                field_transformation_name = transformation.getAttribute('NAME')
                row.append(str(field_transformation_name))
                field_transformation_name = transformation.getAttribute('EXPRESSION')
                row.append(str(field_transformation_name))
                IM_list1.append(row)
            for Filter in filter_list:
                row1=[]
                field_transformation_name = Filter.getAttribute('NAME')
                row1.append(str(field_transformation_name))
                field_transformation_name = Filter.getAttribute('GROUP')
                row1.append(str(field_transformation_name))
                IM_list2.append(row1)
    DF1 = pd.DataFrame(IM_list1)
    if DF1.empty:
        FL1='N'
    else:
        DF1.columns = ['Instance Name', 'Transformation type', 'GROUP', 'EXPRESSION']
    DF2 = pd.DataFrame(IM_list2)
    if DF2.empty:
        FL2='N'
    else:
        DF2.columns = ['NAME', 'GROUP']
    if FL1=='Y' and FL2=='Y':
        merged_df = pd.merge(left=DF1, right=DF2, how='inner', on='GROUP')
        final_list = merged_df.values.tolist()
    # print (final_list)
    return final_list

#final_list1=transformation_extract("C:/Users/kanish/Desktop/UNDIAL FI/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.xml")
#print('Flag='+Flag)

#df = pd.DataFrame(final_list1)
###df.columns = ['Instance Name','Transformation type', 'GROUP','EXPRESSION','Column_Name']
###df.columns
#df.to_csv(outfile, index=False)


