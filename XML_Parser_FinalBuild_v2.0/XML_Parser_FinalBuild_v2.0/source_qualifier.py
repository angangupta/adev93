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
#outfile = "C:/Users/kanish/Desktop/UNDIAL FI/m_STG_TO_HOPM_S_TXN_CPPH_HIST_LOAD_MONTHLY_REWARD_testrun_source.csv"


def source_qualifier_extract(a):
    input_file =a
    #outfile = input_file.replace('.xml', '_new_type_1.csv')

    DOMTree = minidom.parse(input_file)
    # transformations = minidom.parse(input_file)

    # In[16]:

    final_list = []
    transformation_dict = {}
    main_tag_list = DOMTree.getElementsByTagName("TRANSFORMATION")
    for Type in main_tag_list:
        transformation_TYPE = Type.getAttribute('TYPE')
        instance_TYPE = Type.getAttribute('NAME')

        # print(transformation_TYPE)


        if transformation_TYPE == "Source Qualifier":
            transformation_list = Type.getElementsByTagName("TRANSFORMFIELD")
            filter_list = Type.getElementsByTagName("TABLEATTRIBUTE")

            for Filter in filter_list:
                field_transformation_name = Filter.getAttribute('NAME')
                name_check = field_transformation_name
                if name_check == "Sql Query":
                    Sql_Query = Filter.getAttribute('VALUE')
            for transformation in transformation_list:
                row = []
                row.append(instance_TYPE)
                ###field_transformation_name = transformation.getAttribute('NAME')
                ###row.append(str(field_transformation_name))
                row.append(transformation_TYPE)
                ###field_transformation_name = transformation.getAttribute('PORTTYPE')
                ###row.append(str(field_transformation_name))

                filter_condition = Filter.getAttribute('VALUE')
                row.append(str(Sql_Query))
                final_list.append(row)
        elif transformation_TYPE == "Lookup Procedure":
            transformation_list = Type.getElementsByTagName("TRANSFORMFIELD")
            filter_list = Type.getElementsByTagName("TABLEATTRIBUTE")
            dict1 = []
            iter = 1
            row = []
            for Filter in filter_list:
                field_transformation_name = Filter.getAttribute('NAME')
                name_check = field_transformation_name
                if name_check == "Lookup Sql Override":
                 lookup_sql_Override = Filter.getAttribute('VALUE')
            for transformation in transformation_list:
                row = []
                row.append(instance_TYPE)
                #field_transformation_name = transformation.getAttribute('NAME')
                #row.append(str(field_transformation_name))
                row.append(transformation_TYPE)
                ###field_transformation_name = transformation.getAttribute('PORTTYPE')
                ###row.append(str(field_transformation_name))

                filter_condition = Filter.getAttribute('VALUE')
                row.append(str(lookup_sql_Override))
                final_list.append(row)
    df = pd.DataFrame(final_list)
    de = df.drop_duplicates()
    final_list=de.values.tolist()
    # print (final_list)
    return final_list

# print(source_qualifier_extract("C:/Users/angangupta4/Downloads/Query_Builder/XML/XML_Parser_Package/input/m_STG_TO_HOPM_S_TXN_CPPH_HIST_LOAD_MONTHLY_REWARD.XML"))

#
#
# final_list1=transformation_extract("C:/Users/kanish/Desktop/UNDIAL FI/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.xml")
# #print(final_list)
# df = pd.DataFrame(final_list1)
# df.columns = ['Field Name', 'transformation_TYPE', 'Transformation logic']
# ###de=df.drop_duplicates()
# df.to_csv(outfile, index=False)
#
#
