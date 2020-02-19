import xml.dom.minidom as minidom
import os
import re
import pandas as pd
from collections import OrderedDict
import sys
import json
import subprocess,json
import itertools
from functools import reduce
import numpy as np
from xlwt import Workbook
from Expression_transformation_snippet import transformation_extract
from source_qualifier import source_qualifier_extract
from Pre_Post_SQLExtraction_Function import prepostSQLtrans
from Source_Target_DB_Extract_Function import Source_Target_DB_Extract_trans
#from Joiner_extract_Function import Joiner_transform
from Joiner_Lookup_extract_v2 import lookup_transform
from Filter_Extract import filter_transform
from router_snippet_updated import router_transformation_extract


dataframe_list=[]
file_index=0

for filename in os.listdir("input"):

    input_file=filename
    file_index=file_index+1
    DOMTree = minidom.parse(input_file)
    # transformations = minidom.parse(input_file)

    final_list_expression=transformation_extract(input_file)
    src_qualifier_list=source_qualifier_extract(input_file)
    src_target_db_extract_trans_list=Source_Target_DB_Extract_trans(input_file)
    Lookup_extract_list = lookup_transform(input_file)
    filter_list = filter_transform(input_file)
    pre_post_list=prepostSQLtrans(input_file)
    router_expression_list=router_transformation_extract(input_file)


    transformation_dict = {}
    connector_list = DOMTree.getElementsByTagName("CONNECTOR")

    target_field_instance_list=[]
    target_to_source_linked_dict={}

    mapping_list=DOMTree.getElementsByTagName("MAPPING")
    for mapping in mapping_list:
        mapping_name=mapping.getAttribute("NAME")

    for connector in connector_list:
        row=[]
        row.append(connector.getAttribute("TOINSTANCETYPE"))
        row.append(connector.getAttribute("TOINSTANCE"))
        row.append(connector.getAttribute("TOFIELD"))
        if (row not in target_field_instance_list):
            target_field_instance_list.append(row)

    def instance_wise_target_fields_list(instance_name,list):
        for connector in connector_list:
            if(str(connector.getAttribute("TOINSTANCE"))==instance_name and [str(connector.getAttribute("TOINSTANCETYPE")),str(connector.getAttribute("TOINSTANCE")),str(connector.getAttribute("TOFIELD"))] not in list):
                list.append([str(connector.getAttribute("TOINSTANCETYPE")),str(connector.getAttribute("TOINSTANCE")),str(connector.getAttribute("TOFIELD"))])
        return list


    def create_linked_list(instance_name,list):
        for connector in connector_list:
            if(str(connector.getAttribute("TOINSTANCE"))==instance_name and [str(connector.getAttribute("FROMINSTANCE")),str(connector.getAttribute("FROMINSTANCETYPE")),str(connector.getAttribute("TOINSTANCE")),str(connector.getAttribute("TOINSTANCETYPE"))] not in list):
                list.append([str(connector.getAttribute("FROMINSTANCE")),str(connector.getAttribute("FROMINSTANCETYPE")),str(connector.getAttribute("TOINSTANCE")),str(connector.getAttribute("TOINSTANCETYPE"))])
                create_linked_list(str(connector.getAttribute("FROMINSTANCE")), list)
        return list

    target_wise_linked_dict={}
    for connector in connector_list:
        if(connector.getAttribute("TOINSTANCETYPE")=="Target Definition"):
            list=[]
            list.append(connector.getAttribute("TOINSTANCE"))
            transformation_list=create_linked_list(str(connector.getAttribute("TOINSTANCE")),list)
            if(str(connector.getAttribute("TOINSTANCE")) not in target_wise_linked_dict.keys()):
                target_wise_linked_dict[str(connector.getAttribute("TOINSTANCE"))]=transformation_list
            #print(transformation_list)

    #(target_wise_linked_dict)

    def target_source_mapping(target_row):
        for connector in connector_list:
            source_row=['','','']
            if(connector.getAttribute("TOINSTANCETYPE")==target_row[0] and connector.getAttribute("TOINSTANCE")==target_row[1] and connector.getAttribute("TOFIELD")==target_row[2]):
                source_row=[str(connector.getAttribute("FROMINSTANCETYPE")),str(connector.getAttribute("FROMINSTANCE")),str(connector.getAttribute("FROMFIELD"))]
                return (source_row)
    #
    # #

    #Main Code Starts

    target_index=0
    def target_sorted_list(instance_name,list):
        for key in (target_wise_linked_dict[instance_name]):
            list.append(key)
        return list

    list=[]
    original_list=(target_sorted_list("SC_S_TXN_CONTRACT_GIFT_INSERT_RELEASE",list))

    def sorted_list(original_list):
        sorted_list=[]
        for key in original_list:
                for key2 in original_list:
                    if(key2[0] ==key[0] and key2[2] !=key[2] and key2 not in sorted_list):
                        sorted_list.append(key2)
                    elif(key2[2] ==key[2] and key2[0] !=key[0] and key2 not in sorted_list):
                        sorted_list.append(key2)
                if(key not in sorted_list):
                    sorted_list.append(key)
        return sorted_list

    for key in sorted_list(original_list):
        print(key)
#             key_field_list=instance_wise_target_fields_list(key,[])
#             for key_field in key_field_list:
#                 #Expression Extraction Code
#                 output_port_type = ''
#                 output_expression = ''
#                 for exp_record in final_list_expression:
#                     # print(record[2]+" "+record[4]+" "+exp_record[0]+" "+exp_record[1])
#                     if (target_source_mapping(key_field)[2] == exp_record[1] and target_source_mapping(key_field)[1] == exp_record[0]):
#                         output_port_type = exp_record[3]
#                         output_expression = exp_record[4]
#                 # Source Qualifier Extraction Code
#                 src_qualifier_sql=''
#                 for src_qual_record in src_qualifier_list:
#                     if (src_qual_record[0] == key_field[1]):
#                         src_qualifier_sql = src_qual_record[2]
#                 for lookup_extract_record in Lookup_extract_list:
#                     if (lookup_extract_record[1] == target_source_mapping(key_field)[1]):
#                         print("Found Lookup SQL Override")
#                         src_qualifier_sql = lookup_extract_record[7]
#                 # Pre Post Expression Extraction Code
#                 pre_post_sql=''
#                 for pre_post_record in pre_post_list:
#                     if (pre_post_record[1] == key_field[1] and pre_post_record[2] == key_field[0]):
#                         pre_post_sql = pre_post_record[3]
#                 #Joiner Extract Code
#                 joiner_extract=''
#                 for lookup_extract_record in Lookup_extract_list:
#                     if (lookup_extract_record[1] == target_source_mapping(key_field)[1]):
#                         print("Found Lookup Join Condition")
#                         joiner_extract = lookup_extract_record[4]
#
#                 #Filter Extract Code
#                 filter=''
#                 for filter_record in filter_list:
#                     if (target_source_mapping(key_field)[0] == "Filter" and filter_record[3] == target_source_mapping(key_field)[2]):
#                         filter = filter_record[6]
#                 for router_expression in router_expression_list:
#                     if (target_source_mapping(key_field)[0] == "Router" and router_expression[0] ==target_source_mapping(key_field)[1] and router_expression[4]==target_source_mapping(key_field)[2]):
#                         filter = router_expression[3]
#                         print(filter)
#                         break
#
#                 #Source Filter Extract Code
#                 source_filter=''
#                 for src_target_db_extract_record in src_target_db_extract_trans_list:
#                     if (src_target_db_extract_record[1] == target_source_mapping(key_field)[1] ):
#                         source_filter = src_target_db_extract_record[5]
#
#                 df_row=[file_index,target_index,mapping_name,target_source_mapping(key_field)[0],target_source_mapping(key_field)[1],source_filter,target_source_mapping(key_field)[2],key_field[0],key_field[1],key_field[2],output_port_type,output_expression,src_qualifier_sql,pre_post_sql,joiner_extract,filter]
#                 print(df_row)
#                 dataframe_list.append(df_row)
#
#
#
# df=pd.DataFrame(columns = ['Mapping Index','Target Index','Mapping Name','Source Instance Type','Source Instance', 'Source Location','Source Field','Target Instance Type','Target Instance', 'Target Field','Port Type','Expression','Source Override SQL','Pre Post SQL','Joiner Extract','Filter Expression'],data=dataframe_list)
#     # print(df)
# df.to_csv("Output_Dataframe.csv",index=False)
#     # #print(target_field_instance_list)
#     #print (target_to_source_linked_dict)