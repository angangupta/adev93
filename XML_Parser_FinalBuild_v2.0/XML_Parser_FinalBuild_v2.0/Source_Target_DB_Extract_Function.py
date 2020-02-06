#!/usr/bin/env python
# coding: utf-8

# In[4]:


import xml.etree.ElementTree as ET
import csv
import os
import pandas as pd
import numpy as np


# In[6]:


def Source_Target_DB_Extract_trans(filename):
    
    tree = ET.parse(filename)
    #tree = ET.parse('C:/Users/poojjoshi/Desktop/Undial/XML_Files/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.XML')
    root=tree.getroot()
    # print(root.attrib.get("NAME"))
    DB_table_typ=""
    DB_databasename=""
    DB_tablename=""
    DB_transname=""
    DB_transtype=""
    DB_insttype=""
    db_df=pd.DataFrame([])
    c=tree.iter('INSTANCE')
    for node in c:
        DB_insttype = node.attrib.get('TYPE')
        if(DB_insttype=="SOURCE"):
            DB_table_typ="SOURCE"
            DB_databasename = node.attrib.get('DBDNAME')
            DB_tablename = node.attrib.get('NAME')
            DB_transname = node.attrib.get('TRANSFORMATION_NAME')
            DB_transtype = node.attrib.get('TRANSFORMATION_TYPE')

            db_df=db_df.append(pd.DataFrame({'Script Name':'m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD' ,'Input Transformation':DB_transname, 'Table Name':DB_tablename,'Transformation_ Type': DB_transtype,
                                                         'Instance Type':DB_insttype ,
                                                         'Database Name': DB_databasename
                                                        }, index=[0]))

        #print(nameofexp+"--"+JNR_attrib_join_typ+ "--"+JNR_attrib_join_cond+"--"+JNR_fieldname+"--"+JNR_port_type)
        elif(DB_insttype=="TARGET"):
            DB_table_typ="SOURCE"
            DB_databasename = node.attrib.get('DBDNAME')
            DB_tablename = node.attrib.get('NAME')
            DB_transname = node.attrib.get('TRANSFORMATION_NAME')
            DB_transtype = node.attrib.get('TRANSFORMATION_TYPE')

            db_df=db_df.append(pd.DataFrame({'Script Name':'m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD' ,'Input Transformation':DB_transname, 'Table Name':DB_tablename,'Transformation_ Type': DB_transtype,
                                                         'Instance Type':DB_insttype ,
                                                         'Database Name': DB_databasename
                                                        }, index=[0]))

        #print(nameofexp+"--"+JNR_attrib_join_typ+ "--"+JNR_attrib_join_cond+"--"+JNR_fieldname+"--"+JNR_port_type)
    return db_df.values.tolist()


# In[7]:





# In[ ]:


#db_df.to_csv("C:/Users/poojjoshi/Desktop/Undial/Output_Files/DBInfo_INFA.csv",header=True,index = False)

