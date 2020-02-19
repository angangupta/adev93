#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xml.etree.ElementTree as ET
import csv
import os
import pandas as pd
import numpy as np


# In[7]:


def prepostSQLtrans(filename):
    
    #tree = ET.parse('C:/Users/poojjoshi/Desktop/Undial/XML_Files/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.XML')
    tree = ET.parse(filename)
    root=tree.getroot()
    found=0
    sq_df=pd.DataFrame([])
    ##Source Qualifier Pre-SQL and Post-SQL
    c=tree.iter('TRANSFORMATION')
    for node in c:
        trans_type=""
        trans_name=""
        trans_attrib=""
        trans_sq_query=""
        trans_presql=""
        trans_postsql=""
        trans_type = node.attrib.get('TYPE')
        if(trans_type=="Source Qualifier"):
            found=1
            trans_name=node.attrib.get('NAME')
            lx=node.findall("TABLEATTRIBUTE")
            for xx in lx:
                trans_attrib=xx.attrib.get('NAME')

                if(trans_attrib =='Sql Query'):
                    trans_sq_query=xx.attrib.get('VALUE')
                elif(trans_attrib =='Pre SQL'):
                    trans_presql=xx.attrib.get('VALUE')
                elif(trans_attrib =='Post SQL'):
                    trans_postsql=xx.attrib.get('VALUE')
                    sq_df=sq_df.append(pd.DataFrame({'Script Name':'m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD' ,'Input Transformation':trans_name,'Transformation_ Type': trans_type,'SQL Query': trans_sq_query,'Pre SQL': trans_presql,'Post SQL': trans_postsql

                                                        }, index=[0]))
    ##Target Definition Pre-SQL and Post-SQL

    c=tree.iter('INSTANCE')
    for node in c:
        trans_type=""
        trans_name=""
        trans_attrib=""
        trans_sq_query=""
        trans_presql=""
        trans_postsql=""
        trans_type = node.attrib.get('TYPE')
        if(trans_type=="TARGET"):
            trans_name=node.attrib.get('NAME')
            found=1

            lx=node.findall("TABLEATTRIBUTE")
            for xx in lx:
                trans_attrib=xx.attrib.get('NAME')
                # print("attrib found is "+trans_attrib)
                if(trans_attrib =='Sql Query'):
                    trans_sq_query=xx.attrib.get('VALUE')
                elif(trans_attrib =='Pre SQL'):
                    trans_presql=xx.attrib.get('VALUE')
                elif(trans_attrib =='Post SQL'):
                    trans_postsql=xx.attrib.get('VALUE')
                sq_df=sq_df.append(pd.DataFrame({'Script Name':'m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD' ,'Input Transformation':trans_name,'Transformation_ Type': trans_type,'SQL Query': trans_sq_query,'Pre SQL': trans_presql,'Post SQL': trans_postsql
                                                    }, index=[0]))
            
    return sq_df.values.tolist()


# In[17]:





# In[8]:


#sq_list=prepostSQLtrans('C:/Users/poojjoshi/Desktop/Undial/XML_Files/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.XML')


# In[9]:





# In[ ]:


#sq_df.to_csv("C:/Users/poojjoshi/Desktop/Undial/Output_Files/PrePostsql_INFA.csv",header=True,index = False)

