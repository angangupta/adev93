#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xml.etree.ElementTree as ET
import csv
import os
import pandas as pd


# In[19]:


def filter_transform(filename):
    
    #tree = ET.parse('C:/Users/poojjoshi/Desktop/Undial/XML_Files/m_S_TXN_RECON_HIST_LOAD.XML')
    tree = ET.parse(filename)
    root=tree.getroot()
    # print(root.attrib.get("NAME"))
    filter_df=pd.DataFrame([])
    c=tree.iter('TRANSFORMATION')
    for node in c:
        nameofexp = node.attrib.get('NAME')
        typ = node.attrib.get('TYPE')
        if (typ=="Filter"):
            tableattr_list=node.findall("TABLEATTRIBUTE")
            for xx in tableattr_list:
                Trans_attrib=xx.attrib.get('NAME')
                if(Trans_attrib =='Filter Condition'):
                    Trans_filter_condition=xx.attrib.get('VALUE')

            transformfield_list=node.findall("TRANSFORMFIELD")
           

            for yy in transformfield_list:   
                Trans_fieldname=yy.attrib.get('NAME')
                Trans_porttype=yy.attrib.get('PORTTYPE')
                Trans_datatype=yy.attrib.get('DATATYPE')
                #expr11[J]={nameofexp,typ,JNR_attrib_join_typ,JNR_attrib_join_cond,JNR_fieldname,JNR_port_type}
                filter_df=filter_df.append(pd.DataFrame({'Script Name':filename.rsplit('/')[-1].replace('.XML',''), 'Input Transformation':nameofexp, 'Transformation Type': typ,
                                        'Field Name': Trans_fieldname,
                                        'Port Type': Trans_porttype,
                                        'Data Type': Trans_datatype,
                                        'Filter Condition': Trans_filter_condition,
                                                        }, index=[0]))
    #filter_df.to_csv("C:/Users/poojjoshi/Desktop/Undial/Filter_INFA.csv",header=True,index = False)          
    return filter_df.values.tolist()   


# In[12]:





# In[20]:





# In[14]:





# In[ ]:




