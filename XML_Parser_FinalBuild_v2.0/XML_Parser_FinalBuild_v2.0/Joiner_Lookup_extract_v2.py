#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[23]:


import xml.etree.ElementTree as ET
import csv
import os
import pandas as pd




# In[19]:



# In[33]:


def lookup_transform(filename):
    
    tree = ET.parse(filename)
    root=tree.getroot()
    # print(root.attrib.get("NAME"))
    JNR_attrib_join_typ=""
    JNR_attrib_join_cond=""
    JNR_field_name=""
    JNR_porttype=""
    nameofexp=""
    typ=""
    ly=[]
    joiner_df=pd.DataFrame([])
    c=tree.iter('TRANSFORMATION')
    for node in c:
        nameofexp = node.attrib.get('NAME')
        typ = node.attrib.get('TYPE')
        if (typ=="Joiner"):
            lx=node.findall("TABLEATTRIBUTE")
            for xx in lx:
                JNR_attrib=xx.attrib.get('NAME')
                if(JNR_attrib =='Sql Query'):
                    JNR_attrib_join_typ=xx.attrib.get('VALUE')
                elif (JNR_attrib =='Join Condition'):
                    JNR_attrib_join_cond=xx.attrib.get('VALUE')
            ly=node.findall("TRANSFORMFIELD")


            for yy in ly:   
                JNR_fieldname=yy.attrib.get('NAME')
                JNR_port_type=yy.attrib.get('PORTTYPE')
                #expr11[J]={nameofexp,typ,JNR_attrib_join_typ,JNR_attrib_join_cond,JNR_fieldname,JNR_port_type}
                joiner_df=joiner_df.append(pd.DataFrame({'Script Name':filename.rsplit('/')[-1].replace('.XML',''), 'Input Transformation':nameofexp, 'Transformation': typ,
                                                         'Join Type': JNR_attrib_join_typ,
                                                         'Join Condition': JNR_attrib_join_cond,
                                                         'Field Name': JNR_fieldname,
                                                         'Port Type': JNR_port_type,
                                                         'Lookup sql override':'NA'
                                                        }, index=[0]))
        if (typ=="Lookup Procedure"):
            
            nameofexp = node.attrib.get('NAME')
            lx=node.findall("TABLEATTRIBUTE")
            for xx in lx:
                JNR_attrib=xx.attrib.get('NAME')
                if(JNR_attrib =='Lookup condition'):
                    JNR_attrib_join_cond=xx.attrib.get('VALUE')
                if(JNR_attrib =='Lookup Sql Override'):
                    Lookup_SQL_ovverride=xx.attrib.get('VALUE')
            joiner_df=joiner_df.append(pd.DataFrame({'Script Name':filename.rsplit('/')[-1].replace('.XML',''), 'Input Transformation':nameofexp, 'Transformation': typ,
                                                         'Join Type': 'NA',
                                                         'Join Condition': JNR_attrib_join_cond,
                                                         'Field Name': 'NA',
                                                         'Port Type': 'NA',
                                                         'Lookup sql override':Lookup_SQL_ovverride
                                                        }, index=[0]))
                    



    return joiner_df.values.tolist()


#print(Joiner_transform("C:/Users/angangupta4/Downloads/Query_Builder/XML/m_PS_TO_STG_S_TXN_CONTRACT_GIFT_LOAD.XML"))