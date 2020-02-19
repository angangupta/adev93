#!/usr/bin/env python
# coding: utf-8

# In[53]:


import pandas as pd
from anytree import Node, RenderTree


# In[31]:


df=pd.read_csv('C:/Users/poojjoshi/Desktop/Undial/Output_Files/Src_target.csv')
noofRows=df.shape[0]
x=[]
root=Node(df["Source Name"][0])
print(root)
for i in range (0,noofRows):
    x.append(Node(df["Target Name"][i],parent=root))


# In[54]:


for pre, fill, node in RenderTree(root):
    print("%s%s" % (pre, node.name))


# In[ ]:




