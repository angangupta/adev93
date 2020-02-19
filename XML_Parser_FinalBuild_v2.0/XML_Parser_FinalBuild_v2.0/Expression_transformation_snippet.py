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




def transformation_extract(a):
    input_file =a

    DOMTree = minidom.parse(input_file)


    final_list = []
    transformation_dict = {}
    main_tag_list = DOMTree.getElementsByTagName("TRANSFORMATION")
    for Type in main_tag_list:
        transformation_TYPE = Type.getAttribute('TYPE')
        instance_TYPE = Type.getAttribute('NAME')

        # print(transformation_TYPE)
        if transformation_TYPE == "Expression":
            # print('step1')
            transformation_list = Type.getElementsByTagName("TRANSFORMFIELD")
            # print(transformation_list)

            dict = []
            iter = 1
            for transformation in transformation_list:

                row = []

                row.append(instance_TYPE)
                field_transformation_name = transformation.getAttribute('NAME')

                row.append(field_transformation_name)
                row.append(transformation_TYPE)
                field_transformation_name = transformation.getAttribute('PORTTYPE')

                CHECK = field_transformation_name
                row.append(field_transformation_name)
                field_transformation_name = transformation.getAttribute('EXPRESSION')

                if CHECK != "OUTPUT" and CHECK != 'LOCAL VARIABLE' and CHECK != 'LOOKUP/OUTPUT':
                    row.append('DIRECT MAP')
                else:
                    row.append(field_transformation_name)
                final_list.append(row)

    # print (final_list)
    return final_list




