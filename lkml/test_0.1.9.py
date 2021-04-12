import lkml
from pprint import pprint

import pandas as pd
import os
import pandas

import sys

lookml = {
    "joins": [
        {
            "relationship": "many_to_one",
            "type": "inner",
            "sql_on": "${view_one.dimension} = ${view_two.dimension}",
            "name": "view_two"
        },
        {
            "relationship": "one_to_many",
            "type": "inner",
            "sql_on": "${view_one.dimension} = ${view_three.dimension}",
            "name": "view_three"
        }
    ]

}

    #value_list = view['dimension']
    #print('Values of key "dimension" are:')
    #print(value_list)


    ##print(view)

##print(lkml.dump(lookml))

print(lookml)