import lkml as looker
from pprint import pprint

import pandas as pd
import os
import pandas

import sys


sql = """

with source as (

select *,

case when data_type in  ('INT64','FLOAT64','NUMERIC') then ' number'
when data_type = 'BOOL' then ' yesno'
when data_type = 'TIMESTAMP' then ' time'
when data_type =  'STRING' then ' string'

end as looker_data_type

from `project-x-ray.analytics_qa.INFORMATION_SCHEMA.COLUMNS`

),

facts as (
select 
*
from source

where table_name like '%fact%'

or table_name like '%dim%'

)

select * from facts

"""

# Run a Standard SQL query using the environment's default project
df = pandas.read_gbq(sql, dialect='standard')

# Run a Standard SQL query with the project set explicitly
project_id = 'ra-development'
df = pandas.read_gbq(sql, project_id=project_id, dialect='standard')

for row in df.itertuples():
    
    view = { 

        "view": [

            {
        
                "dimensions": [

                {
                    "type": row.data_type,
                    "sql_table_name": row.column_name,
                    "hidden": "yes",
                    "name": row.column_name

                }

                ],

                "name": row.table_name,
            }
        ],
        

        }



    #value_list = view['dimension']
    #print('Values of key "dimension" are:')
    #print(value_list)

   # for key, value in view.items() :
   #     print (key, value)

    #print(looker.dump(view))

  #  print(view)

    for row in df.itertuples():
    
        print(looker.dump(view))

 ##       print (view)
