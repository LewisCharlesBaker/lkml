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

    file_name = row.table_name
    
    view = { "view_name": row.table_name,

    "dimension": {
        "type": row.data_type,
        "sql": row.table_name,
        "name": row.column_name
    }
}
 
    print(looker.dump(view))



   ##ß script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
   ##ß rel_path = "lookml/view"
   ##ß path = os.path.join(script_dir, rel_path)

   ##ß if not os.path.exists(path):
   ##ß     os.makedirs(path)

   ## filename = '%s.view' % view_name
   ## with open(os.path.join(path, filename), 'w') as f:
   ##     f.print(looker.dump(lookml)

   ## filename = '%s.view' % view_name
   ## with open(os.path.join(path, filename), 'w') as file:
   ##     result = looker.load(file)

###################


   # script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
  #  rel_path = "lookml/view"
  #  path = os.path.join(script_dir, rel_path)

  #  if not os.path.exists(path):
   #     os.makedirs(path)

   # filename = '%s.view' % view_name
  # with open(os.path.join(path, filename), 'w') as f:
  #      looker.dump(f,lookml)

                ##        v.generate_lookml(f, GeneratorFormatOptions(view_fields_alphabetical=False))
##################

  # with open('out.txt', 'w') as f:
 #       print(looker.dump(lookml))
 #       print('Filename:', filename, file=f)  # Python 3.x


########

#print(lkml.dump(lookml))