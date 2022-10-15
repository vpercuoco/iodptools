# IODPTools


## Finding and displaying report errors:



```python
# install packages:
!pip uninstall iodptools --yes
!pip install --upgrade git+https://github.com/vpercuoco/iodptools

# import packages:
from tools import iodptools
import pandas as pd

# import dataframe
df = pd.read_csv('./mad.csv')

# Import analysis schema:
mad_schema = iodptools.AnalysisSchema().get_analysis_schema('mad')

# Find errors and color dataframe
painter = ErrorPainter(df, mad_schema)
painter.highlight_column_errors()

# Display dataframe of columns and indices where schema rules were broken:
painter.errors

# Display highlighted dataframe
painter.highlighted_dataframe

# Export highlighted dataframe to excel
painter.to_excel(filename='./mad_with_errors.xlsx')
```



## List available schemas
```python
schema = iodptools.AnalysisSchema()
print(schema.analysis_schemas.keys())
```
