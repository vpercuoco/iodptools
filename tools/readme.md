# IODPTools


## Finding and displaying report errors:

```python
# install packages:
!pip uninstall iodptools --yes
!pip install --upgrade git+https://github.com/vpercuoco/iodptools

# import packages:
from tools import validation as v
import pandas as pd

# import dataframe
df = pd.read_csv('./mad.csv')

# Import analysis schema:
mad_schema = v.AnalysisSchema().get_analysis_schema('mad')

# Find errors and color dataframe
checker = ErrorChecker(df, mad_schema)
checker.highlight_column_errors()

# Display dataframe of columns and indices where schema rules were broken:
checker.errors

# Display highlighted dataframe
checker.highlighted_dataframe

# Export highlighted dataframe to excel
checker.to_excel(filename='./mad_with_errors.xlsx')
```



## List available schemas
```python
schema = v.AnalysisSchema()
print(schema.analysis_schemas.keys())
```
