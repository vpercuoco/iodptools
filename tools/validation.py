import pandas as pd
import pandera as pa
import numpy as np

# Poor data is captured in in thrown errors:
# lazy = True tells the engine to validate all columns before throwning error, otherwise error is thrown after first invalid column
def get_errors(dataframe: pd.DataFrame, dataframe_schema: pa.DataFrameSchema):
    errors = None
    try:
        dataframe_schema(dataframe, lazy=True)
    except pa.errors.SchemaErrors as errs:
        errors = errs.failure_cases
    
    if not errors is None:
        return errors
    else:
        return None
    

class BaseSchema():
    """A base schema for validating iodp data reports"""

    def __init__(self):
        pass
    
    @staticmethod
    def get_base_schema(self) -> pa.DataFrameSchema:
        core_type = ['H','X','F','R']
        archive_types = ['A','W']

        base_schema = pa.DataFrameSchema(
            {
                # Sample hierarchy columns:
                "Exp": pa.Column(str,checks = pa.Check.str_matches("^[A-Z0-9]+$"),coerce=True),
                "Site" : pa.Column(str, checks=pa.Check.str_matches('(U|J)[0-9]+')),
                "Hole": pa.Column(str,checks=pa.Check.str_matches("^[A-Z]$")),
                "Core" : pa.Column(int,coerce=True),
                "Type" : pa.Column(str,checks=pa.Check.isin(core_type),nullable=True),
                "Sect" : pa.Column(str, checks=pa.Check.str_matches("^[0-9]+$|^CC$"),coerce=True),
                "A/W" : pa.Column(pa.Category,checks=pa.Check.isin(archive_types),nullable=True,coerce=True),
                "Offset (cm)": pa.Column(float,checks=pa.Check.in_range(0,200)),
                "Depth .+" : pa.Column(float,checks=pa.Check.in_range(0,5000),regex=True), # uses regex to apply this column check to all "Depth...." columns
    
                # Additional metadata columns:
                "Instrument" : pa.Column(str),
                "Instrument group" : pa.Column(str),
                "Timestamp (UTC)" : pa.Column(str),
                "Text ID" : pa.Column(str,checks=pa.Check.str_matches("[A-Z]+[0-9]+"), nullable=True),
                "Test No." : pa.Column(int,checks=pa.Check.gt(0)),
                "(Sample|Test|Result) comments" : pa.Column(str,coerce=True,nullable=True,regex=True,) # this applies to three columns
            })
        return base_schema



class AnalysisSchema():
    
    def __init__(self):
        self.analysis_schemas = {}
        self.base_schema = BaseSchema().get_base_schema()
        self.generate_lore_report_basic_schemas()
    
    @staticmethod
    def generate_lore_report_basic_schemas(self):
        
        # Analysis Schemas
        
        ## SRM-Sect
        srm_treatments = ["NRM","IN-LINE AF DEMAG"]
        self.analysis_schemas['srm-sect'] = self.base_schema.add_columns({
            "Treatment Type": pa.Column(pa.Category,checks=pa.Check.isin(srm_treatments),nullable=True,coerce=True),
            "Treatment Value" : pa.Column(int,checks=pa.Check.in_range(0,100),coerce=True),
            "Inclination background & drift corrected (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Declination background & drift corrected (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Intensity background & drift corrected (A/m)" : pa.Column(float,coerce=True),
            "Inclination raw (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Declination raw (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Intensity raw (A/m)" : pa.Column(float,coerce=True),
            "Magnetic moment x" : pa.Column(float,coerce=True, regex=True),
            "Magnetic moment y" : pa.Column(float,coerce=True, regex=True),
            "Magnetic moment z" : pa.Column(float,coerce=True, regex=True),
            "Sample orientation" : pa.Column(str,nullable = True,coerce=True),
            "Raw data" : pa.Column(str,nullable = True,coerce=True)
        })
    
        ## MAD
        self.analysis_schemas['mad'] = self.base_schema.add_columns({
            "Submethod" : pa.Column(str,coerce=True),
            "Moisture dry (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), coerce=True),
            "Moisture wet (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), coerce=True),
            "Bulk density (g/cm³)": pa.Column(float,checks=pa.Check.gt(0), nullable=False, coerce=True),
            "Dry density (g/cm³)":  pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Grain density (g/cm³)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Porosity (vol%)": pa.Column(float,checks=pa.Check.in_range(0,100), coerce=True),
            "Void ratio": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Container number": pa.Column(int,checks=pa.Check.gt(0), coerce=True),
            "Mass wet sample (g)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Mass dried sample (g)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Mass porewater (g)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Mass salt (g)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Mass solids (g)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Vol wet sample (cm³)" : pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Vol dried sample (cm³)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Vol porewater (cm³)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Vol salt (cm³)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Vol solids (cm³)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            })
        
        ## RGB
        self.analysis_schemas['rgb'] = self.base_schema.add_columns({
            "R":  pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "G": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "B" : pa.Column(float,checks=pa.Check.gt(0), coerce=True) 
        })
        
        ## RSC
        self.analysis_schemas['rsc'] = self.base_schema.add_columns({
            "Reflectance L*":  pa.Column(float, coerce=True),
            "Reflectance a*": pa.Column(float, coerce=True),
            "Reflectance b*" : pa.Column(float, coerce=True),
            "Tristimulus X": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Tristimulus Y": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Tristimulus Z": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Normalized spectral data link": pa.Column(int,checks=pa.Check.gt(0), nullable=True, coerce=True),
            "Unnormalized spectral data link": pa.Column(int,checks=pa.Check.gt(0), nullable=True, coerce=True),
            
        })
        
        ## GRA
        self.analysis_schemas['gra'] = self.base_schema.add_columns({
            "Bulk density (GRA)":  pa.Column(float,checks=pa.Check.gt(0), coerce=True)
        })
        
        ## NGR
        self.analysis_schemas['ngr'] = self.base_schema.add_columns({
            "NGR total counts (cps)":  pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Error (cps)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Relative Error" : pa.Column(float,checks=pa.Check.gt(0), coerce=True) 
        })
    
    def get_analysis_schema(self, analysis:str) -> pa.DataFrameSchema:
        return self.analysis_schemas[analysis]
        


class ErrorPainter():
    """Class to find dataframe errors given a DataFrameSchema to compare against.
    """
    
    def __init__(self, dataframe: pd.DataFrame, schema: pa.DataFrameSchema):
        """Intializes ErrorPainter and find column/indices in dataframe breaking schema rules

        Args:
            dataframe (pd.DataFrame): Dataframe to find errors
            schema (pa.DataFrameSchema): A DataFrameSchema to compare against
        """
        self.dataframe = dataframe
        self.schema = schema
        self.errors = get_errors(self.dataframe,self.schema)
        
    def highlight_column_errors(self):
        """Renders dataframe error cells with a red background. 

        Returns:
            pd.io.formats.style.Styler: A styler object which renders the dataframe error cells in red. To access the underlying data use the .data method.
        """
        __highlighted_dataframe = None
        
        # Currently only returning errors for "Column", and filtering out datatype errors because they seem to break pandera
        __distinct_columns = self.errors[
            (self.errors['schema_context']=='Column') &
            (self.errors['index'].notnull()) &
            (self.errors['check_number'].notna())]
        
        __distinct_columns = np.unique(__distinct_columns.loc[:,'column']) #
        
    
        for col in __distinct_columns: 
            indices = self.errors[(self.errors['column']==col) & (self.errors['index'].notnull())].loc[:,'index']
            
            if __highlighted_dataframe == None:
                __highlighted_dataframe  = self.dataframe.style.applymap(self.__color_negative, color='red', subset=(indices,col))
            else:
                __highlighted_dataframe.applymap(self.__color_negative,color='red',subset=(indices,col))  
        
        self.highlighted_dataframe = __highlighted_dataframe
     
    def to_excel(self,filename: str):
        """Exports the styled dataframe to excel. Exporting to csv does not maintain highlights.

        Args:
            filename (str): A filename for the exported .xlsx.
        """
        self.highlighted_dataframe.to_excel(filename, index=False)
        print(f'saved as {filename}')
        
        
    def __color_negative(v, color):
        # return f"color: {color};" # foreground color
        return f'background-color: {color}' # background color

