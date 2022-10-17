import pandas as pd
import pandera as pa
import numpy as np

# Poor data is captured in in thrown errors:
# lazy = True tells the engine to validate all columns before throwning error, otherwise error is thrown after first invalid column
def get_errors(dataframe: pd.DataFrame, dataframe_schema: pa.DataFrameSchema):
    """Gets dataframe errors when comparing against a given schema

    Args:
        dataframe (pd.DataFrame): A pandas dataframe
        dataframe_schema (pa.DataFrameSchema): A pandera dataframeschema

    Returns:
        dataframe (pd.DataFrame): A dataframe showing indices and columns of violated schema rules when they exist, otherwise None.
    """
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
                "Top offset .+": pa.Column(float,checks=pa.Check.in_range(0,200),regex=True),
                "Bottom offset .+": pa.Column(float,checks=pa.Check.in_range(0,200),regex=True),
                "Offset on .+":  pa.Column(float,checks=pa.Check.in_range(0,200),regex=True),
                
                "Depth .+" : pa.Column(float,checks=pa.Check.in_range(0,5000),regex=True), # uses regex to apply this column check to all "Depth...." columns
                "Top depth .+" : pa.Column(float,checks=pa.Check.in_range(0,5000),regex=True),
                "Bottom depth .+" : pa.Column(float,checks=pa.Check.in_range(0,5000),regex=True),
    
                # Additional metadata columns:
                "Label (I|i)dentifier" : pa.Column(str, coerce=True,regex=True),
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
    
    def generate_lore_report_basic_schemas(self):
        
        # Analysis Schemas
        
        
        ### Summaries ###
        
        # Forgo adding summary reports at the moment. Must account for "Totals" row at the bottom of tables.
        
        self.analysis_schemas['PIECELOG'] = self.base_schema.add_columns({
            "Piece Number": pa.Column(int, checks=pa.Check.gt(0), nullable=False, coerce=True),
            "Bin length (cm)": pa.Column(int, checks=pa.Check.gt(0), nullable=False, coerce=True),
            "Whole-round piece length (cm)" : pa.Column(int, checks=pa.Check.gt(0), nullable=True, coerce=True),
            "Comments": pa.Column(str,coerce=True)
        })
        
        ### Images ###
        
        ## LSIMG
        self.analysis_schemas['LSIMG'] = self.base_schema.add_columns({
            "Display status (T/F)": pa.Column(pa.Category, checks=pa.Check.isin(['T','F']), nullable=False, coerce=True),
            "Uncropped image (JPG) link":  pa.Column(int, nullable=False, coerce=True),
            "Uncropped image filename": pa.Column(str, nullable=False, coerce=True),
            "Cropped image (JPG) link": pa.Column(int, nullable=False, coerce=True),
            "Cropped image filename":  pa.Column(str, nullable=False, coerce=True)
        })
        
        
        ## MICROIMG
        illumination_types = ['Reflected', 'Transmitted']
        contrast_method = ['Uncrossed polarisation', 'Crossed polarization', 'Brightfield','Other: see comments']
        self.analysis_schemas['MICROIMG'] = self.base_schema.add_columns({
            "Image (JPG) link": pa.Column(int, nullable=False, coerce=True),
            "Image (Tif) link": pa.Column(int, nullable=False, coerce=True),
            "Image filename" :  pa.Column(str, nullable=False, coerce=True),
            "Illumination type" : pa.Column(pa.Category, checks=pa.Check.isin(illumination_types), nullable=False, coerce=True),
            "Contrast method" : pa.Column(pa.Category, checks=pa.Check.isin(contrast_method), nullable=False, coerce=True),  
        })
        
        ## TSIMG
        self.analysis_schemas['TSIMAGE'] = self.base_schema.add_columns({
            'Image (JPG) link': pa.Column(int, nullable=False, coerce=True),
            'Image Filename':  pa.Column(str, nullable=False, coerce=True),
            'Magnification': pa.Column(str, nullable=False, coerce=True),
            "Polarization":  pa.Column(str, nullable=False, coerce=True)
        })
        
        ## CLOSEUP
        self.analysis_schemas['CLOSEUP'] = self.base_schema.add_columns({
            'Image (JPG) link':  pa.Column(int, nullable=False, coerce=True),
            'Image (TIF) link':  pa.Column(int, nullable=False, coerce=True),
            'Image filename':  pa.Column(str, nullable=False, coerce=True),
            'State': pa.Column(pa.Category, checks=pa.Check.isin(['Dry','Wet']), nullable=False, coerce=True),
        })
        
        ## SEM
        sem_categories = ['FOSSIL','LITHOLOGY']
        self.analysis_schemas['SEM'] = self.base_schema.add_columns({
            "Image link": pa.Column(int, nullable=False, coerce=True),
            "Image Filename": pa.Column(str, nullable=False, coerce=True),
            "Image Metadata": pa.Column(int, nullable=False, coerce=True),
            "Category": pa.Column(pa.Category, checks=pa.Check.isin(sem_categories), nullable=False, coerce=True),  
            "Observable":  pa.Column(str, coerce=True),
            "Magnification": pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True)
        })
        
        ## WRLSC
        self.analysis_schemas['WRLSC'] = self.base_schema.add_columns({
            "quadrant image (JPG) link": pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True, regex=True),
            "composite image (JPG) link":  pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True, regex=True)
        })
        
        ### Magnetism ###
        
        ## SRM-SECT
        srm_treatments = ["NRM","IN-LINE AF DEMAG"]
        self.analysis_schemas['SRM-SECT'] = self.base_schema.add_columns({
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
    
        self.analysis_schemas['SRM-DISC'] = self.base_schema.add_columns({
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
    
        ## SPINNER
        self.analysis_schemas['SPINNER'] = self.base_schema.add_columns({
            "Inclination (Â°)":  pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Declination (Â°)":  pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Inclination (geographic) (Â°)":  pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Declination (geographic) (Â°)":  pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Total intensity (A/m)" : pa.Column(float,coerce=True),
            "Intensity X (A/m)" : pa.Column(float,coerce=True),
            "Intensity Y (A/m)" : pa.Column(float,coerce=True),
            "Intensity Z (A/m)": pa.Column(float,coerce=True),
            "Treatment type":  pa.Column(pa.Category,checks=pa.Check.isin(srm_treatments),nullable=True,coerce=True),
            "Treatment value (mT or Â°C or ex. B14)" : pa.Column(float,coerce=True),
            "Azimuth" : pa.Column(float,coerce=True),
            "Dip": pa.Column(float,coerce=True),
            "Sample type": pa.Column(str),
            "Sample volume (cm3)": pa.Column(float,coerce=True),
            "Raw data .csv file":  pa.Column(int,coerce=True),
            "Raw data .txt file":  pa.Column(int,coerce=True),
            "Raw data .jr6odp file":  pa.Column(int,coerce=True),
            "Raw data .jr6 file":  pa.Column(int,coerce=True),
            "Raw data .dat file":  pa.Column(int,coerce=True)                             
        })
        
        ## MSLOOP
        self.analysis_schemas['MSLOOP'] = self.base_schema.add_columns({
            "Magnetic susceptibility (instr. units)" : pa.Column(float,coerce=True)    
        })
        
        ### Physical Properties ###
        
        ## DHTEMP
        self.analysis_schemas['DHTEMP'] = self.base_schema.add_columns({
            "Measurement Depth (MBSF)": pa.Column(float,nullable = False, coerce=True), 
            "Equilibrium temperature (Â°C)" : pa.Column(float,nullable = False, coerce=True), 
            "Analysis program": pa.Column(str,coerce=True), 
            "Analysis report link": pa.Column(int,coerce=True),
            "Results image link": pa.Column(int,coerce=True),
            "Contour image link": pa.Column(int,coerce=True),
            "Session file link":pa.Column(int,coerce=True),
            "Raw data link": pa.Column(int,nullable=False,coerce=True)       
            })
        
    
        ## MAD
        self.analysis_schemas['MAD'] = self.base_schema.add_columns({
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
        self.analysis_schemas['RGB'] = self.base_schema.add_columns({
            "R":  pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "G": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "B" : pa.Column(float,checks=pa.Check.gt(0), coerce=True) 
        })
        
        ## RSC
        self.analysis_schemas['RSC'] = self.base_schema.add_columns({
            "Reflectance L*":  pa.Column(float, coerce=True),
            "Reflectance a*": pa.Column(float, coerce=True),
            "Reflectance b*" : pa.Column(float, coerce=True),
            "Tristimulus X": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Tristimulus Y": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Tristimulus Z": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Normalized spectral data link": pa.Column(int,checks=pa.Check.gt(0), nullable=True, coerce=True),
            "Unnormalized spectral data link": pa.Column(int,checks=pa.Check.gt(0), nullable=True, coerce=True)    
        })
        
        ## GRA
        self.analysis_schemas['GRA'] = self.base_schema.add_columns({
            "Bulk density (GRA)":  pa.Column(float,checks=pa.Check.gt(0), coerce=True)
        })
        
        ## NGR
        self.analysis_schemas['NGR'] = self.base_schema.add_columns({
            "NGR total counts (cps)":  pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Error (cps)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Relative Error" : pa.Column(float,checks=pa.Check.gt(0), coerce=True) 
        })
    
        ## PWAVE-C
        self.analysis_schemas['PWAVE-C-SECT'] = self.base_schema.add_columns({
            "P-wave velocity x (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True), 
            "P-wave velocity y (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity unknown (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Caliper separation (mm)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity x [manual] (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity y [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity unknown [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime [manual] (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True)    
        })
        
        self.analysis_schemas['PWAVE-C-DISC'] = self.base_schema.add_columns({
            "P-wave velocity x (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True), 
            "P-wave velocity y (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity unknown (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Caliper separation (mm)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity x [manual] (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity y [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity unknown [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime [manual] (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True)    
        })
        
        ## PWAVE-B
        self.analysis_schemas['PWAVE-B'] = self.base_schema.add_columns({
            "P-wave velocity y (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Caliper separation (mm)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity y [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "P-wave velocity z [manual] (m/s)":pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime [manual] (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True)    
        })
        
        ## PWAVE-L
        self.analysis_schemas['PWAVE-L'] = self.base_schema.add_columns({
            "P-wave velocity xy (m/s)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Caliper separation (mm)": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Sonic traveltime (Âµs)": pa.Column(float,checks=pa.Check.gt(0), coerce=True)
        })
        
        ## TCON
        self.analysis_schemas['TCON'] = self.base_schema.add_columns({
            "Thermal conductivity mean (W/(m*K))": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Conductivity std. dev (W/(m*K))": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Measurements (no)": pa.Column(int,checks=pa.Check.gt(0), coerce=True),
            "Thermal conductivity observations (W/(m*K))": pa.Column(float,checks=pa.Check.gt(0), coerce=True),
            "Calculated TCON value (W/(m*K))": pa.Column(float,checks=pa.Check.gt(0), nullable=False, coerce=True),
            "Needle name": pa.Column(str,nullable =False, coerce=True)
        })
        
        ## PEN
        self.analysis_schemas['PEN'] = self.base_schema.add_columns({
            "Compressional strength (KG/CM2)" : pa.Column(float,checks=pa.Check.ge(0), nullable = False, coerce=True),
            "Penetration direction": pa.Column(str, coerce=True),
            "Adapter foot identifier": pa.Column(str,coerce=True)
        })
        
        ## TOR
        self.analysis_schemas['TOR'] = self.base_schema.add_columns({
            "Torvane shear strength (KG/CM2)": pa.Column(float,checks=pa.Check.ge(0), nullable = False, coerce=True),
            "Penetration direction": pa.Column(str, coerce=True),
            "Adapter foot identifier": pa.Column(str, coerce=True)
        })
        
        ## ORIENT
        self.analysis_schemas['ORIENT'] = self.base_schema.add_columns({
            "Hx (nT)" : pa.Column(float, nullable = False, coerce=True),
            "Hv (nT)" : pa.Column(float, nullable = False, coerce=True),
            "Azimuth (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Dip (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Gravitational field (G)": pa.Column(float, checks=pa.Check.gt(0), nullable = False, coerce=True),
            "Magnetic dip (deg)" : pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Total magnetic field (mT)": pa.Column(float, nullable = False, coerce=True),
            "Magnetic tool face (deg)": pa.Column(float,checks=pa.Check.in_range(-360,360),coerce=True),
            "Temperature (Â°C)": pa.Column(float, checks=pa.Check.gt(0), nullable = False, coerce=True),
            "Tool ID": pa.Column(str,nullable = False,coerce=True)
        })
    
        ### XRAY ###
        
        ## XRD
        self.analysis_schemas['XRD'] = self.base_schema.add_columns({
            "Viewable file (PDF or PNG) link":  pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True),    
            "Viewable filename": pa.Column(str,coerce=False),
            "RAW or XRDML link": pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True),
            "UXD file link": pa.Column(int,checks=pa.Check.gt(0), nullable=False, coerce=True)
        })
        
        ## XRF
        
        ## PXRF   
        # The PXRF report template needs to be cleaned up by a developer before creating a schema

        ### CHEMISTRY ###
        
        ## CARB
        self.analysis_schemas['CARB'] = self.base_schema.add_columns({
            "Inorganic carbon (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Calcium carbonate (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Total carbon (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Hydrogen (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Nitrogen (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Sulfur (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
            "Organic carbon (wt%) by difference (CHNS-COUL)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=True, coerce=True),
        })
        
        ## GASSAFETY
        self.analysis_schemas['GASSAFETY'] = self.base_schema.add_columns({
            "Test list": pa.Column(str,coerce=True),
            "Methane (ppmv) GC3": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True),
            "Ethene (ppmv) GC3": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True),
            "Ethane (ppmv) GC3": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True),
            "Propene (ppmv) GC3": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True),
            "Propane (ppmv) GC3": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True),
            f"c1_c2_gc3 % GC3":  pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True)
            
        })
        
        ## IONCHROM
        
        ## IWREPORT
        # Notice the column names are regex searches
        self.analysis_schemas['IWREPORT'] = self.base_schema.add_columns({
            "SPEC": pa.Column(float,checks=pa.Check.ge(0),nullable=True,coerce=True,regex=True),
            "ICPAES": pa.Column(float,checks=pa.Check.ge(0),nullable=True,coerce=True,regex=True),
            "IC" : pa.Column(float,checks=pa.Check.ge(0),nullable=True,coerce=True,regex=True),
            "ALKALINITY" : pa.Column(float,checks=pa.Check.ge(0),nullable=True,coerce=True,regex=True),
            "SALINITY":  pa.Column(float,checks=pa.Check.ge(0),nullable=True,coerce=True,regex=True)
        })
        
        ## ICP-SOLIDS
        self.analysis_schemas['ICP-SOLIDS'] = self.base_schema.add_columns({
            "[\w]+ % .+ (nm)$": pa.Column(float,checks=pa.Check.in_range(0,100),nullable=True,coerce=True, regex=True),
            "[\w]+ ppm .+ (nm)$": pa.Column(float,checks=pa.Check.ge(0), nullable=True,coerce=True,regex=True)
        })
        
        ## SRA
        self.analysis_schemas['SRA'] = self.base_schema.add_columns({
            "S1 (mg HC/g C)" : pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "S2 (mg HC/g C)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "S3 (mg HC/g C)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "Tmax (Â°C)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "TOC (wt%)": pa.Column(float,checks=pa.Check.in_range(0,100), nullable=False,coerce=True),
            "Hydrogen index (HI) (n/a)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "Oxygen index (OI) (n/a)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "Pyrolysis carbon (PC) (mg/g)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "Production index (PI) (n/a)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True),
            "Pyrolysis carbon (PC) (mg/g)": pa.Column(float,checks=pa.Check.ge(0), nullable=False,coerce=True)
        })
        
        
         
    def get_analysis_schema(self, analysis:str) -> pa.DataFrameSchema:
        """Returns the LORE analysis schema given the name

        Args:
            analysis (str): The shorthand code for identifying a LORE report

        Returns:
            pa.DataFrameSchema: The report's associated pandera DataFrameSchema 
        """
        return self.analysis_schemas[analysis]
        


class ErrorChecker():
    """Class to find dataframe errors given a DataFrameSchema to compare against.
    """
    
    def __init__(self, dataframe: pd.DataFrame, schema: pa.DataFrameSchema):
        """Intializes ErrorChecker and finds column/indices in dataframe breaking schema rules

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
        highlight_dataframe = None
        
        # Currently only returning errors for "Column", and filtering out datatype errors because they seem to break pandera
        distinct_columns = self.errors[
            (self.errors['schema_context']=='Column') &
            (self.errors['index'].notnull()) &
            (self.errors['check_number'].notna())]
        
        distinct_columns = np.unique(distinct_columns.loc[:,'column']) #
        
    
        for col in distinct_columns: 
            indices = self.errors[(self.errors['column']==col) & (self.errors['index'].notnull())].loc[:,'index']
            
            if highlight_dataframe == None:
                highlight_dataframe  = self.dataframe.style.applymap(self.__color_negative, color='red', subset=(indices,col))
            else:
                highlight_dataframe.applymap(self.__color_negative,color='red',subset=(indices,col))  
        
        self.highlighted_dataframe = highlight_dataframe
     
    def to_excel(self,filename: str):
        """Exports the styled dataframe to excel. Exporting to csv does not maintain highlights.

        Args:
            filename (str): A filename for the exported .xlsx.
        """
        self.highlighted_dataframe.to_excel(filename, index=False)
        print(f'saved as {filename}')
        
        
    def __color_negative(self, v, color):
        # return f"color: {color};" # foreground color
        return f'background-color: {color}' # background color

