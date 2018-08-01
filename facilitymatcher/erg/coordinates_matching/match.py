import pandas as pd
import numpy as np
#Path to files
EIAinventoryfacilitytable="EIAinventoryfacilitytable.xlsx"
FRS_Facility="FRS_Facility.xlsx"

#Read first file
FRS_df=pd.read_excel(FRS_Facility)

#Remove text values first
cols = ['LATITUDE','LONGITUDE']
mask = FRS_df[cols].applymap(lambda x: isinstance(x, ( float)))
FRS_df[cols] = FRS_df[cols].where(mask)
#conevrt columns to float
FRS_df["LATITUDE"]=FRS_df["LATITUDE"].astype("float64")
FRS_df["LONGITUDE"]=FRS_df["LONGITUDE"].astype("float64")
#print(FRS_df.dtypes)
#Drop rows withput coordinates
FRS_df_NONA = FRS_df[np.isfinite(FRS_df['LATITUDE'])]
FRS_df_NONA = FRS_df[np.isfinite(FRS_df['LONGITUDE'])]
#read second excel
EIA_df=pd.read_excel(EIAinventoryfacilitytable)
#Remove text values first
EIA_df["Latitude"]=EIA_df["Latitude"].replace(r'\s+', np.nan, regex=True)
EIA_df["Longitude"]=EIA_df["Longitude"].replace(r'\s+', np.nan, regex=True)

EIA_df.columns=['Plant Code', 'Plant Name', 'Street Address', 'City', 'State', 'Zip',
                'County', 'LATITUDE', 'LONGITUDE']
#print(EIA_df.dtypes)
EIA_df_NONA = EIA_df[np.isfinite(EIA_df['LATITUDE'])]
EIA_df_NONA = EIA_df[np.isfinite(EIA_df['LONGITUDE'])]
#Round to 2 first
decimals=2
EIA_df_NONA["LATITUDE_R"]=EIA_df_NONA["LATITUDE"].round(decimals=decimals)
EIA_df_NONA["LONGITUDE_R"]=EIA_df_NONA["LONGITUDE"].round(decimals=decimals)
FRS_df_NONA["LATITUDE_R"]=FRS_df_NONA["LATITUDE"].round(decimals=decimals)
FRS_df_NONA["LONGITUDE_R"]=FRS_df_NONA["LONGITUDE"].round(decimals=decimals)
#Merge df
merged_df_NONA=FRS_df_NONA.merge(EIA_df_NONA,on=["LATITUDE_R","LONGITUDE_R"],how="inner")
#Calculate difference between values 
merged_df_NONA["LONGITUDE_del"]=abs(merged_df_NONA["LONGITUDE_x"]-merged_df_NONA["LONGITUDE_y"])
merged_df_NONA["LATITUDE_del"]=abs(merged_df_NONA["LATITUDE_x"]-merged_df_NONA["LATITUDE_y"])

accuracy=0.001
#filter values with match +/- 0.001
merged_df_NONA=merged_df_NONA[(merged_df_NONA["LONGITUDE_del"]<accuracy) & (merged_df_NONA["LATITUDE_del"]<accuracy)]
#filter with same postcode
#
# merged_df_NONA=merged_df_NONA[merged_df_NONA["Zip"]==merged_df_NONA["POSTAL_CODE"]]

merged_df_NONA.to_csv("test_result.csv")