
import pandas as pd
from  Create_Columns import get_MBSAQIP_comp
import polars as pl

#rename all columns according to name changes
df_0 = pl.scan_csv("MBSAQIP_22_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_0 = df_0.rename({'EMERG_VISIT_OUT': 'PRIORITY',  'PROCEDURE_TYPE':  'CPTUNLISTED_REVCONV'})

df_1 = pl.scan_csv("MBSAQIP_21_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_1 = df_1.rename({'EMERG_VISIT_OUT': 'PRIORITY',  'PROCEDURE_TYPE':  'CPTUNLISTED_REVCONV'})

df_2 = pl.scan_csv("MBSAQIP_20_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_2 = df_2.rename({'HIP': 'HYPERTENSION', 'CHRONIC_STEROIDS': 'IMMUNOSUPR_THER', 'EMERG_VISIT_OUT': 'PRIORITY', 'PROCEDURE_TYPE':  'CPTUNLISTED_REVCONV'})

df_3 = pl.scan_csv("MBSAQIP_19_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_3 = df_3.rename({'HIP': 'HYPERTENSION', 'CHRONIC_STEROIDS': 'IMMUNOSUPR_THER', 'SURGICAL_APPROACH': 'ROBOTIC_ASST'})
df_4 = pl.scan_csv("MBSAQIP_18_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_4 = df_4.rename({'HIP': 'HYPERTENSION', 'CHRONIC_STEROIDS': 'IMMUNOSUPR_THER', 'SURGICAL_APPROACH': 'ROBOTIC_ASST'})
df_5 = pl.scan_csv("MBSAQIP_17_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_5 = df_5.rename({'HIP': 'HYPERTENSION', 'CHRONIC_STEROIDS': 'IMMUNOSUPR_THER', 'SURGICAL_APPROACH': 'ROBOTIC_ASST'})
df_6 = pl.scan_csv("MBSAQIP_16_Path", separator = '\t', ignore_errors= True).with_columns(pl.all().cast(pl.Utf8))
df_6 = df_6.rename({'HIP': 'HYPERTENSION', 
                    'CHRONIC_STEROIDS': 'IMMUNOSUPR_THER', 
                    'SURGICAL_APPROACH': 'ROBOTIC_ASST',
                    })

df = pl.concat([df_0, df_1, df_2], how = 'diagonal_relaxed')
print(df.columns)

#Choose columns to keep
comp_arr = ['DTACTERENALFAILURE','DTDEATH_OP',  'DTCARDIACARRESTCPR',
'DTMYOCARDIALINFR','DTTRANSFINTOPPSTOP', 'DTPOSTOPSEPSIS', 
'DTPROGRSRENALINSUF',  'DTCVA', 
'DTPOSTOPORGANSPACESSI', 'DTPOSTOPDEEPINCISIONALSSI', 'DTPULMONARYEMBOLSM', 
'DTVEINTHROMBREQTER', 'DTPOSTOPVENTILATOR',  'DTPOSTOPUTI',
'DTPOSTOPPNEUMONIA']
for i in range(len(comp_arr)):
    column_name = comp_arr[i] + '_30DAY'
    df = df.with_columns(
        pl.when(pl.col(comp_arr[i]).is_null()).then(0.0).otherwise(1.0).alias(column_name)
    )
print(df.describe())

for i in range(0, len(comp_arr)):
    comp_arr[i] = comp_arr[i] + '_30DAY'
    
DAY_30 = df.select(pl.col(comp_arr)).collect()
print(df.columns)


df = df.with_columns(
    pl.all().replace({'Yes': 1.0, 'No': 0.0, 'Female': 1.0, 'Male': 0.0,
    'Robotic-assisted': 1.0, 'Open': 0.0, 
    'Conventional laparoscopic (thoracoscopic)': 0.0, 
    'N.O.T.E.S. (Natural Orifice Transluminal Endoscopic Surgery)': 0.0,
    'Single Incision': 0.0, 'Hand-assisted': 0.0,
    'Laparoscopic assisted (thoracoscopic assisted)': 0.0, 
    'Insulin': 1.0, 'Non-Insulin': 1.0, 
    'Yes, insulin': 1.0, 'Yes, non-insulin': 1.0, 
    'Non-Binary': None, 'Non-binary': None, 'Unknown': 0.0, 'Initial': 0.0,
    'Conversion':1.0, 'Revision': 2.0}))

df = get_MBSAQIP_comp(df, 'DTACTERENALFAILURE', 'ACUTE_RENAL_FAILURE', 1)
df = get_MBSAQIP_comp(df, 'DTDEATH_OP', 'DEATH', 2)
df = get_MBSAQIP_comp(df, 'DTCARDIACARRESTCPR', 'CARDIAC_ARREST', 3)
df = get_MBSAQIP_comp(df, 'DTMYOCARDIALINFR', 'MI', 4)
df = get_MBSAQIP_comp(df, 'DTTRANSFINTOPPSTOP', 'BLOOD_TRANSFUSION', 5)
df = get_MBSAQIP_comp(df, 'DTPOSTOPSEPSIS', 'SEPSIS', 6)

df = get_MBSAQIP_comp(df, 'DTPOSTOPSUPERFINCSSI', 'INFECTION_1', 10)
df = get_MBSAQIP_comp(df, 'DTPOSTOPORGANSPACESSI', 'INFECTION_2', 11)
df = get_MBSAQIP_comp(df, 'DTPOSTOPDEEPINCISIONALSSI', 'INFECTION_3', 12)
df = get_MBSAQIP_comp(df, 'DTPULMONARYEMBOLSM', 'PE', 13)
df = get_MBSAQIP_comp(df, 'DTVEINTHROMBREQTER', 'DVT', 17)
df = get_MBSAQIP_comp(df, 'DTPOSTOPVENTILATOR', 'VENTILATOR', 14)
df = get_MBSAQIP_comp(df, 'DTPOSTOPUTI', 'UTI', 15)
df = get_MBSAQIP_comp(df, 'DTPOSTOPPNEUMONIA', 'PNEUMONIA', 16)

print(df.columns)

necessary_cols = [
'ACUTE_RENAL_FAILURE', 'DEATH', 'CARDIAC_ARREST', 'MI',
'BLOOD_TRANSFUSION', 'SEPSIS',
'INFECTION_1', 'DTDEATH_OP', 
'CARDIACARRESTCPR', 'PREVIOUS_SURGERY',
'INFECTION_2', 'INFECTION_3', 'DVT',
'PE', 'VENTILATOR', 'UTI', 'PNEUMONIA','DIABETES',
'HYPERTENSION', 'HISTORY_PE', 'HYPERLIPIDEMIA', 'HISTORY_DVT',
'COPD', 'SLEEP_APNEA', 'GERD', 'IMMUNOSUPR_THER', 'SMOKER',
'DIALYSIS', 'AGE', 'RACE_PUF', 'OPYEAR', 'CPT', 'SEX', 
'VENOUS_STASIS', 'THERAPEUTIC_ANTICOAGULATION',
'ROBOTIC_ASST', 'PRIORITY', 'DTDISCH_OP', 'HISPANIC',
'OPLENGTH', 'BMI', 'ALBUMIN', 'IVC_FILTER', 'METH_VTEPROPHYL', 
'WGT_HIGH_BAR', 'WGT_HIGH_UNIT_BAR',
'WGT_CLOSEST', 'WGTUNIT_CLOSEST','OTHCPT1', 'OTHCPT2', 
'OTHCPT3', 'OTHCPT4', 'OTHCPT5',
'OTHCPT6', 'OTHCPT7', 'OTHCPT8', 'OTHCPT9', 'OTHCPT10',
'CONCPT1', 'CONCPT2', 'CONCPT3', 'CONCPT4', 'CONCPT5',
'CONCPT6', 'CONCPT7', 'CONCPT8', 'CONCPT9', 'CONCPT10']
df = df.select(pl.col(necessary_cols))

df = df.with_columns(
    pl.col('ACUTE_RENAL_FAILURE', 'DEATH', 'CARDIAC_ARREST', 'MI',
    'BLOOD_TRANSFUSION', 'SEPSIS','INFECTION_1', 'INFECTION_2', 'INFECTION_3',
    'PE', 'DVT', 'VENTILATOR', 'UTI', 'PNEUMONIA','DIABETES', 'PREVIOUS_SURGERY',
    'HYPERTENSION', 'HISTORY_PE', 'HYPERLIPIDEMIA', 'HISTORY_DVT',
    'COPD', 'SLEEP_APNEA', 'GERD', 'IMMUNOSUPR_THER', 'SMOKER','DIALYSIS',
    'OPYEAR', 'SEX', 'ROBOTIC_ASST', 'AGE', 'PRIORITY', 'DTDISCH_OP',
    'HISPANIC', 'THERAPEUTIC_ANTICOAGULATION', 'OPLENGTH', 'VENOUS_STASIS', 
    'BMI', 'WGT_HIGH_BAR', 'WGT_CLOSEST').cast(pl.Float32),
     pl.lit(1.0).alias('COUNT'),
     pl.when(pl.any_horizontal(pl.col('INFECTION_1') == 1.0, 
                               pl.col('INFECTION_2') == 1.0))
                               .then(1.0).otherwise(0.0).alias('INFECTION'),
     pl.col('METH_VTEPROPHYL', 'WGT_HIGH_UNIT_BAR', 'WGTUNIT_CLOSEST').cast(pl.Utf8)
)

print(df.columns)

temp_1 = df.select(pl.col('RACE_PUF')).collect()
temp_1 = temp_1.to_dummies()
print(temp_1.columns)
temp_1 = temp_1.rename({'RACE_PUF_Asian': 'ASIAN', 
                        'RACE_PUF_Black or African American': 'BLACK',
                        'RACE_PUF_White': 'WHITE',
                        'RACE_PUF_American Indian or Alaska Native': 'NATIVE_AMERICAN'})
temp_1 = temp_1.select(pl.col(['ASIAN', 'BLACK', 'WHITE', 'NATIVE_AMERICAN']))
df = df.collect()
df = pl.concat([df, temp_1], how = 'horizontal')
'''
df = df.select(pl.col('COUNT', 'CPT', 'ACUTE_RENAL_FAILURE', 'DEATH', 'CARDIAC_ARREST', 'MI',
    'BLOOD_TRANSFUSION', 'SEPSIS',
    'INFECTION','DVT',
    'PE', 'VENTILATOR', 'UTI', 'PNEUMONIA','DIABETES', 'IMMUNOSUPR_THER',
    'HYPERTENSION', 'HISTORY_PE', 'HYPERLIPIDEMIA', 'HISTORY_DVT',
    'COPD', 'SLEEP_APNEA', 'GERD', 'SMOKER','DIALYSIS', 'PREVIOUS_SURGERY',
    'OPYEAR', 'SEX', 'ROBOTIC_ASST', 'AGE', 'PRIORITY', 'DTDISCH_OP',
    'HISPANIC', 'ASIAN', 'BLACK', 'WHITE', 'NATIVE_AMERICAN', 'THERAPEUTIC_ANTICOAGULATION','OPLENGTH', 'BMI'))
'''
print(temp_1.columns)
for i in DAY_30.columns:
    print(DAY_30.select(pl.col(i).value_counts()))


final = pl.concat([df, DAY_30], how = 'horizontal')
final = final.rename({'DTACTERENALFAILURE_30DAY': 'ACUTE_RENAL_FAILURE_30DAY', 'DTDISCH_OP': 'LOS',
'DTDEATH_OP_30DAY': 'MORTALITY_30DAY', 
'DTCARDIACARRESTCPR_30DAY': 'CARDIAC_ARREST_30DAY',
'DTMYOCARDIALINFR_30DAY': 'MI_30DAY', 
'DTTRANSFINTOPPSTOP_30DAY': 'BLOOD_TRANSFUSION_30DAY',
'DTPOSTOPSEPSIS_30DAY': 'SEPSIS_30DAY', 
'DTPULMONARYEMBOLSM_30DAY': 'PE_30DAY',
'DTVEINTHROMBREQTER_30DAY': 'DVT_30DAY',
 'DTPOSTOPVENTILATOR_30DAY': 'VENTILATOR_30DAY',
'DTPOSTOPUTI_30DAY': 'UTI_30DAY', 
'DTPOSTOPPNEUMONIA_30DAY': 'PNEUMONIA_30DAY',
 'SEX': 'FEMALE',
'OPYEAR': 'YEAR', 'PREVIOUS_SURGERY': 'HISTORY_BARIATRIC_SURGERY',
'THERAPEUTIC_ANTICOAGULATION': 'ANTICOAGULANTS',
 'ROBOTIC_ASST': 'ROBOTIC_APPROACH'})
df = final.with_columns(
pl.when(pl.any_horizontal(pl.col('BLOOD_TRANSFUSION') == 1.0, 
pl.col('UTI') == 1.0, 
pl.col('VENTILATOR') == 1.0, 
pl.col('SEPSIS') == 1.0, 
pl.col('PNEUMONIA') == 1.0,
pl.col('INFECTION') == 1.0,
pl.col('MI') == 1.0,
pl.col('CARDIAC_ARREST') == 1.0,
pl.col('DVT') == 1.0,
pl.col('PE') == 1.0,
pl.col('ACUTE_RENAL_FAILURE') == 1.0))
                                .then(1.0).otherwise(0.0).alias('MORBIDITY')
)


temp = df.filter(pl.any_horizontal(pl.col('CPT').str.starts_with('43775'), pl.col('CPT').str.starts_with('43644')))
print(temp.columns)
temp.write_csv("MBSAQIP_Cleaned.csv")