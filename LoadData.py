import requests
import json
import pandas as pd
import numpy as np
import os

#os.chdir("C:\\Users\\gutie\\Dropbox\\Documentos Insumos\\")
#os.chdir("C:/Users/gutie/Dropbox/Documentos Insumos/PREWORK_CGF/Project-1/")
os.chdir("/Users/carlosgutz/Dropbox/Documentos Insumos/PREWORK_CGF/Project-1/")

url0="https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/"
tk="4e1130c8-d8dc-6097-906f-7a84cb402e3d"
indicador="6204482291"
6204482225,6204482291
idioma="es"
geo="00000"
version="2.0"
serie="false"
fuente="BIE"
formato="json"

url=url0+indicador+"/"+idioma+"/"+geo+"/"+serie+"/"+fuente+"/"+version+"/"+tk+"?type="+formato

response=requests.get(url)
print(response)
data=response.json()

print(json.dumps(data, indent=4, sort_keys=True))


#### Datos de Crimenes
#Datos de 2015 a 2019
crime= pd.read_csv("Data/IDM_NM2019.csv", delimiter=",", encoding='latin')
claves=pd.read_csv("Data/Claves Municipio.csv", delimiter=",", encoding='latin')
catalogo=pd.read_csv("Data/CatalogoMunicipios.csv", delimiter=",", encoding='utf-8')

### Building State Catalog with abreviated names
est=pd.DataFrame(catalogo[["Nom_Abr", "Cve_Ent", "Nom_Ent"]].drop_duplicates()).reset_index().drop(columns="index").rename(columns={"Nom_Abr":"Abr", "Cve_Ent":"id_estado", "Nom_Ent":"Estado"})
est["Abr"]=est["Abr"].str.upper().str.replace(".", "")


##### Investment Database preparation
###                 Annual Investment
inversion=pd.DataFrame()
for estado in est["Abr"]:
    inv=pd.read_excel("Data/Flujosporentidadfederativa.xls", sheet_name =estado,
                            nrows=5,
                            skiprows=9,
                            headers=None,
                            usecols='F,K,P,U,Z,AE,AJ,AO,AT,AY,BD,BI,BN,BS,BX,CC,CH,CM,CR,CW')
    inv=inv.iloc[[4],:]
    inv["Abr"]=estado
    inversion=inversion.append(inv)
inversion=inversion.melt(id_vars=["Abr"])
inversion["variable"]=inversion["variable"].str.replace("Total ", "")
inversion=inversion.rename(columns={"variable":"Año", "value":"inversion"})



###                 Monthly Investment
inversion=pd.DataFrame()
for estado in est["Abr"]:
    inv=pd.read_excel("Data/Flujosporentidadfederativa.xls", sheet_name =estado,
                            nrows=5,
                            skiprows=9,
                            headers=None)
    inv=inv.iloc[[0,4],:]
    inv["Abr"]=estado
    inversion=inversion.append(inv)

inv=inv.melt(id_vars=["Abr", "Unnamed: 0"]).rename(columns={"Unnamed: 0":"cat"})
inv.loc[inv.cat.isna(), "cat"]="Trim"

inv.pivot(index=["Abr", "variable"], columns="cat", values="value")

inv= pd.wide_to_long(inv, []])



inversion=inversion.melt(id_vars=["Abr"])
inversion["variable"]=inversion["variable"].str.replace("Total ", "")
inversion=inversion.rename(columns={"variable":"Año", "value":"inversion"})



#crime= pd.read_csv("R_Projects/Concentracion_Violencia/IDM_NM2019.csv", sep=";", encoding= "UTF-8")

crime=crime.rename(columns={"Modalidad":"MODALIDAD",
                         "Tipo de delito":"TIPO",
                         "Subtipo de delito":"SUBTIPO",
                         "Clave_Ent":"id_estado",
                         "Entidad":"Estado",
                         "Cve. Municipio":"id_municipio",
                         "Enero":"m_1",
                         "Febrero":"m_2",
                         "Marzo":"m_3",
                         "Abril":"m_4",
                         "Mayo":"m_5",
                         "Junio":"m_6",
                         "Julio":"m_7",
                         "Agosto":"m_8",
                         "Septiembre":"m_9",
                         "Octubre":"m_10",
                         "Noviembre":"m_11",
                         "Diciembre":"m_12"})


crime["Tipo"]=""
crime["Tipo"]=""
crime.loc[(crime.MODALIDAD=="Extorsión") & (crime.TIPO=="Extorsión") & (crime.SUBTIPO=="Extorsión"), "Tipo"]="Extorsión"
crime.loc[(crime.TIPO=="Homicidio") & (crime.SUBTIPO=="Homicidio culposo"), "Tipo"]="HomicidioCulp"
crime.loc[(crime.TIPO=="Homicidio") & (crime.SUBTIPO=="Homicidio doloso"), "Tipo"]="HomicidioDolo"
crime.loc[(crime.TIPO=="Feminicidio"), "Tipo"]="HomicidioDolo"
crime.loc[(crime.TIPO=="Lesiones") & (crime.SUBTIPO=="Lesiones culposas"), "Tipo"]="Lesiones"
crime.loc[(crime.TIPO=="Lesiones") & (crime.SUBTIPO=="Lesiones dolosas") , "Tipo"]="Lesiones"
crime.loc[(crime.SUBTIPO=="Secuestro"), "Tipo"]="Secuestro"
crime.loc[(crime.SUBTIPO=="Robo a casa habitación"), "Tipo"]="RoboCasa"
crime.loc[(crime.SUBTIPO=="Robo a negocio"), "Tipo"]="RoboNegocio"
crime.loc[(crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo de vehículo automotor"), "Tipo"]="RoboVehi"

crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo de autopartes"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transportista"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transeúnte en vía pública"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transeúnte en espacio abierto al público"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo en transporte individual"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo en transporte público colectivo"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a institución bancaria"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo de maquinaria"), "Tipo"]="RoboViol"
crime.loc[crime.MODALIDAD.str.contains("Con violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Otros robos"), "Tipo"]="RoboViol"

crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo de autopartes"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transportista"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transeúnte en vía pública"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a transeúnte en espacio abierto al público"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo en transporte individual"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo en transporte público colectivo"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo a institución bancaria"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Robo de maquinaria"), "Tipo"]="RoboSinViol"
crime.loc[crime.MODALIDAD.str.contains("Sin violencia") & (crime.TIPO=="Robo") & (crime.SUBTIPO=="Otros robos"), "Tipo"]="RoboSinViol"
           
crime["Tipo"].value_counts(sort=True)
           
crime["Tipo"].value_counts(sort=True)
crime=crime.loc[crime.Tipo!=""]
crime.columns.values

crime1=crime[["Año", "id_estado", "Estado", "id_municipio", "Municipio", "Tipo", "m_1", "m_2", "m_3", "m_4", "m_5",
              "m_6", "m_7", "m_8", "m_9", "m_10", "m_11", "m_12"]]
crime1=crime.melt(id_vars=["Año", "id_estado", "Estado", "id_municipio", "Municipio", "Tipo"])
crime1["Tipo"].value_counts(sort=True)