from pyspark import SparkContext
import json
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


def media_travel_time_age_range(df):
    df.groupBy("ageRange").mean("travel_time").show()
    
def media_total_travel(df):
    df_aux = df.filter(df["ageRange"]>0) # limpiamos los usuarios de edad 0, pues sus edades son desconocidas
    num = df_aux.count()
    total = df_aux.rdd.map(lambda x: x["travel_time"]).sum()
    media = (total/num)/60
    print("De media se recorren ", media, "minutos en cada viaje")
    
def usuarios_por_range_edad(df): #número de usuarios por rango de edad+
    print("Número de usuarios por rango de edad:\n")
    df_aux = df.filter(df["ageRange"]>0)
    df_aux.groupBy("ageRange").count().show()
    
    
def estacion_mas_transitada(df):
    print("La estación donde más se inician los viajes: \n")
    df_aux = df.groupBy("idunplug_station").count()
    maximo = df_aux.rdd.map(lambda x : x["count"]).max()
    df_aux.filter(df_aux["count"] == maximo).show()#muestra la estación más transitada para coger la bici y cuantas veces se ha transitado en estos 6 meses

def estacion_menos_transitada(df):
    print("La estación donde menos viajes se inician: \n")
    df_aux = df.groupBy("idunplug_station").count()
    minimo = df_aux.rdd.map(lambda x : x["count"]).min()
    df_aux.filter(df_aux["count"] == minimo).show()
    
def estacion_destino_menos_transitada(df):
    print("La estación de destino menos habitual:\n")
    df_aux = df.groupBy("idunplug_station").count()
    minimo = df_aux.rdd.map(lambda x : x["count"]).min()
    df_aux.filter(df_aux["count"] == minimo).show()
    
def estacion_destino_mas_transitada(df):
    print("La estación de destino más habitual: \n")
    df_aux = df.groupBy("idplug_station").count()
    maximo = df_aux.rdd.map(lambda x : x["count"]).max()
    df_aux.filter(df_aux["count"] == maximo).show()
    
def tipo_de_usuario_mas_comun(df):
    print("Tipo de usuario más usual: \n")
    df1 = df.filter(df["user_type"]>0)
    df_aux = df1.groupBy("user_type").count()
    maximo = df_aux.rdd.map(lambda x : x["count"]).max()
    df_aux.filter(df_aux["count"] == maximo).show() #Muestra el tipo de usuario más común y cuantos visajes han hecho ese tipo de usuarios

def tipo_de_usuario_menos_comun(df):
    print("Tipo de usuario menos usual: \n")
    df1 = df.filter(df["user_type"]>0)
    df_aux = df1.groupBy("user_type").count()
    minimo = df_aux.rdd.map(lambda x : x["count"]).min()
    df_aux.filter(df_aux["count"] == minimo).show()
    
def grafico_edad_travel_time(df):
    tupla = {}
    for x,y in df.select("ageRange","travel_time").collect():
        tupla[x] = y
    fig, ax = plt.subplots()
    ax.bar(tupla.keys(),tupla.values())
    fig.show()   
    
def main():
    print("Vamos a realizar un etstudio de las características más importantes del programa bicimad durante los meses de enero a junio de 2021. \n Nos parece interesante este estudio ya que es el año post pandemia y es interesante observar cómo pueden variar distintos factores en relación a las medidas, restricciones y distancia social impuestas por el gobierno.")
    with SparkContext() as sc:
        spark = SparkSession.builder.getOrCreate()
        sc.setLogLevel("ERROR")
        #Limpiamos los datos, con las variables que nos interesan
        schema = StructType()\
            .add("travel_time", IntegerType(), False)\
            .add("idunplug_station", IntegerType(), False)\
            .add("idplug_station", IntegerType(), False)\
            .add("ageRange",IntegerType(),False)\
            .add("unplug_hourTime", StringType(),False)\
            .add("user_type",IntegerType(),False)
        
        dfo1 = spark.read.json("202101_movements.json",schema=schema)
        dfo2 = spark.read.json("202102_movements.json",schema=schema)
        dfo3 = spark.read.json("202103_movements.json",schema=schema)
        dfo4 = spark.read.json("202104_movements.json",schema=schema)
        dfo5 = spark.read.json("202105_movements.json",schema=schema)
        dfo6 = spark.read.json("202106_movements.json",schema=schema)
        
        df = dfo1.union(dfo2)
        df = df.union(dfo3)
        df = df.union(dfo4)
        df = df.union(dfo5)
        df = df.union(dfo6)
        
        df.show()
        
        media_travel_time_age_range(df)
        
        media_total_travel(df)
        
        usuarios_por_range_edad(df)
        
        estacion_mas_transitada(df)
        
        estacion_menos_transitada(df)
        
        estacion_destino_mas_transitada(df)
        
        estacion_destino_menos_transitada(df)
        
        tipo_de_usuario_mas_comun(df)
        
        tipo_de_usuario_menos_comun(df)
        
        grafico_edad_travel_time(df)

if __name__ == '__main__':
    main()
