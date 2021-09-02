from pandas.io.sql import DatabaseError
from SqlUtil import *

def subEjecutor(array,currentIndex,proceso):
  if(currentIndex< len(array.index)):
    dt = array.iloc[currentIndex]
    print("Ejecutando %s %s" %(proceso.loc['NombreProceso'],dt.loc['fileOwner']))
    print(proceso.loc['Query']%dt.loc['fileOwner'])
    sqlStore(proceso.loc['TableName'],proceso.loc['Query']%dt.loc['fileOwner'])
    subEjecutor(array, currentIndex + 1, proceso)

def ejecutor(array,currentIndex):
  if(currentIndex< len(array.index)):
    proceso = array.iloc[currentIndex]
    print("Ejecutando %s" %proceso.loc['NombreProceso'])
    data = sqlExec(proceso.loc['TableName'],proceso.loc['Query'])
    if(proceso.loc['isSubProcess'] != 1):
      subProceso = array.iloc[currentIndex + 1]
      subEjecutor(data,0,subProceso)
      ejecutor(array,currentIndex + 2)
    else:
      ejecutor(array,currentIndex + 1)

procesos = sqlExec('STRATEGIO_BRUTO_MOLITALIA',"select * from diccionario_insercion_datos where NombreProceso in ('Listar Dts','Cargar datos Dts')")

ejecutor(procesos,0)

 