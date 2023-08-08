import json
import click
import datetime
import psycopg2
from mobilitydb.psycopg import register
from pymeos import *
from pygeoapi.provider.postgresql import PostgreSQLProvider
from pygeofilter.parsers.ecql import parse

class ProcessMobilityData:
    host = '127.0.0.1'
    port = 5432
    db = 'mobilitydb'
    user = 'docker'
    password = 'docker'
    connection = None

    def __init__(self, datasource=None):
        self.connection = None

        if datasource is not None:
            self.host = datasource['host']
            self.port = int(datasource['port'])
            self.db = datasource['dbname']
            self.user = datasource['user']
            self.password = datasource['password']
        
    def connect(self):        
        # Set the connection parameters to PostgreSQL
        self.connection = psycopg2.connect(host=self.host, database=self.db, user=self.user, password=self.password, port=self.port)
        self.connection.autocommit = True
        
        # Register MobilityDB data types
        register(self.connection)


    def disconnect(self):  
        # Close the connection      
        if self.connection:
            self.connection.close()

    def getCollectionsList(self):  
        cursor = self.connection.cursor()

        select_query = "SELECT collection_id FROM collection"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def getCollections(self):  
        cursor = self.connection.cursor()        
        select_query = "select collection.collection_id, collection.collection_property, extentTGeometry, extentTPropertiesValue "
        select_query +="from (select collection.collection_id, collection.collection_property, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="group by collection.collection_id, collection.collection_property) collection "
        select_query +="left outer join (select collection.collection_id, collection.collection_property, extent(tpropertiesvalue.pvalue_float) as extentTPropertiesValue "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tproperties on mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="left outer join tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name  "
        select_query +="group by collection.collection_id, collection.collection_property) tproperties ON collection.collection_id = tproperties.collection_id "
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows 
    
    def getCollection(self, collection_id):   
        cursor = self.connection.cursor()        
        select_query = "select collection.collection_id, collection.collection_property, extentTGeometry, extentTPropertiesValue "
        select_query +="from (select collection.collection_id, collection.collection_property, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where collection.collection_id ='{0}' ".format(collection_id)
        select_query +="group by collection.collection_id, collection.collection_property) collection "
        select_query +="left outer join (select collection.collection_id, collection.collection_property, extent(tpropertiesvalue.pvalue_float) as extentTPropertiesValue "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tproperties on mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="left outer join tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name  "
        select_query +="where collection.collection_id ='{0}' ".format(collection_id)
        select_query +="group by collection.collection_id, collection.collection_property) tproperties ON collection.collection_id = tproperties.collection_id "
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows   

    def getFeaturesList(self):  
        cursor = self.connection.cursor()

        select_query = "SELECT collection_id, mfeature_id FROM mfeature"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows    
    
    def gettPropertiesNameList(self):  
        cursor = self.connection.cursor()

        select_query = "SELECT collection_id, mfeature_id, tproperties_name FROM tproperties"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def getFeatures(self, collection_id, bbox='', datetime='', limit=10, offset=0): 
        cursor = self.connection.cursor()
        select_query = "select mfeature.collection_id, mfeature.mfeature_id, st_asgeojson(mfeature.mf_geometry) as mf_geometry, mfeature.mf_property, extentTGeometry, extentTPropertiesValue "
        select_query +="from (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from mfeature "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where mfeature.collection_id ='{0}' ".format(collection_id)
        select_query +="group by mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property) mfeature "
        select_query +="left outer join (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, extent(tpropertiesvalue.pvalue_float) as extentTPropertiesValue "
        select_query +="from mfeature "
        select_query +="left outer join tproperties on mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="left outer join tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name "
        select_query +="where mfeature.collection_id ='{0}' ".format(collection_id)
        select_query +="group by  mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property) tproperties ON mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="where 1=1 "

        if bbox != ''and bbox is not None:  
            s_bbox = ','.join(str(x) for x in bbox)          
            if len(bbox) == 4:
                select_query += " and box2d(stbox(" + s_bbox + ")) &&& box2d(extentTGeometry) "                
            elif len(bbox) == 6:
                select_query += " and box3d(stbox_z(" + s_bbox + ")) &&& box3d(extentTGeometry) "          
        if datetime != '' and datetime is not None:
            select_query += " and period(extentTPropertiesValue) && period('[" + datetime + "]')" 
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += " LIMIT " + str(limit) + " OFFSET " + str(offset)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows)
        return rows, numberMatched, numberReturned    

    def getFeature(self, collection_id, mfeature_id): 
        cursor = self.connection.cursor()        
        select_query = "select mfeature.collection_id, mfeature.mfeature_id, st_asgeojson(mfeature.mf_geometry) as mf_geometry, mfeature.mf_property, extentTGeometry, extentTPropertiesValue "
        select_query +="from (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from mfeature "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where mfeature.collection_id ='{0}' AND mfeature.mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +="group by mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property) mfeature "
        select_query +="left outer join (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, extent(tpropertiesvalue.pvalue_float) as extentTPropertiesValue "
        select_query +="from mfeature "
        select_query +="left outer join tproperties on mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="left outer join tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name "
        select_query +="where mfeature.collection_id ='{0}' AND mfeature.mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +="group by  mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property) tproperties ON mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "        
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def getTemporalGeometries(self, collection_id, mfeature_id, bbox='', leaf='', datetime='', limit=10, offset=0):  
        cursor = self.connection.cursor()
        tgeometry_property = 'tgeometry_property'
        if leaf != '' and leaf is not None:
            tgeometry_property = "attimestampset(tgeometry_property, '{" + leaf + "}')"

        select_query = "SELECT collection_id,mfeature_id,tgeometry_id, " + tgeometry_property + " FROM tgeometry WHERE collection_id ='{0}' AND mfeature_id='{1}' ".format(collection_id, mfeature_id)
        if bbox != ''and bbox is not None:  
            s_bbox = ','.join(str(x) for x in bbox)          
            if len(bbox) == 4:
                select_query += " and box2d(stbox(" + s_bbox + ")) &&& box2d(stbox(tgeometry_property))"                
            elif len(bbox) == 6:
                select_query += " and box3d(stbox_z(" + s_bbox + ")) &&& box3d(stbox(tgeometry_property))" 
        if datetime != ''and datetime is not None:
            select_query += " and atperiod(tgeometry_property, '[" + datetime + "]') is not null "
        if leaf != '' and leaf is not None:
            select_query += " and attimestampset(tgeometry_property, '{" + leaf + "}') is not null "
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += " LIMIT " + str(limit) + " OFFSET " + str(offset)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows);
        return rows, numberMatched, numberReturned

    def getTemporalProperties(self, collection_id, mfeature_id, datetime='', limit=10, offset=0):  
        cursor = self.connection.cursor()
        select_query = "select distinct tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty "
        select_query +="from tproperties "
        select_query +="left outer join tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id "
        select_query +="and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name "
        select_query +="WHERE tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' ".format(collection_id, mfeature_id)
        if datetime != ''and datetime is not None:
            select_query += " and (atperiod(pvalue_float, '[" + datetime + "]') is not null or atperiod(pvalue_text, '[" + datetime + "]') is not null)"        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += " LIMIT " + str(limit) + " OFFSET " + str(offset)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows);
        return rows, numberMatched, numberReturned
    
    def getTemporalPropertiesValue(self, collection_id, mfeature_id, tProperty_name, leaf='', datetime='', limit=10, offset=0):  
        cursor = self.connection.cursor()
        pvalue = 'pvalue_float, pvalue_text'
        if leaf != '' and leaf is not None:
            pvalue = "attimestampset(pvalue_float, '{" + leaf + "}'), attimestampset(pvalue_text, '{" + leaf + "}')"

        select_query = "SELECT collection_id,mfeature_id,tproperties_name,pvalue_id, " + pvalue + " FROM tpropertiesvalue WHERE collection_id ='{0}' AND mfeature_id='{1}' AND tproperties_name='{2}' ".format(collection_id, mfeature_id, tProperty_name)
        if datetime != ''and datetime is not None:
            select_query += " and (atperiod(pvalue_float, '[" + datetime + "]') is not null or atperiod(pvalue_text, '[" + datetime + "]') is not null)"
        if leaf != '' and leaf is not None:
            select_query += " and (attimestampset(pvalue_float, '{" + leaf + "}') is not null or attimestampset(pvalue_text, '{" + leaf + "}') is not null)"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += " LIMIT " + str(limit) + " OFFSET " + str(offset)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows);
        return rows, numberMatched, numberReturned

    def postCollection(self, collection_property):     
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO collection(collection_property) VALUES ('{0}') RETURNING collection_id".format(json.dumps(collection_property)))

        collection_id = cursor.fetchone()[0]
        return collection_id
    

    def postMovingFeature(self, collection_id, movingfeature):  
        cursor = self.connection.cursor()
        g_movingfeature = dict(movingfeature)
        temporalGeometries = g_movingfeature.pop("temporalGeometry", None) 
        temporalProperties = g_movingfeature.pop("temporalProperties", None)

        if 'geometry' in g_movingfeature:          
            geometry = g_movingfeature.pop("geometry", None)
            cursor.execute("INSERT INTO mfeature(collection_id, mf_geometry, mf_property) VALUES ('{0}', ST_GeomFromGeoJSON('{1}'), '{2}') RETURNING mfeature_id".format(collection_id, json.dumps(geometry), json.dumps(g_movingfeature)))
        else:
            cursor.execute("INSERT INTO mfeature(collection_id, mf_property) VALUES ('{0}', '{1}') RETURNING mfeature_id".format(collection_id, json.dumps(g_movingfeature)))
        mfeature_id = cursor.fetchone()[0]

        if temporalGeometries is not None:
            temporalGeometries = [temporalGeometries] if not isinstance(temporalGeometries, list) else temporalGeometries
            for temporalGeometry in temporalGeometries:
                # for _ in range(10000):
                    self.postTemporalGeometry(collection_id, mfeature_id, temporalGeometry)

        if temporalProperties is not None:
            temporalProperties = [temporalProperties] if not isinstance(temporalProperties, list) else temporalProperties
            for temporalProperty in temporalProperties:
                self.postTemporalProperties(collection_id, mfeature_id, temporalProperty)

        return mfeature_id

    def postTemporalGeometry(self, collection_id, mfeature_id, temporalGeometry):        
        cursor = self.connection.cursor()

        pymeos_initialize()
        temporalGeometry = self.convertTemporalGeometryToNewVersion(temporalGeometry)
        value = Temporal.from_mfjson(json.dumps(temporalGeometry))   

        cursor.execute("INSERT INTO tgeometry(collection_id, mfeature_id, tgeometry_property) VALUES ('{0}', '{1}', '{2}') RETURNING tgeometry_id".format(collection_id, mfeature_id, str(value)))
        tgeometry_id = cursor.fetchone()[0]

        return tgeometry_id

    def postTemporalProperties(self, collection_id, mfeature_id, temporalProperty):         
        g_temporalProperty = dict(temporalProperty)
        if 'datetimes' in g_temporalProperty:
            datetimes = g_temporalProperty.pop("datetimes", None) 
        for tproperties_name in g_temporalProperty:
            if 'values' in g_temporalProperty[tproperties_name] and 'interpolation' in g_temporalProperty[tproperties_name]:
                values = g_temporalProperty[tproperties_name].pop("values", None)
                interpolation = g_temporalProperty[tproperties_name].pop("interpolation", None)                
                temporalValue = self.createTemporalPropertyValue(datetimes, values, interpolation)

            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO tproperties(collection_id, mfeature_id, tproperties_name, tproperty) VALUES ('{0}', '{1}', '{2}', '{3}')".format(collection_id, mfeature_id, tproperties_name, json.dumps(temporalProperty[tproperties_name])))

            if temporalValue is not None:
                self.postTemporalValue(collection_id, mfeature_id, tproperties_name, temporalValue)

        return tproperties_name

    def postTemporalValue(self, collection_id, mfeature_id, tproperties_name, temporalValue): 
        cursor = self.connection.cursor()
        dataType = temporalValue["type"]
        pymeos_initialize()
        value = Temporal.from_mfjson(json.dumps(temporalValue))   
        
        if dataType == 'MovingFloat':
            cursor.execute("INSERT INTO tpropertiesvalue(collection_id, mfeature_id, tproperties_name, pValue_float) VALUES ('{0}', '{1}', '{2}', '{3}') RETURNING pvalue_id".format(collection_id, mfeature_id, tproperties_name, str(value)))
        else:
            cursor.execute("INSERT INTO tpropertiesvalue(collection_id, mfeature_id, tproperties_name, pValue_text) VALUES ('{0}', '{1}', '{2}', '{3}') RETURNING pvalue_id".format(collection_id, mfeature_id, tproperties_name, str(value)))
        pValue_id = cursor.fetchone()[0]

        return pValue_id

    def putCollection(self, collection_id, collection_property):   
        cursor = self.connection.cursor()
        cursor.execute("UPDATE collection set collection_property = '{0}' WHERE collection_id = '{1}'".format(json.dumps(collection_property), collection_id))

    def deleteCollection(self, restriction):  
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM tgeometry WHERE 1=1 {0}".format(restriction)) 
        cursor.execute("DELETE FROM tproperties WHERE 1=1 {0}".format(restriction)) 
        cursor.execute("DELETE FROM mfeature WHERE 1=1 {0}".format(restriction)) 
        cursor.execute("DELETE FROM collection WHERE 1=1 {0}".format(restriction)) 

    def deleteMovingFeature(self, restriction):     
        cursor = self.connection.cursor()  
        cursor.execute("DELETE FROM tproperties WHERE 1=1 {0}".format(restriction)) 
        cursor.execute("DELETE FROM tgeometry WHERE 1=1 {0}".format(restriction))  
        cursor.execute("DELETE FROM mfeature WHERE 1=1 {0}".format(restriction))  
    
    def deleteTemporalGeometry(self, restriction):   
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM tgeometry WHERE 1=1 {0}".format(restriction))  

    def deleteTemporalProperties(self, restriction):   
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM tpropertiesvalue WHERE 1=1 {0}".format(restriction)) 
        cursor.execute("DELETE FROM tproperties WHERE 1=1 {0}".format(restriction))         

    def convertTemporalGeometryToNewVersion(self, temporalGeometry):
        if 'interpolation' in temporalGeometry:
            if temporalGeometry['interpolation'] == 'Step':
                temporalGeometry['interpolation'] = 'Stepwise'
            temporalGeometry['interpolations'] = [temporalGeometry['interpolation']]
            del temporalGeometry['interpolation']

        if 'type' in temporalGeometry:
            if temporalGeometry['type'] == 'MovingPoint':
                temporalGeometry['type'] = 'MovingGeomPoint'

        if 'datetimes' in temporalGeometry:
            datetimes = temporalGeometry['datetimes']
            for i in range( len(datetimes)):
                datetimes[i] = datetimes[i].replace('Z','')
            temporalGeometry['datetimes'] = datetimes

        if 'lower_inc' not in temporalGeometry:
            temporalGeometry['lower_inc'] = True
        if 'upper_inc' not in temporalGeometry:
            temporalGeometry['upper_inc'] = True
        return temporalGeometry

    def convertTemporalGeometryToOldVersion(self, temporalGeometry):
        if 'interpolations' in temporalGeometry:            
            temporalGeometry['interpolation'] = temporalGeometry['interpolations'][0]            
            if temporalGeometry['interpolation'] == 'Stepwise':
                temporalGeometry['interpolation'] = 'Step'
            del temporalGeometry['interpolations']
            
        if temporalGeometry['type'] == 'MovingGeomPoint':
            temporalGeometry['type'] = 'MovingPoint'

        if 'datetimes' in temporalGeometry:
            datetimes = temporalGeometry['datetimes']            
            for i in range( len(datetimes)):
                datetimes[i] = datetimes[i].split('+')[0] + 'Z'
            temporalGeometry['datetimes'] = datetimes

        if 'lower_inc' in temporalGeometry:
            del temporalGeometry['lower_inc']
        if 'upper_inc' in temporalGeometry:
            del temporalGeometry['upper_inc'] 
        
        return temporalGeometry

    def createTemporalPropertyValue(self, datetimes, values, interpolation):
        if interpolation == 'Step':
            interpolation = 'Stepwise'

        for i in range( len(datetimes)):
            if isinstance(datetimes[i], int):
                datetimes[i] = datetime.datetime.fromtimestamp(datetimes[i] / 1e3).strftime("%Y/%m/%dT%H:%M:%S.%f")
            else:
                datetimes[i] = datetimes[i].replace('Z','')

        if all([isinstance(item, int) or isinstance(item, float) for item in values]):
            dataType = 'MovingFloat'
        else:
            dataType = 'MovingText'
        temporalValue = {
            "type": dataType,        
            "lower_inc": True,
            "upper_inc": True,
            'datetimes': datetimes,
            'values': values,
            'interpolations': [interpolation]
        }      
        return temporalValue
    
    def convertTemporalPropertyValueToBaseVersion(self, temporalPropertyValue):
        if 'interpolations' in temporalPropertyValue:            
            temporalPropertyValue['interpolation'] = temporalPropertyValue['interpolations'][0]
            del temporalPropertyValue['interpolations']

        if 'type' in temporalPropertyValue:
            del temporalPropertyValue['type']

        if 'datetimes' in temporalPropertyValue:
            datetimes = temporalPropertyValue['datetimes']            
            for i in range( len(datetimes)):
                datetimes[i] = datetimes[i].split('+')[0] + 'Z'
            temporalPropertyValue['datetimes'] = datetimes

        if 'lower_inc' in temporalPropertyValue:
            del temporalPropertyValue['lower_inc']
        if 'upper_inc' in temporalPropertyValue:
            del temporalPropertyValue['upper_inc'] 
        return temporalPropertyValue