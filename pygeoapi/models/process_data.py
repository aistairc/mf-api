import json
import click
import datetime
import psycopg2
from functools import partial
from dateutil.parser import parse as dateparse
import pytz
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
        select_query = "select collection.collection_id, collection.collection_property, extentLifespan, extentTGeometry "
        select_query +="from (select collection.collection_id, collection.collection_property, extent(mfeature.lifespan) as extentLifespan, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="group by collection.collection_id, collection.collection_property) collection "
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows 
    
    def getCollection(self, collection_id):   
        cursor = self.connection.cursor()        
        select_query = "select collection.collection_id, collection.collection_property, extentLifespan, extentTGeometry "
        select_query +="from (select collection.collection_id, collection.collection_property, extent(mfeature.lifespan) as extentLifespan, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from collection "
        select_query +="left outer join mfeature on collection.collection_id = mfeature.collection_id "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where collection.collection_id ='{0}' ".format(collection_id)
        select_query +="group by collection.collection_id, collection.collection_property) collection "
        
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

    def getFeatures(self, collection_id, bbox='', datetime='', limit=10, offset=0, subTrajectory=False): 
        cursor = self.connection.cursor()

        bbox_restriction = ""
        if bbox != ''and bbox is not None:  
            s_bbox = ','.join(str(x) for x in bbox)          
            if len(bbox) == 4:
                bbox_restriction = " and box2d(stbox(" + s_bbox + ")) &&& box2d(extentTGeometry) "                
            elif len(bbox) == 6:
                bbox_restriction = " and box3d(stbox_z(" + s_bbox + ")) &&& box3d(extentTGeometry) "

        datetime_restriction = ""
        if datetime != '' and datetime is not None:
            if subTrajectory == False or subTrajectory == "false": 
                datetime_restriction = " and ((lifespan && period('[" + datetime + "]')) or (period(extentTPropertiesValueFloat) && period('[" + datetime + "]')) or (period(extentTPropertiesValueText) && period('[" + datetime + "]')) or (period(extentTGeometry) && period('[" + datetime + "]')))"

        limit_restriction = " LIMIT " + str(limit) + " OFFSET " + str(offset)

        select_query = "select mfeature.collection_id, mfeature.mfeature_id, st_asgeojson(mfeature.mf_geometry) as mf_geometry, mfeature.mf_property, mfeature.lifespan, extentTGeometry, extentTPropertiesValueFloat, extentTPropertiesValueText "
        select_query +="from (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from mfeature "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where mfeature.collection_id ='{0}' ".format(collection_id)
        select_query +="group by mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan) mfeature "
        select_query +="left outer join (select mfeature.collection_id, mfeature.mfeature_id, extent(tproperties.pvalue_float) as extentTPropertiesValueFloat, extent(tproperties.pvalue_text) as extentTPropertiesValueText "
        select_query +="from mfeature "
        select_query +="left outer join tproperties on mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="where mfeature.collection_id ='{0}' ".format(collection_id)
        select_query +="group by  mfeature.collection_id, mfeature.mfeature_id) tproperties ON mfeature.collection_id = tproperties.collection_id and mfeature.mfeature_id = tproperties.mfeature_id "
        select_query +="where 1=1 {0} {1}".format(bbox_restriction, datetime_restriction)

        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += limit_restriction
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows)

        if subTrajectory == True or subTrajectory == "true":  
            subTrajectory_field = "atperiod(tgeometry.tgeometry_property, '[" + datetime + "]')" 
            select_geometry_query = "select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan, extentTGeometry, tgeometry.tgeometry_id, tgeometry_property "
            select_geometry_query +="from (select mfeature.collection_id, mfeature.mfeature_id, st_asgeojson(mfeature.mf_geometry) as mf_geometry, mfeature.mf_property, mfeature.lifespan, extentTGeometry "
            select_geometry_query +="from (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan, extent(tgeometry.tgeometry_property) as extentTGeometry "
            select_geometry_query +="from mfeature "
            select_geometry_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
            select_geometry_query +="where mfeature.collection_id ='{0}' ".format(collection_id)
            select_geometry_query +="group by mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan) mfeature "
            select_geometry_query +="where 1=1 {0} {1}) mfeature ".format(bbox_restriction, limit_restriction)
            select_geometry_query +="left outer join (select tgeometry.collection_id, tgeometry.mfeature_id, tgeometry.tgeometry_id, {0} as tgeometry_property ".format(subTrajectory_field)
            select_geometry_query +="from tgeometry "
            select_geometry_query +="where tgeometry.collection_id ='{0}' and {1} is not null".format(collection_id, subTrajectory_field)
            select_geometry_query +=") tgeometry ON mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
            select_geometry_query +="where 1=1 "

            cursor.execute(select_geometry_query)
            rows = cursor.fetchall()
        return rows, numberMatched, numberReturned    

    def getFeature(self, collection_id, mfeature_id): 
        cursor = self.connection.cursor()        
        select_query = "select mfeature.collection_id, mfeature.mfeature_id, st_asgeojson(mfeature.mf_geometry) as mf_geometry, mfeature.mf_property, mfeature.lifespan, extentTGeometry "
        select_query +="from (select mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan, extent(tgeometry.tgeometry_property) as extentTGeometry "
        select_query +="from mfeature "
        select_query +="left outer join tgeometry on mfeature.collection_id = tgeometry.collection_id and mfeature.mfeature_id = tgeometry.mfeature_id "
        select_query +="where mfeature.collection_id ='{0}' AND mfeature.mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +="group by mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property, mfeature.lifespan) mfeature "
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def getTemporalGeometries(self, collection_id, mfeature_id, bbox='', leaf='', datetime='', limit=10, offset=0, subTrajectory=False):  
        cursor = self.connection.cursor()
        tgeometry_property = 'null'

        bbox_restriction = ""
        if bbox != ''and bbox is not None:  
            s_bbox = ','.join(str(x) for x in bbox)          
            if len(bbox) == 4:
                bbox_restriction = " and box2d(stbox(" + s_bbox + ")) &&& box2d(stbox(tgeometry_property))"                
            elif len(bbox) == 6:
                bbox_restriction = " and box3d(stbox_z(" + s_bbox + ")) &&& box3d(stbox(tgeometry_property))" 
        datetime_restriction = ""
        if datetime != ''and datetime is not None:
            datetime_restriction = " and atperiod(tgeometry_property, '[" + datetime + "]') is not null "    

        if leaf != '' and leaf is not None:
            tgeometry_property = "attimestampset(tgeometry_property, '{" + leaf + "}')"
        elif subTrajectory == True or subTrajectory == "true": 
            tgeometry_property = "atperiod(tgeometry_property, '[" + datetime + "]')"

        select_query = "SELECT collection_id,mfeature_id,tgeometry_id, tgeometry_property, {0} ".format(tgeometry_property) 
        select_query +="FROM tgeometry WHERE collection_id ='{0}' AND mfeature_id='{1}' {2} {3}".format(collection_id, mfeature_id, bbox_restriction, datetime_restriction)

        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += " LIMIT " + str(limit) + " OFFSET " + str(offset)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows);
        return rows, numberMatched, numberReturned

    def getTemporalProperties(self, collection_id, mfeature_id, datetime='', limit=10, offset=0, subTemporalValue=False):  
        cursor = self.connection.cursor()

        datetime_restriction = ''
        if datetime != '' and datetime is not None:
            if subTemporalValue == False or subTemporalValue == "false": 
                datetime_restriction = " and (atperiod(pvalue_float, '[" + datetime + "]') is not null or atperiod(pvalue_text, '[" + datetime + "]') is not null)"

        limit_restriction = " LIMIT " + str(limit) + " OFFSET " + str(offset)
        select_query = "select distinct on (tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name) "
        select_query +="tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty "
        select_query +="from tproperties "
        select_query +="WHERE tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' {2}".format(collection_id, mfeature_id, datetime_restriction)
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberMatched = len(rows)

        select_query += limit_restriction
        cursor.execute(select_query)
        rows = cursor.fetchall()
        numberReturned = len(rows);

        if subTemporalValue == True or subTemporalValue == "true":
            subTemporalValue_float_field = "atperiod(tproperties.pvalue_float, '[" + datetime + "]')" 
            subTemporalValue_text_field = "atperiod(tproperties.pvalue_text, '[" + datetime + "]')" 

            select_temporalValue_query = "select tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty, datetime_group, pvalue_float, pvalue_text "
            select_temporalValue_query +="from (select distinct on (tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name) "
            select_temporalValue_query +="tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty "
            select_temporalValue_query +="from tproperties where tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' {2} {3}) tproperties ".format(collection_id, mfeature_id, datetime_restriction, limit_restriction)
            select_temporalValue_query +="left outer join (select tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.datetime_group, "
            select_temporalValue_query +="{0} as pvalue_float, {1} as pvalue_text ".format(subTemporalValue_float_field, subTemporalValue_text_field)
            select_temporalValue_query +="from tproperties where tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' ".format(collection_id, mfeature_id)
            select_temporalValue_query +="and ({0} is not null or {1} is not null)".format(subTemporalValue_float_field, subTemporalValue_text_field)
            select_temporalValue_query +=") tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id "
            select_temporalValue_query +="and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name "
            select_temporalValue_query +="where 1=1 order by datetime_group"

            
            click.echo(select_temporalValue_query)
            cursor.execute(select_temporalValue_query)
            rows = cursor.fetchall()
        return rows, numberMatched, numberReturned
    
    def getTemporalPropertiesValue(self, collection_id, mfeature_id, tProperty_name, leaf='', datetime='', subTemporalValue=False):  
        cursor = self.connection.cursor()

        datetime_restriction = ""
        if datetime != ''and datetime is not None:
            datetime_restriction = " and (atperiod(tproperties.pvalue_float, '[" + datetime + "]') is not null or atperiod(tproperties.pvalue_text, '[" + datetime + "]') is not null) "    

        float_field = 'pvalue_float'   
        text_field = 'pvalue_text'
        if leaf != '' and leaf is not None:
            float_field = "attimestampset(tproperties.pvalue_float, '{" + leaf + "}')" 
            text_field = "attimestampset(tproperties.pvalue_text, '{" + leaf + "}')" 
        elif subTemporalValue == True or subTemporalValue == "true": 
            float_field = "atperiod(tproperties.pvalue_float, '[" + datetime + "]')" 
            text_field = "atperiod(tproperties.pvalue_text, '[" + datetime + "]')" 

        select_query = "select tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty, datetime_group, pvalue_float, pvalue_text "
        select_query +="from (select distinct on (tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name) "
        select_query +="tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.tproperty "
        select_query +="from tproperties where tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' AND tproperties.tproperties_name='{2}') tproperties ".format(collection_id, mfeature_id, tProperty_name)
        select_query +="left outer join (select tproperties.collection_id, tproperties.mfeature_id, tproperties.tproperties_name, tproperties.datetime_group, "
        select_query +="{0} as pvalue_float, {1} as pvalue_text ".format(float_field, text_field)
        select_query +="from tproperties where tproperties.collection_id ='{0}' AND tproperties.mfeature_id='{1}' AND tproperties.tproperties_name='{2}' {3}".format(collection_id, mfeature_id, tProperty_name, datetime_restriction)
        select_query +=") tpropertiesvalue on tproperties.collection_id = tpropertiesvalue.collection_id "
        select_query +="and tproperties.mfeature_id = tpropertiesvalue.mfeature_id and tproperties.tproperties_name = tpropertiesvalue.tproperties_name "
        select_query +="where 1=1 order by datetime_group"
        
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def postCollection(self, collection_property):     
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO collection(collection_property) VALUES ('{0}') RETURNING collection_id".format(json.dumps(collection_property)))

        collection_id = cursor.fetchone()[0]
        return collection_id
    

    def postMovingFeature(self, collection_id, movingfeature):  
        cursor = self.connection.cursor()
        g_movingfeature = dict(movingfeature)
        lifespan = g_movingfeature.pop("time", None) 
        if lifespan is not None:
            lifespan = "'[" + self.validate_lifespan(lifespan) + "]'"
        else:
            lifespan = "NULL"
        temporalGeometries = g_movingfeature.pop("temporalGeometry", None) 
        temporalProperties = g_movingfeature.pop("temporalProperties", None)

        if 'geometry' in g_movingfeature:          
            geometry = g_movingfeature.pop("geometry", None)
            cursor.execute("INSERT INTO mfeature(collection_id, mf_geometry, mf_property, lifespan) VALUES ('{0}', ST_GeomFromGeoJSON('{1}'), '{2}', {3}) RETURNING mfeature_id".format(collection_id, json.dumps(geometry), json.dumps(g_movingfeature), lifespan))
        else:
            cursor.execute("INSERT INTO mfeature(collection_id, mf_property, lifespan) VALUES ('{0}', '{1}', {2}) RETURNING mfeature_id".format(collection_id, json.dumps(g_movingfeature), lifespan))
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

        #pymeos of python
        pymeos_initialize()
        temporalGeometry = self.convertTemporalGeometryToNewVersion(temporalGeometry)
        value = Temporal.from_mfjson(json.dumps(temporalGeometry))   

        cursor.execute("INSERT INTO tgeometry(collection_id, mfeature_id, tgeometry_property) VALUES ('{0}', '{1}', '{2}') RETURNING tgeometry_id".format(collection_id, mfeature_id, str(value)))
        tgeometry_id = cursor.fetchone()[0]

        return tgeometry_id

    def postTemporalProperties(self, collection_id, mfeature_id, temporalProperty):         
        g_temporalProperty = dict(temporalProperty)
        datetimes = []
        if 'datetimes' in g_temporalProperty:
            datetimes = g_temporalProperty.pop("datetimes", None) 
        datetime_group = self.getTemporalPropertiesGroup(collection_id, mfeature_id, datetimes)
        for tproperties_name in g_temporalProperty:
            cursor = self.connection.cursor()
            if 'values' in g_temporalProperty[tproperties_name] and 'interpolation' in g_temporalProperty[tproperties_name]:
                values = g_temporalProperty[tproperties_name].pop("values", None)
                interpolation = g_temporalProperty[tproperties_name].pop("interpolation", None)                
                
                temporalValue = self.createTemporalPropertyValue(datetimes, values, interpolation)
                
                dataType = temporalValue["type"]
                pvalue_Column = "pValue_text"
                if dataType == 'MovingFloat':
                    pvalue_Column = "pValue_float"

                pymeos_initialize()
                value = Temporal.from_mfjson(json.dumps(temporalValue))   
                insert_querry = "INSERT INTO tproperties(collection_id, mfeature_id, tproperties_name, datetime_group, tproperty, {0}) ".format(pvalue_Column)
                insert_querry +="VALUES ('{0}', '{1}', '{2}', {3}, '{4}', '{5}')".format(collection_id, mfeature_id, tproperties_name, datetime_group, json.dumps(temporalProperty[tproperties_name]), str(value))
                cursor.execute(insert_querry)
            else:
                insert_querry = "INSERT INTO tproperties(collection_id, mfeature_id, tproperties_name, datetime_group, tproperty) "
                insert_querry +="VALUES ('{0}', '{1}', '{2}', {3}, '{4}')".format(collection_id, mfeature_id, tproperties_name, datetime_group, json.dumps(temporalProperty[tproperties_name]))
                cursor.execute(insert_querry)

        return tproperties_name

    def postTemporalValue(self, collection_id, mfeature_id, tproperties_name, temporalValueData): 
        cursor = self.connection.cursor()
        
        datetimes = temporalValueData['datetimes']
        values = temporalValueData['values']
        interpolation = temporalValueData['interpolation']             
        temporalValue = self.createTemporalPropertyValue(datetimes, values, interpolation)

        datetime_group = self.getTemporalPropertiesGroup(collection_id, mfeature_id, datetimes)
        dataType = temporalValue["type"]
        pymeos_initialize()
        value = Temporal.from_mfjson(json.dumps(temporalValue))   
        
        pvalue_Column = "pValue_text"
        if dataType == 'MovingFloat':
            pvalue_Column = "pValue_float"

        insert_querry = "INSERT INTO tproperties(collection_id, mfeature_id, tproperties_name, datetime_group, {0}) ".format(pvalue_Column)
        insert_querry +="VALUES ('{0}', '{1}', '{2}', {3}, '{4}')".format(collection_id, mfeature_id, tproperties_name, datetime_group, str(value))
        cursor.execute(insert_querry)
        
        pValue_id = ''

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
    
    def validate_lifespan(self, datetime_=None) -> str:
        datetime_for_return = ''
        if datetime_ is not None and datetime_ != []:
            dateparse_begin = partial(dateparse, default=datetime.datetime.min)
            dateparse_end = partial(dateparse, default=datetime.datetime.max)

            datetime_begin = datetime_[0]
            datetime_end = datetime_[-1]
            datetime_begin = dateparse_begin(datetime_begin)
            if datetime_begin.tzinfo is None:
                datetime_begin = datetime_begin.replace(
                    tzinfo=pytz.UTC)

            datetime_end = dateparse_end(datetime_end)
            if datetime_end.tzinfo is None:
                datetime_end = datetime_end.replace(tzinfo=pytz.UTC)

            datetime_invalid = any([
                (datetime_begin > datetime_end)
            ])
            
            if not datetime_invalid:
                datetime_for_return = datetime_begin.strftime('%Y-%m-%d %H:%M:%S.%f') + ',' +  datetime_end.strftime('%Y-%m-%d %H:%M:%S.%f')
        return datetime_for_return
    
    def checkIfTemporalPropertyCanPost(self, collection_id, mfeature_id, temporalProperties, tproperties_name=None):
        cursor = self.connection.cursor()
        for temporalProperty in temporalProperties:
            g_temporalProperty = dict(temporalProperty)
            if 'datetimes' in g_temporalProperty:
                datetimes = g_temporalProperty["datetimes"]
                for i in range( len(datetimes)):
                    if isinstance(datetimes[i], int):
                        datetimes[i] = datetime.datetime.fromtimestamp(datetimes[i] / 1e3).strftime("%Y/%m/%dT%H:%M:%S.%f")
                    else:
                        datetimes[i] = datetimes[i].replace('Z','')

                tproperties_name_list = []
                if tproperties_name is not None:
                    tproperties_name_list = [tproperties_name]
                else:
                    for tproperties_name in g_temporalProperty:
                        tproperties_name_list.append(tproperties_name)

                select_query = "select collection_id, mfeature_id, tproperties_name, count(datetime_group) as intersect_count "
                select_query +="from tproperties "
                select_query +="where collection_id ='{0}' and mfeature_id='{1}' and tproperties_name in ({2}) ".format(collection_id, mfeature_id, "'"+ "', '".join(tproperties_name_list) + "'")
                select_query +="and ((period(pvalue_float) && period(timestampset('{0}'))) or (period(pvalue_text) && period(timestampset('{0}')))) ".format("{"+ ", ".join(datetimes) + "}")
                select_query +="group by collection_id, mfeature_id, tproperties_name"
                click.echo(select_query)
                cursor.execute(select_query)
                rows = cursor.fetchall()
                
                for row in rows:
                    if int(row[3]) > 0:
                        return False
        return True
        
    def getTemporalPropertiesGroup(self, collection_id, mfeature_id, datetimes):
        cursor = self.connection.cursor()
        for i in range( len(datetimes)):
            if isinstance(datetimes[i], int):
                datetimes[i] = datetime.datetime.fromtimestamp(datetimes[i] / 1e3).strftime("%Y/%m/%dT%H:%M:%S.%f")
            else:
                datetimes[i] = datetimes[i].replace('Z','')

        select_query = "select temp1.collection_id, temp1.mfeature_id, COALESCE(temp2.datetime_group, temp3.max_datetime_group) "
        select_query +="from (select collection_id, mfeature_id "
        select_query +="from tproperties "
        select_query +="where collection_id ='{0}' and mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +=") temp1 "
        select_query +="left outer join (select collection_id, mfeature_id, datetime_group "
        select_query +="from tproperties "
        select_query +="where collection_id ='{0}' and mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +="and (timestampset_eq(timestampset(timestamps(pvalue_float)), timestampset('{0}')) or timestampset_eq(timestampset(timestamps(pvalue_text)), timestampset('{0}'))) ".format("{"+ ", ".join(datetimes) + "}")
        select_query +=") temp2 on temp1.collection_id = temp2.collection_id and temp1.mfeature_id = temp2.mfeature_id "
        select_query +="left outer join (select collection_id, mfeature_id, COALESCE(max(datetime_group), 0) + 1 as max_datetime_group "
        select_query +="from tproperties "
        select_query +="where collection_id ='{0}' and mfeature_id='{1}' ".format(collection_id, mfeature_id)
        select_query +="group by collection_id, mfeature_id "
        select_query +=") temp3 on temp1.collection_id = temp3.collection_id and temp1.mfeature_id = temp3.mfeature_id "

        cursor.execute(select_query)
        rows = cursor.fetchall()
        if len(rows) > 0:
            return rows[0][2]
        return 1