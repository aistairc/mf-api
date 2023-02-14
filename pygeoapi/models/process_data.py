from mobilitydb.psycopg import register
import psycopg2
from pymeos import *
import json
from pygeoapi.provider.postgresql import PostgreSQLProvider
import click

from pygeofilter.parsers.ecql import parse
class ProcessData:
    host = '127.0.0.1'
    port = 5432
    db = 'mobility'
    user = 'postgres'
    password = 'postgres'
    connection = None

    def __init__(self):  
        self.connection = None
        
    def connect(self):        
        # Set the connection parameters to PostgreSQL
        self.connection = psycopg2.connect(host=self.host, database=self.db, user=self.user, password=self.password)
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

    def getCollections(self, bbox='', datetime='', limit='', offset=0):  
        geom_field = 'extent'  
        if bbox != None and len(bbox) == 6:
            geom_field = 'extent3d'
        config = {
            'name': 'PostgreSQL',
            'type': 'feature',
            'data': {'host': self.host,
                    'dbname': self.db,
                    'user': self.user,
                    'password': self.password,
                    'search_path': ['public']
                    },
            'id_field': 'collection_id',
            'table': 'collection_mfeature_view',
            'geom_field': geom_field
        }
        p = PostgreSQLProvider(config)
        collections = p.query(bbox=bbox, datetime= datetime, limit=limit, offset=offset)
        return collections
    
    def getCollection(self, collection_id):   
        geom_field = 'extent'
        filterq = parse("collection_id = '{0}'".format(collection_id))
        config = {
            'name': 'PostgreSQL',
            'type': 'feature',
            'data': {'host': self.host,
                    'dbname': self.db,
                    'user': self.user,
                    'password': self.password,
                    'search_path': ['public']
                    },
            'id_field': 'collection_id',
            'table': 'collection_mfeature_view',
            'geom_field': geom_field
        }
        p = PostgreSQLProvider(config)
        collection = p.query(filterq=filterq)
        return collection   

    def getFeaturesList(self):  
        cursor = self.connection.cursor()

        select_query = "SELECT collection_id, mfeature_id FROM mfeature"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return rows

    def getFeatures(self, collection_id, bbox='', datetime='', limit='', offset=0):  
        geom_field = 'extent'  
        filterq = parse("collection_id = '{0}'".format(collection_id))
        config = {
            'name': 'PostgreSQL',
            'type': 'feature',
            'data': {'host': self.host,
                    'dbname': self.db,
                    'user': self.user,
                    'password': self.password,
                    'search_path': ['public']
                    },
            'id_field': 'mfeature_id',
            'table': 'mfeature_tgeometry_view',
            'geom_field': geom_field
        }
        p = PostgreSQLProvider(config)
        features = p.query(bbox=bbox, datetime= datetime, limit=limit, offset=offset, filterq=filterq)

        features['numberMatched'] = (p.query(resulttype="hits", bbox=bbox, datetime= datetime, limit=limit, filterq=filterq)["numberMatched"])        
        features['numberReturned'] = (len(features.get('features')))

        return features

    def getFeature(self, collection_id, mfeature_id):  
        filterq = parse("collection_id = '{0}' AND mfeature_id='{1}'".format(collection_id, mfeature_id))
        geom_field = 'extent'  
        config = {
            'name': 'PostgreSQL',
            'type': 'feature',
            'data': {'host': self.host,
                    'dbname': self.db,
                    'user': self.user,
                    'password': self.password,
                    'search_path': ['public']
                    },
            'id_field': 'mfeature_id',
            'table': 'mfeature_tgeometry_view',
            'geom_field': geom_field
        }
        p = PostgreSQLProvider(config)
        feature = p.query(filterq=filterq)
        return feature

    def getTemporalGeometries(self, collection_id, mfeature_id, bbox='', datetime='', limit='', offset=0):  
        geom_field = 'geom'  
        filterq = parse("collection_id = '{0}' AND mfeature_id='{1}'".format(collection_id, mfeature_id))
        config = {
            'name': 'PostgreSQL',
            'type': 'feature',
            'data': {'host': self.host,
                    'dbname': self.db,
                    'user': self.user,
                    'password': self.password,
                    'search_path': ['public']
                    },
            'id_field': 'tgeometry_id',
            'table': 'tgeometry_view',
            'geom_field': geom_field
        }
        p = PostgreSQLProvider(config)
        features = p.query(bbox=bbox, datetime= datetime, limit=limit, offset=offset, filterq=filterq)

        features['numberMatched'] = (p.query(resulttype="hits", bbox=bbox, datetime= datetime, limit=limit, filterq=filterq)["numberMatched"])        
        features['numberReturned'] = (len(features.get('features')))

        return features

    def getTemporalProperties(self, collection_id, mfeature_id):  
        cursor = self.connection.cursor()
        select_query = "SELECT collection_id,mfeature_id,tpropertiesname,tproperty FROM tproperties WHERE collection_id ='{0}' AND mfeature_id='{1}'".format(collection_id, mfeature_id)
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
        temporalGeometries = g_movingfeature.pop("temporalGeometry", None) 
        temporalProperties = g_movingfeature.pop("temporalProperties", None)

        if 'geometry' in g_movingfeature:          
            geometry = g_movingfeature.pop("geometry", None)
            cursor.execute("INSERT INTO mfeature(collection_id, mf_geometry, mf_property) VALUES ('{0}', ST_GeomFromGeoJSON('{1}'), '{2}') RETURNING mfeature_id".format(collection_id, json.dumps(geometry), json.dumps(g_movingfeature)))
        else:
            cursor.execute("INSERT INTO mfeature(collection_id, mf_property) VALUES ('{0}', '{1}') RETURNING mfeature_id".format(collection_id, json.dumps(g_movingfeature)))
        mfeature_id = cursor.fetchone()[0]

        if temporalGeometries != None:
            temporalGeometries = [temporalGeometries] if not isinstance(temporalGeometries, list) else temporalGeometries
            for temporalGeometry in temporalGeometries:
                # for _ in range(10000):
                    self.postTemporalGeometry(collection_id, mfeature_id, temporalGeometry)

        if temporalProperties != None:
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
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO tproperties(collection_id, mfeature_id, tproperty) VALUES ('{0}', '{1}', '{2}') RETURNING tpropertiesname".format(collection_id, mfeature_id, json.dumps(temporalProperty)))
        tpropertiesname = cursor.fetchone()[0]

        return tpropertiesname

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

        temporalGeometry['lower_inc'] = True
        temporalGeometry['upper_inc'] = True
        return temporalGeometry

    def convertTemporalGeometryToOldVersion(self, temporalGeometry):
        if 'interpolations' in temporalGeometry:            
            temporalGeometry['interpolation'] = temporalGeometry['interpolations'][0]
            del temporalGeometry['interpolations']

        if temporalGeometry['type'] == 'MovingGeomPoint':
            temporalGeometry['type'] = 'MovingPoint'

        if 'datetimes' in temporalGeometry:
            datetimes = temporalGeometry['datetimes']            
            for i in range( len(datetimes)):
                datetimes[i] = datetimes[i] + 'Z'
            temporalGeometry['datetimes'] = datetimes

        del temporalGeometry['lower_inc']
        del temporalGeometry['upper_inc'] 
        return temporalGeometry
