#!/bin/bash

echo "shared_preload_libraries = 'postgis-3.so'" >> $PGDATA/postgresql.conf

set -e

# Create the 'mobilitydb' extension in the mobilitydb database
echo "Loading MobilityDB extension into mobilitydb"
psql --user="$POSTGRES_USER" --dbname="mobilitydb" <<- 'EOSQL'
	CREATE EXTENSION IF NOT EXISTS PostGIS;
	CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

	-- Table collection
  CREATE TABLE public.collection (
    collection_id uuid DEFAULT uuid_generate_v4 (),
    collection_property jsonb NULL,
    PRIMARY KEY (collection_id)
  );

  -- Table MovingFeature
  CREATE TABLE public.mfeature (
    collection_id uuid NOT NULL,
    mFeature_id uuid DEFAULT uuid_generate_v4 (),
    mf_geometry geometry NULL,
    mf_property jsonb NULL,
    PRIMARY KEY (collection_id, mFeature_id),
    FOREIGN KEY (collection_id) REFERENCES collection(collection_id)
  );

  -- Table TemporalGeometry
  CREATE TABLE public.tgeometry (
    collection_id uuid NOT NULL,
    mFeature_id uuid NOT NULL,
    tGeometry_id uuid DEFAULT uuid_generate_v4 (),
    tGeometry_property tgeompoint NULL,
    PRIMARY KEY (collection_id, mFeature_id, tGeometry_id),
    FOREIGN KEY (collection_id, mFeature_id) REFERENCES mfeature(collection_id, mFeature_id)
  );


  -- Table TemporalProperty
  CREATE TABLE public.tproperties (
    collection_id uuid NOT NULL,
    mFeature_id uuid NOT NULL,
    tProperties_Name text NOT NULL,
    tProperty jsonb NULL,
    PRIMARY KEY (collection_id, mFeature_id, tProperties_Name),
    FOREIGN KEY (collection_id, mFeature_id) REFERENCES mfeature(collection_id, mFeature_id)
  );

  -- Table TemporalValues
  CREATE TABLE public.tpropertiesvalue (
    collection_id uuid NOT NULL,
    mFeature_id uuid NOT NULL,
    tProperties_Name text NOT NULL,
    pValue_id uuid DEFAULT uuid_generate_v4 (),
    pvalue_float tfloat NULL,
    pvalue_text ttext NULL,
    PRIMARY KEY (collection_id, mFeature_id, tProperties_Name, pValue_id),
    FOREIGN KEY (collection_id, mFeature_id, tProperties_Name) REFERENCES tproperties(collection_id, mFeature_id, tProperties_Name)
  );


  -- View of the combination of collection and MovingFeature
  CREATE OR REPLACE VIEW public.collection_mfeature_view
  AS SELECT collection.collection_id,
    collection.collection_property,
    string_agg(DISTINCT st_srid(mfeature.mf_geometry)::text, ';'::text) AS crs,
    Max(ST_NDims(mfeature.mf_geometry)) as ndims,
    st_extent(mfeature.mf_geometry)::text AS bbox,
    st_3dextent(mfeature.mf_geometry)::text AS bbox3d,
    st_extent(mfeature.mf_geometry)::geometry AS extent,
    st_3dextent(mfeature.mf_geometry)::geometry AS extent3d
    FROM collection
      LEFT JOIN mfeature ON collection.collection_id = mfeature.collection_id
    GROUP BY collection.collection_id, collection.collection_property;

  -- Permissions

  ALTER TABLE public.collection_mfeature_view OWNER TO docker;
  GRANT ALL ON TABLE public.collection_mfeature_view TO docker;


  -- View of the combination of MovingFeature and TemporalGeometry
  CREATE OR REPLACE VIEW public.mfeature_tgeometry_view
  AS SELECT mfeature.collection_id,
    mfeature.mfeature_id,
    st_asgeojson(mfeature.mf_geometry) AS mf_geometry,
    mfeature.mf_property,
    string_agg(DISTINCT srid(tgeometry.tgeometry_property)::text, ';'::text) AS crs,
    extent(tgeometry.tgeometry_property)::text AS bbox,
    extent(tgeometry.tgeometry_property)::geometry AS extent
    FROM mfeature
      LEFT JOIN tgeometry ON mfeature.collection_id = tgeometry.collection_id AND mfeature.mfeature_id = tgeometry.mfeature_id
    GROUP BY mfeature.collection_id, mfeature.mfeature_id, mfeature.mf_geometry, mfeature.mf_property;

  -- Permissions

  ALTER TABLE public.mfeature_tgeometry_view OWNER TO docker;
  GRANT ALL ON TABLE public.mfeature_tgeometry_view TO docker;


  -- View of converting TemporalGeometry's tgeompoint data to mfjson
  CREATE OR REPLACE VIEW public.tgeometry_view
  AS SELECT tgeometry.collection_id,
    tgeometry.mfeature_id,
    tgeometry.tgeometry_id,
    asmfjson(tgeometry.tgeometry_property) as tgeometry_property,
    tgeometry.tgeometry_property::geometry AS geom
    FROM tgeometry;

  -- Permissions

  ALTER TABLE public.tgeometry_view OWNER TO docker;
  GRANT ALL ON TABLE public.tgeometry_view TO docker;
EOSQL
