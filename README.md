# System setup
## １．Database construction
###	Install Postgresql
- Create the file repository configuration:
```
$ sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
```

- Import the repository signing key:
```
$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
```

- Update the package lists:

```
sudo apt-get update
		
```

- Install PostgreSQL version 14:
		
```
sudo apt-get -y install postgresql-14
		
```

- Install PostGIS
```
sudo apt -y install postgresql-14-postgis-3
```

- Change permissions to edit files
```
sudo chmod 777 /etc/postgresql/14/main/pg_hba.conf
sudo chmod 777 /etc/postgresql/14/main/postgresql.conf
```

- Open file /etc/postgresql/14/main/pg_hba.conf and add at the end
```
sudo nano /etc/postgresql/14/main/pg_hba.conf
> host    all             all             0.0.0.0/0               md5
```

- Open file /etc/postgresql/14/main/postgresql.conf and configure as follows
```
sudo nano /etc/postgresql/14/main/postgresql.conf
> listen_addresses = '*'
> shared_preload_libraries = 'postgis-3'
> max_locks_per_transaction = 128
```
- Restore the original permissions
```
sudo chmod 640 /etc/postgresql/14/main/pg_hba.conf
sudo chmod 644 /etc/postgresql/14/main/postgresql.conf
```

- Log in as postgres user
```
sudo -i -u postgres
```

- Start postgresql service
```
service postgresql restart
```


### Install mobilityDB
- Update package list
```
sudo apt update
sudo apt -y upgrade
```

- Install the required packages
```
sudo apt install build-essential cmake libproj-dev libjson-c-dev
```

- Install postgresql-server-dev-14
```
sudo apt install postgresql-server-dev-14 

sudo apt install libgeos-dev   => Install if error in cmake
sudo apt-get install libgsl-dev   => Install if error in cmake
```

- Download mobilityDB package
```
wget https://github.com/MobilityDB/MobilityDB/archive/refs/tags/v1.0.zip
unzip v1.0.zip
mv MobilityDB-1.0 MobilityDB
```

- Create mobilityDB installation file
```
mkdir MobilityDB/build
cd MobilityDB/build
cmake ..

make
```

- Execute mobilityDB installation file
```
sudo make install
```

### Create database and extension
- Log in as postgres user
```
sudo -i -u postgres
```

- Set password for user postgres
```
psql -c  "alter role postgres with password 'postgres'"
```

- Create database
```
createdb mobility
```

- Create extension
```
psql mobility -c "CREATE EXTENSION PostGIS"
psql mobility -c "CREATE EXTENSION MobilityDB"
psql mobility -c 'CREATE EXTENSION "uuid-ossp"'
```

### Create tables
```
psql mobility
```
```
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
	FOREIGN KEY (collection_id, mfeature_id) REFERENCES mfeature(collection_id, mfeature_id)
);

-- Table TemporalProperty
CREATE TABLE public.tproperties (	
	collection_id uuid NOT NULL,
	mFeature_id uuid NOT NULL,
	tPropertiesName uuid DEFAULT uuid_generate_v4 (),
	tProperty jsonb NULL,
	PRIMARY KEY (collection_id, mFeature_id, tPropertiesName),
	FOREIGN KEY (collection_id, mfeature_id) REFERENCES mfeature(collection_id, mfeature_id)
);

-- Table TemporalValues
CREATE TABLE public.tpropertiesvalue (	
	collection_id uuid NOT NULL,
	mFeature_id uuid NOT NULL,
	tPropertiesName uuid NOT NULL,
	pValue_id uuid DEFAULT uuid_generate_v4 (),
	pValue jsonb NULL,
	PRIMARY KEY (collection_id, mFeature_id, tPropertiesName, pValue_id),
	FOREIGN KEY (collection_id, mfeature_id, tPropertiesName) REFERENCES tproperties(collection_id, mfeature_id, tPropertiesName)
);
```

### Create views
```
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

ALTER TABLE public.collection_mfeature_view OWNER TO postgres;
GRANT ALL ON TABLE public.collection_mfeature_view TO postgres;


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

ALTER TABLE public.mfeature_tgeometry_view OWNER TO postgres;
GRANT ALL ON TABLE public.mfeature_tgeometry_view TO postgres;


-- View of converting TemporalGeometry's tgeompoint data to mfjson
CREATE OR REPLACE VIEW public.tgeometry_view
AS SELECT tgeometry.collection_id,
	tgeometry.mfeature_id,
	tgeometry.tgeometry_id,
	asmfjson(tgeometry.tgeometry_property) as tgeometry_property,
	tgeometry.tgeometry_property::geometry AS geom
	FROM tgeometry;

-- Permissions

ALTER TABLE public.tgeometry_view OWNER TO postgres;
GRANT ALL ON TABLE public.tgeometry_view TO postgres;
```


## ２．Pygeoapi construction
### Install python
- execute the following three lines of command to install python 3.7
```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.7-dev python3.7-venv
```

### Install pygeoapi
- Run in etc folder。
```
cd /etc
```

- Create pygeoapi folder, create and activate virtual environment
```
sudo mkdir pygeoapi 
cd pygeoapi
sudo chmod -R 777 /etc/pygeoapi/
python3.7 -m venv pygeoapi 
cd pygeoapi
source bin/activate
```

- download mf-api(pygeoapi)
```
git clone https://github.com/aistairc/mf-api.git
```

- Install the required packages
```
cd mf-api
pip3 install -r requirements.txt
```

- Execute pygeoapi installation
```
python3 setup-mf-api.py install
```

- Run bash file to configure file
```
bash build.sh
```

- Restore the original permissions
```
sudo chmod -R 755 /etc/pygeoapi/
```

- Set environment path
```
export PYGEOAPI_CONFIG=example-config.yml
export PYGEOAPI_OPENAPI=example-openapi.yml
```

### Install required libraries in virtual environment
- MEOS (Mobility Engine, Open Source) is a C library which enables the manipulation of temporal and spatio-temporal data based on MobilityDB's data types and functions
```
pip install pymeos 	
```
- A Flask extension for handling Cross Origin Resource Sharing (CORS), making cross-origin AJAX possible
```
pip install -U flask-cors	
```
- SQLAlchemy is the Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL
```
pip install SQLAlchemy		
```
- GeoAlchemy 2 is a Python toolkit for working with spatial databases
```
pip install GeoAlchemy2
```
- Psycopg is the most popular PostgreSQL database adapter for the Python programming language
```
pip install psycopg2-binary
```
- MobilityDB-python is a database adapter to access MobilityDB from Python
```
pip install python-mobilitydb	
```

### Start pygeoapi
- Start server
```
pygeoapi serve
```

- Run in another terminal and open homepage
```
curl http://localhost:5000  # Or open in web browser
```