CREATE TABLE Locations (
	Location_ID SERIAL PRIMARY KEY,
	Lat FLOAT,
	Long FLOAT,
	City VARCHAR(100),
	Timezone Varchar(100),
	Timezone_offset INTEGER
);

CREATE TABLE Weather (
	Weather_ID INTEGER PRIMARY KEY, --provided from the data source
	Main VARCHAR(50),
	Description TEXT
);

CREATE TABLE Records (
	Record_ID SERIAL PRIMARY KEY,
	Location_ID INTEGER,
	Weather_ID INTEGER,
	Local_time TIMESTAMP,
	Sunrise TIMESTAMP,
	Sunset TIMESTAMP,
	Temp_F FLOAT,
	Feels_like_F FLOAT,
	Humidity FLOAT,
	Dew_Point FLOAT,
	UVI FLOAT,
	Clouds Integer,
	Visibility FLOAT,
	Wind_speed_mph FLOAT,
	Wind_deg INTEGER,
	Wind_gust_mph FLOAT,
	FOREIGN KEY (Location_ID) REFERENCES Locations(Location_ID),
	FOREIGN KEY (Weather_ID) REFERENCES Weather(Weather_ID)
);

CREATE TABLE Alerts (
	Alert_ID SERIAL PRIMARY KEY,
	Record_ID INTEGER,
	Description TEXT,
	FOREIGN KEY (Record_ID) REFERENCES Records(Record_ID)
);







