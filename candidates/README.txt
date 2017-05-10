README

Step 0: Set up a local postgres database with schema where you can create these tables using the following ddls.

Step 1: Clone the repo and place 311_service_requests.csv and traffic_accidents.csv at the root of the project.

Step 2: Open intellij and set up a build configuration to run the project (Normally this would be run on EMR and issued via spark-submit but it is just easier locally for this data)
    Class Path: com.carpenter.ibotta.Ingest
    VM options: -Dspark.master=local[*]
    Program Arguments: -j {linktolocalpostgres} -u {username} -p {password}
        my -j is jdbc:postgresql:postgres
    Module: IbottaBigData

Step 3: Run main to populate tables

Step 4: Run the queries described and documented below :)

service_requests DDL:
CREATE TABLE service_requests
(
  case_summary            VARCHAR,
  case_status             VARCHAR,
  case_source             VARCHAR,
  case_created_date       DATE,
  case_created_dttm       VARCHAR,
  case_closed_date        DATE,
  case_closed_dttm        VARCHAR,
  first_call_resolution   VARCHAR,
  customer_zip_code       INT,
  incident_address_1      VARCHAR,
  incident_address_2      VARCHAR,
  incident_intersection_1 VARCHAR,
  incident_intersection_2 VARCHAR,
  incident_zip_code       INT,
  longitude               FLOAT,
  latitude                FLOAT,
  agency                  VARCHAR,
  division                VARCHAR,
  major_area              VARCHAR,
  type                    VARCHAR,
  topic                   VARCHAR,
  council_district        INT,
  police_district         INT,
  neighborhood            VARCHAR
);
CREATE INDEX service_requests__index
  ON service_requests (police_district, case_closed_date, case_created_date, incident_address_1)


traffic_accidents DDL:
CREATE TABLE traffic_accidents
(
  "incident_id"                INT,
  "offense_id"                 INT,
  "offense_code"               INT,
  "offense_code_extension"     INT,
  "offense_type_id"            VARCHAR,
  "offense_category_id"        VARCHAR,
  "first_occurrence_date"      VARCHAR,
  "last_occurrence_date"       VARCHAR,
  "reported_date"              VARCHAR,
  "first_occurrence_date_cast" DATE,
  "last_occurrence_date_cast"  DATE,
  "reported_date_cast"         DATE,
  "incident_address"           VARCHAR,
  "geo_x"                      INT,
  "geo_y"                      INT,
  "geo_lon"                    FLOAT,
  "geo_lat"                    FLOAT,
  "district_id"                INT,
  "precinct_id"                INT,
  "neighborhood_id"            VARCHAR,
  "bicycle_ind"                INT,
  "pedestrian_ind"             INT
);
CREATE INDEX traffic_accidents__index
  ON traffic_accidents (district_id, reported_date_cast, geo_lon, geo_lat, neighborhood_id, first_occurrence_date_cast)


Selected queries:

    I am not a sql wizard, though I am working on it more every day! My specialy is definitely in ingesting and transforming data at this point, but my biggest
    goal for the rest of this year is to improve my SQL-fu.

    Stats about neighborhoods where neighborhood data is available.

    Information about the frequency of different reported cases by neighborhood with regrads to service requests.

    SELECT
        neighborhood,
        case_summary,
        count(case_summary)
    FROM service_requests
    WHERE neighborhood != ''
    GROUP BY case_summary, neighborhood;

    Neighborhoods with the most service requests.
        Unsurprisingly athmar-park, montbello and gateway---green-valley-ranch top this list.

    SELECT
      neighborhood,
      count(neighborhood) AS count
    FROM service_requests
    WHERE neighborhood != ''
    GROUP BY neighborhood
    ORDER BY count DESC;

    Neighborhoods with the most traffic incidents

    SELECT
      neighborhood_id,
      count(neighborhood_id) AS count
    FROM traffic_accidents
    WHERE neighborhood_id != '' OR neighborhood_id IS NOT NULL
    GROUP BY neighborhood_id
    ORDER BY count DESC;

    Stats about traffic incident types:

     Traffic accidents are by far the most common, but it is still surprising that there where 33,397 hit and runs!
     That is nearly 100 a day!

    SELECT
      offense_type_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    GROUP BY offense_type_id
    ORDER BY count DESC;

    Expanding a bit to include the neighborhood in the group by we find that high density areas like cap hill, five points and globeville
    are heavy on traffic incidents as well as high throughput areas like stapelton and speer.

    SELECT
      offense_type_id,
      neighborhood_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    GROUP BY offense_type_id, neighborhood_id
    ORDER BY count DESC;

    Traffic incident types by season:

    I was very surprised to see that the accident rate was not skewed towards the snowier months at all! In fact,
    there were more in summer than in winter!

    SELECT
      offense_type_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    WHERE EXTRACT(MONTH FROM traffic_accidents.first_occurrence_date_cast) IN (12, 1, 2)
    GROUP BY offense_type_id;

    SELECT
      offense_type_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    WHERE EXTRACT(MONTH FROM traffic_accidents.first_occurrence_date_cast) IN (3, 4, 5)
    GROUP BY offense_type_id;

    SELECT
      offense_type_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    WHERE EXTRACT(MONTH FROM traffic_accidents.first_occurrence_date_cast) IN (6, 7, 8)
    GROUP BY offense_type_id;

    SELECT
      offense_type_id,
      count(offense_type_id) AS count
    FROM traffic_accidents
    WHERE EXTRACT(MONTH FROM traffic_accidents.first_occurrence_date_cast) IN (9, 10, 11)
    GROUP BY offense_type_id;

    Port-o problem:

    By this query it appears that portable toilet issues stand as an outlier in resolution time

    SELECT
      avg(case_closed_date - service_requests.case_created_date) AS average,
      case_summary
    FROM service_requests
    WHERE case_closed_date IS NOT NULL
    GROUP BY case_summary
    ORDER BY average DESC;