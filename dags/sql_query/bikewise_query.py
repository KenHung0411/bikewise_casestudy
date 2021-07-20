create_bikewise_stage = '''
    CREATE TABLE IF NOT EXISTS bikewise_stage (
        date_stolen bigint,
        description text,
        frame_colors text[],
        frame_model text,
        id bigint,
        is_stock_img boolean,
        large_img text,
        location_found text,
        manufacturer_name text,
        external_id text,
        registry_name text,
        registry_url text,
        serial text,
        status text,
        stolen bool,
        stolen_location text,
        thumb text,
        title text,
        url text,
        year integer,
        CONSTRAINT bikewise_stage_id_pk PRIMARY KEY (id)
    );
    TRUNCATE TABLE bikewise_stage;
'''

create_bikewise = '''
    CREATE TABLE IF NOT EXISTS bikewise (
        date_stolen bigint,
        description text,
        frame_colors text[],
        frame_model text,
        id bigint,
        is_stock_img boolean,
        large_img text,
        location_found text,
        manufacturer_name text,
        external_id text,
        registry_name text,
        registry_url text,
        serial text,
        status text,
        stolen bool,
        stolen_location text,
        thumb text,
        title text,
        url text,
        year integer,
        CONSTRAINT bikewise_stage_id_pk PRIMARY KEY (id)
    );
'''

upsert_bike_wise = '''
    INSERT INTO bikewise
    SELECT * FROM bikewise_stage
    ON CONFLICT (id) 
    DO 
    UPDATE SET date_stolen = EXCLUDED.date_stolen,
               description = EXCLUDED.description,
               frame_colors = EXCLUDED.frame_colors,
               frame_model = EXCLUDED.frame_model,
               is_stock_img = EXCLUDED.is_stock_img,
               large_img = EXCLUDED.large_img,
               location_found = EXCLUDED.location_found,
               manufacturer_name = EXCLUDED.manufacturer_name,
               external_id = EXCLUDED.external_id,
               registry_name  = EXCLUDED.registry_name,
               registry_url = EXCLUDED.registry_url,
               serial = EXCLUDED.serial,
               status = EXCLUDED.status,
               stolen = EXCLUDED.stolen,
               stolen_location = EXCLUDED.stolen_location,
               thumb = EXCLUDED.thumb,
               title = EXCLUDED.title,
               url = EXCLUDED.url,
               year = EXCLUDED.year
    ;
'''
