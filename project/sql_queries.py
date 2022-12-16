modes_table_drop = ("DROP TABLE IF EXISTS modes;")
ports_table_drop = ("DROP TABLE IF EXISTS ports;")
visas_table_drop = ("DROP TABLE IF EXISTS visas;")
states_table_drop = ("DROP TABLE IF EXISTS states;")
countries_table_drop = ("DROP TABLE IF EXISTS countires;")
i94_table_drop = ("DROP TABLE IF EXISTS i94_data;")



modes_table_create = ("""
    CREATE TABLE IF NOT EXISTS modes (
        i94_mode_id integer PRIMARY KEY,
        mode varchar(50)
    ) DISTKEY(i94_mode_id);
""")

ports_table_create = ("""
    CREATE TABLE IF NOT EXISTS ports (
        i94_port_id char(3)  PRIMARY KEY,
        city_state varchar(100),
        city varchar(50),
        state varchar(50) NULL
    ) DISTKEY(i94_port_id);
""")

visa_table_create = ("""
    CREATE TABLE IF NOT EXISTS visas (
        i94_visa_id integer PRIMARY KEY,
        visa_type varchar(100)
    ) DISTKEY(i94_visa_id);
""")

states_table_create = ("""
    CREATE TABLE IF NOT EXISTS states (
        state_code char(2) PRIMARY KEY,
        state varchar(100)
    ) DISTKEY(state_code);
""")

countries_table_create = ("""
    CREATE TABLE IF NOT EXISTS countries (
        i94_res_id integer PRIMARY KEY,
        country varchar(100)
    ) DISTKEY(i94_res_id);
""")

i94_table_create = ("""
    CREATE TABLE IF NOT EXISTS i94_data (     
        cicid integer,
        i94cit integer,
        i94res integer,
        i94port varchar(3),
        arrdate date,
        i94mode integer,
        i94addr varchar(2),
        depdate date,
        i94bir integer,
        i94visa integer,
        count integer,
        dtadfile date,
        visapost varchar(3),
        occup varchar(3),
        entdepa varchar(1),
        entdepd varchar(1),
        entdepu varchar(1),
        matflag varchar(1),
        biryear integer,
        dtaddto date,
        gender varchar(1),
        insnum varchar(10),
        airline varchar(3),
        admnum integer,
        fltno varchar(5),
        visatype varchar(3),
        FOREIGN KEY(visatype) REFERENCES visas(i94_visa_id),
      FOREIGN KEY(i94mode) REFERENCES modes(i94_mode_id),
      FOREIGN KEY(i94port) REFERENCES ports(i94_port_id),
      FOREIGN KEY(i94addr) REFERENCES states(state_code)
    ) DISTKEY(i94port);
""")

modes_copy = ("""
    copy modes from 's3://tom-baird-capstone-project-2/raw/dim/I94MODE.csv'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format csv 
        IGNOREHEADER 1;
""")

ports_copy = ("""
    copy ports from 's3://tom-baird-capstone-project-2/raw/dim/I94PORT.csv'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format csv 
        IGNOREHEADER 1;
""")

visa_copy = ("""
    copy visas from 's3://tom-baird-capstone-project-2/raw/dim/I94VISA.csv'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format csv 
        IGNOREHEADER 1;
""")

states_copy = ("""
    copy states from 's3://tom-baird-capstone-project-2/raw/dim/I94ADDR.csv'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format csv 
        IGNOREHEADER 1;
""")

countries_copy = ("""
    copy countries from 's3://tom-baird-capstone-project-2/raw/dim/I94RES.csv'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format csv 
        IGNOREHEADER 1;
""")
#imm/i94yr=2016/i94mon=1/part-00000-1c04fe9e-a2e6-43aa-84b4-23e749fa3167.c000.snappy.parquet

i94_copy = ("""
    copy i94_data from 's3://tbcp/raw/imm/'
        credentials 'aws_iam_role={}'
        format parquet;
""")


# QUERY LISTS

create_table_queries = [modes_table_create, ports_table_create, visa_table_create, states_table_create, countries_table_create, i94_table_create]
copy_to_dim_queries = [modes_copy, ports_copy, visa_copy, states_copy, countries_copy]
copy_to_fact_queries = [i94_copy]
drop_dim_table_queries = [i94_table_drop, modes_table_drop, ports_table_drop, visas_table_drop, states_table_drop, countries_table_drop]