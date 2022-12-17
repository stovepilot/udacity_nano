# Data dictionary

## i94_data
Feature|Description
---|---
CICID| unique numer of the file
I94CIT| city where the applicant is living
I94RES| state where the applicant is living
I94PORT| location (port) where the application is issued
ARRDATE| arrival date in USA in SAS date format
I94MODE| how did the applicant arrived in the USA
I94ADDR| US state where the port is
DEPDATE| is the Departure Date from the USA
I94BIR| age of applicant in years
I94VISA| what kind of VISA
COUNT| used for summary statistics| always 1
DTADFILE| date added to I-94 Files
VISAPOST| department of State where where Visa was issued
OCCUP| occupation that will be performed in U.S.
ENTDEPA| arrival Flag
ENTDEPD| departure Flag
ENTDEPU| update Flag
MATFLAG| match flag
BIRYEAR| 4 digit year of birth
DTADDTO| date to which admitted to U.S. (allowed to stay until)
GENDER| non-immigrant sex
INSNUM| INS number
AIRLINE| airline used to arrive in USA
ADMNUM| admission Number
FLTNO| flight number of Airline used to arrive in USA
VISATYPE| class of admission legally admitting the non-immigrant to temporarily stay in USA

## modes
Field|Datatype|Description
---|---|---
i94_mode_id |INT|The arrival mode ID held in the I94 data
mode|VARCHAR(50)| The mode of arrival

## ports
Field|Datatype|Description
---|---|---
i94_port_id| char(3)| Unique ID
city_state|varchar(100)| The city and state of the port
city|varchar(50)| City
state|varchar(50)| State

## states
Field|Datatype|Description
---|---|---
state_code| char(2)| Unique id
state |varchar(100)|State

## countries
Field|Datatype|Description
---|---|---
i94_res_id| INT | Unique ID
country|varchar(50)| Country where the applicant is living

## visas
Field|Datatype|Description
---|---|---
i94_visa_id| integer| unique id
visa_type| varchar(100)|class of admission legally admitting the non-immigrant to temporarily stay in USA
