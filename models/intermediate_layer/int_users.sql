
SELECT
ID,
FIRST_NAME,
LAST_NAME,
EMAIL,
AGE,
GENDER,
STATE,
STREET_ADDRESS,
POSTAL_CODE,
CITY,
COUNTRY,
LATITUDE,
LONGITUDE,
TRAFFIC_SOURCE,
{{ time_travel('created_at',10,'year')}} AS created_at

from {{ref('Stg_users')}}