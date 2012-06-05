from nose.tools import eq_ as eq

from ubernear.util import address

def test_normalize_simple():
    item = '  foo: BAR  ,sna  '
    res = address.normalize_string(item)

    eq(res, 'foo bar sna')

def test_normalize_street_simple():
    street = '2970 Swan Rd.,'
    res = address.normalize_street(street)

    eq(res, '2970 swan rd')

def test_normalize_street_cardinal_abbrev():
    street = '2970 N. Swan'
    res = address.normalize_street(street)

    eq(res, '2970 n swan')

    street = '1716 NORTH CAHUENGA,'
    res = address.normalize_street(street)

    eq(res, '1716 n cahuenga')

    street = '555 MAIN SOUTHEAST'
    res = address.normalize_street(street)

    eq(res, '555 main se')

    street = '555 northwest broadway'
    res = address.normalize_street(street)

    eq(res, '555 nw broadway')

def test_normalize_street_cardinal_abbrev_in_word():
    street = '2970 N. Least'
    res = address.normalize_street(street)

    eq(res, '2970 n least')

def test_normalize_street_street_abbrev():
    street = '2970 N. Swan Rd at Ft. Lowell'
    res = address.normalize_street(street)

    eq(res, '2970 n swan rd at ft lowell')

    street = '1716 NORTH CAHUENGA BOULEVARD,'
    res = address.normalize_street(street)

    eq(res, '1716 n cahuenga blvd')

    street = '1448 Gower St. (On The Corner Of Sunset Blvd)'
    res = address.normalize_street(street)

    eq(res, '1448 gower st on the cor of sunset blvd')

def test_normalize_street_street_abbrev_in_word():
    street = '555 Falley Boulevard)'
    res = address.normalize_street(street)

    eq(res, '555 falley blvd')

def test_normalize_street_empty():
    street = '.,'
    res = address.normalize_street(street)

    eq(res, '')

def test_normalize_city_simple():
    city = 'Los Angeles'
    res = address.normalize_city(city)

    eq(res, 'los angeles')

    city = 'L.A.'
    res = address.normalize_city(city)

    eq(res, 'la')

def test_normalize_city_empty():
    city = '.,'
    res = address.normalize_city(city)

    eq(res, '')

def test_normalize_state_simple():
    state = 'CALIFORNIA'
    res = address.normalize_state(state)

    eq(res, 'ca')

def test_normalize_state_invalid():
    state = 'FOO BAR'
    res = address.normalize_state(state)

    eq(res, 'foo bar')

def test_normalize_state_multiple():
    state = 'CA California'
    res = address.normalize_state(state)

    eq(res, 'ca california')

def test_normalize_state_other():
    state = 'Quebec'
    res = address.normalize_state(state)

    eq(res, 'quebec')

def test_normalize_state_empty():
    state = '.,'
    res = address.normalize_state(state)

    eq(res, '')

def test_normalize_country_simple():
    country = 'UNITED STATES'
    res = address.normalize_country(country)

    eq(res, 'usa')

    country = 'U.S.A'
    res = address.normalize_country(country)

    eq(res, 'usa')

def test_normalize_country_other():
    country = 'MEXICO'
    res = address.normalize_country(country)

    eq(res, 'mexico')

def test_normalize_country_empty():
    country = '.,'
    res = address.normalize_country(country)

    eq(res, '')

def test_normalize_zipcode_simple():
    zipcode = '08901'
    res = address.normalize_zipcode(zipcode)

    eq(res, '08901')

def test_normalize_zipcode_nine_digits():
    zipcode = '089010000'
    res = address.normalize_zipcode(zipcode)

    eq(res, '08901')

def test_normalize_zipcode_dash():
    zipcode = '08901-0000'
    res = address.normalize_zipcode(zipcode)

    eq(res, '08901')

def test_normalize_zipcode_zip4_str():
    zipcode = '08901-bas'
    res = address.normalize_zipcode(zipcode)

    eq(res, '08901')

def test_normalize_zipcode_zip5_str():
    zipcode = 'b8901-0000'
    res = address.normalize_zipcode(zipcode)

    eq(res, '')

def test_normalize_zipcode_zip5_short():
    zipcode = '0890'
    res = address.normalize_zipcode(zipcode)

    eq(res, '')

def test_normalize_zipcode_empty():
    zipcode = ''
    res = address.normalize_zipcode(zipcode)

    eq(res, '')

def test_streets_equal_simple():
    street1 = '555 South Main Street #111'
    street2 = '555 S. MAIN ST.'

    res = address.streets_equal(street1, street2)

    eq(res, True)

def test_streets_equal_not_equal():
    street1 = '555 Main Street South #111'
    street2 = '555 S. MAIN ST.'

    res = address.streets_equal(street1, street2)

    eq(res, False)

def test_streets_equal_empty():
    street1 = ''
    street2 = ''

    res = address.streets_equal(street1, street2)

    eq(res, False)

def test_cities_equal_simple():
    city1 = 'LOS ANGELES,'
    city2 = 'Los Angeles'

    res = address.cities_equal(city1, city2)

    eq(res, True)

def test_cities_equal_not_equal():
    city1 = 'LOS ANGELES'
    city2 = 'L.A.'

    res = address.cities_equal(city1, city2)

    eq(res, False)

def test_cities_equal_empty():
    city1 = ''
    city2 = ''

    res = address.cities_equal(city1, city2)

    eq(res, False)

def test_states_equal_simple():
    state1 = 'California,'
    state2 = 'CA'

    res = address.states_equal(state1, state2)

    eq(res, True)

def test_states_equal_not_equal():
    state1 = 'CALI'
    state2 = 'CA'

    res = address.states_equal(state1, state2)

    eq(res, False)

def test_states_equal_empty():
    state1 = ''
    state2 = ''

    res = address.states_equal(state1, state2)

    eq(res, False)

def test_countries_equal_simple():
    country1 = 'United States of America'
    country2 = 'US. Of A.'

    res = address.countries_equal(country1, country2)

    eq(res, True)

def test_countries_equal_not_equal():
    country1 = 'CANADA'
    country2 = 'CA'

    res = address.countries_equal(country1, country2)

    eq(res, False)

def test_countries_equal_empty():
    country1 = ''
    country2 = ''

    res = address.countries_equal(country1, country2)

    eq(res, False)

def test_zipcodes_equal_simple():
    zip1 = '08901'
    zip2 = '08901-0000'

    res = address.zipcodes_equal(zip1, zip2)

    eq(res, True)

def test_zipcodes_equal_not_equal():
    zip1 = '08901'
    zip2 = '8901-0000'

    res = address.zipcodes_equal(zip1, zip2)

    eq(res, False)

def test_zipcodes_equal_empty():
    zip1 = ''
    zip2 = ''

    res = address.zipcodes_equal(zip1, zip2)

    eq(res, False)
