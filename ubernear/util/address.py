import string

from collections import OrderedDict

cardinal_abbrev = OrderedDict([
        ('s', ['south']),
        ('n', ['north']),
        ('e', ['east']),
        ('w', ['west']),
        ('nw', ['northwest','north-west','north west']),
        ('ne', ['northeast','north-east','north east']),
        ('sw', ['southwest','south-west','south west']),
        ('se', ['southeast','south-east','south east']),
        ])

# https://www.usps.com/send/official-abbreviations.htm
street_abbrev = OrderedDict([
        ('aly', ['allee', 'alley', 'ally', 'aly']),
        ('anx', ['anex', 'annex', 'annex', 'anx']),
        ('arc', ['arc', 'arcade']),
        ('ave', ['av', 'ave', 'aven', 'avenu', 'avenue', 'avn', 'avnue']),
        ('bch', ['bch', 'beach']),
        ('bg', ['burg']),
        ('bgs', ['burgs']),
        ('blf', ['blf', 'bluf', 'bluff']),
        ('blfs', ['bluffs']),
        ('blvd', ['blvd', 'boul', 'boulevard', 'boulv']),
        ('bnd', ['bend', 'bnd']),
        ('br', ['br', 'branch', 'brnch']),
        ('brg', ['brdge', 'brg', 'bridge']),
        ('brk', ['brk', 'brook']),
        ('brks', ['brooks']),
        ('btm', ['bot', 'bottm', 'bottom', 'btm']),
        ('byp', ['byp', 'bypa', 'bypas', 'bypass', 'byps']),
        ('byu', ['bayoo', 'bayou']),
        ('cir', ['cir', 'circ', 'circl', 'circle', 'crcl', 'crcle']),
        ('cirs', ['circles']),
        ('clb', ['clb', 'club']),
        ('clf', ['clf', 'cliff']),
        ('clfs', ['clfs', 'cliffs']),
        ('cmn', ['common']),
        ('cor', ['cor', 'corner']),
        ('cors', ['corners', 'cors']),
        ('cp', ['camp', 'cmp', 'cp']),
        ('cpe', ['cape', 'cpe']),
        ('cres', ['crecent', 'cres', 'crescent', 'cresent', 'crscnt', 'crsent', 'crsnt']),
        ('crk', ['ck', 'cr', 'creek', 'crk']),
        ('crse', ['course', 'crse']),
        ('crst', ['crest']),
        ('cswy', ['causeway', 'causway', 'cswy']),
        ('ct', ['court', 'crt', 'ct']),
        ('ctr', ['cen', 'cent', 'center', 'centr', 'centre', 'cnter', 'cntr', 'ctr']),
        ('ctrs', ['centers']),
        ('cts', ['courts', 'ct']),
        ('curv', ['curve']),
        ('cv', ['cove', 'cv']),
        ('cvs', ['coves']),
        ('cyn', ['canyn', 'canyon', 'cnyn', 'cyn']),
        ('dl', ['dale', 'dl']),
        ('dm', ['dam', 'dm']),
        ('dr', ['dr', 'driv', 'drive', 'drv']),
        ('drs', ['drives']),
        ('dv', ['div', 'divide', 'dv', 'dvd']),
        ('est', ['est', 'estate']),
        ('ests', ['estates', 'ests']),
        ('expy', ['exp', 'expr', 'express', 'expressway', 'expw', 'expy']),
        ('ext', ['ext', 'extension', 'extn', 'extnsn']),
        ('exts', ['extensions', 'exts']),
        ('fall', ['fall']),
        ('fld', ['field', 'fld']),
        ('flds', ['fields', 'flds']),
        ('fls', ['falls', 'fls']),
        ('flt', ['flat', 'flt']),
        ('flts', ['flats', 'flts']),
        ('frd', ['ford', 'frd']),
        ('frds', ['fords']),
        ('frg', ['forg', 'forge', 'frg']),
        ('frgs', ['forges']),
        ('frk', ['fork', 'frk']),
        ('frks', ['forks', 'frks']),
        ('frst', ['forest', 'forests', 'frst']),
        ('fry', ['ferry', 'frry', 'fry']),
        ('ft', ['fort', 'frt', 'ft']),
        ('fwy', ['freeway', 'freewy', 'frway', 'frwy', 'fwy']),
        ('gdn', ['garden', 'gardn', 'gdn', 'grden', 'grdn']),
        ('gdns', ['gardens', 'gdns', 'grdns']),
        ('gln', ['glen', 'gln']),
        ('glns', ['glens']),
        ('grn', ['green', 'grn']),
        ('grns', ['greens']),
        ('grv', ['grov', 'grove', 'grv']),
        ('grvs', ['groves']),
        ('gtwy', ['gateway', 'gatewy', 'gatway', 'gtway', 'gtwy']),
        ('hbr', ['harb', 'harbor', 'harbr', 'hbr', 'hrbor']),
        ('hbrs', ['harbors']),
        ('hl', ['hill', 'hl']),
        ('hls', ['hills', 'hls']),
        ('holw', ['hllw', 'hollow', 'hollows', 'holw', 'holws']),
        ('hts', ['height', 'heights', 'hgts', 'ht', 'hts']),
        ('hvn', ['haven', 'havn', 'hvn']),
        ('hwy', ['highway', 'highwy', 'hiway', 'hiwy', 'hway', 'hwy']),
        ('inlt', ['inlet', 'inlt']),
        ('is', ['is', 'island', 'islnd']),
        ('isle', ['isle', 'isles']),
        ('iss', ['islands', 'islnds', 'iss']),
        ('jct', ['jct', 'jction', 'jctn', 'junction', 'junctn', 'juncton']),
        ('jcts', ['jctns', 'jcts', 'junctions']),
        ('knl', ['knl', 'knol', 'knoll']),
        ('knls', ['knls', 'knolls']),
        ('ky', ['key', 'ky']),
        ('kys', ['keys', 'kys']),
        ('land', ['land']),
        ('lck', ['lck', 'lock']),
        ('lcks', ['lcks', 'locks']),
        ('ldg', ['ldg', 'ldge', 'lodg', 'lodge']),
        ('lf', ['lf', 'loaf']),
        ('lgt', ['lgt', 'light']),
        ('lgts', ['lights']),
        ('lk', ['lake', 'lk']),
        ('lks', ['lakes', 'lks']),
        ('ln', ['la', 'lane', 'lanes', 'ln']),
        ('lndg', ['landing', 'lndg', 'lndng']),
        ('loop', ['loop', 'loops']),
        ('mall', ['mall']),
        ('mdw', ['mdw', 'meadow']),
        ('mdws', ['mdws', 'meadows', 'medows']),
        ('mews', ['mews']),
        ('ml', ['mill', 'ml']),
        ('mls', ['mills', 'mls']),
        ('mnr', ['manor', 'mnr']),
        ('mnrs', ['manors', 'mnrs']),
        ('msn', ['mission', 'missn', 'msn', 'mssn']),
        ('mt', ['mnt', 'mount', 'mt']),
        ('mtn', ['mntain', 'mntn', 'mountain', 'mountin', 'mtin', 'mtn']),
        ('mtns', ['mntns', 'mountains']),
        ('mtwy', ['motorway']),
        ('nck', ['nck', 'neck']),
        ('opas', ['overpass']),
        ('orch', ['orch', 'orchard', 'orchrd']),
        ('oval', ['oval', 'ovl']),
        ('park', ['park', 'pk', 'prk', 'parks']),
        ('pass', ['pass']),
        ('path', ['path', 'paths']),
        ('pike', ['pike', 'pikes']),
        ('pkwy', ['parkway', 'parkwy', 'pkway', 'pkwy', 'pky', 'parkways', 'pkwys']),
        ('pl', ['pl', 'place']),
        ('pln', ['plain', 'pln']),
        ('plns', ['plaines', 'plains', 'plns']),
        ('plz', ['plaza', 'plz', 'plza']),
        ('pne', ['pine']),
        ('pnes', ['pines', 'pnes']),
        ('pr', ['pr', 'prairie', 'prarie', 'prr']),
        ('prt', ['port', 'prt']),
        ('prts', ['ports', 'prts']),
        ('psge', ['passage']),
        ('pt', ['point', 'pt']),
        ('pts', ['points', 'pts']),
        ('radl', ['rad', 'radial', 'radiel', 'radl']),
        ('ramp', ['ramp']),
        ('rd', ['rd', 'road']),
        ('rdg', ['rdg', 'rdge', 'ridge']),
        ('rdgs', ['rdgs', 'ridges']),
        ('rds', ['rds', 'roads']),
        ('riv', ['riv', 'river', 'rivr', 'rvr']),
        ('rnch', ['ranch', 'ranches', 'rnch', 'rnchs']),
        ('row', ['row']),
        ('rpd', ['rapid', 'rpd']),
        ('rpds', ['rapids', 'rpds']),
        ('rst', ['rest', 'rst']),
        ('rte', ['route']),
        ('rue', ['rue']),
        ('run', ['run']),
        ('shl', ['shl', 'shoal']),
        ('shls', ['shls', 'shoals']),
        ('shr', ['shoar', 'shore', 'shr']),
        ('shrs', ['shoars', 'shores', 'shrs']),
        ('skwy', ['skyway']),
        ('smt', ['smt', 'sumit', 'sumitt', 'summit']),
        ('spg', ['spg', 'spng', 'spring', 'sprng']),
        ('spgs', ['spgs', 'spngs', 'springs', 'sprngs']),
        ('spur', ['spur', 'spurs']),
        ('sq', ['sq', 'sqr', 'sqre', 'squ', 'square']),
        ('sqs', ['sqrs', 'squares']),
        ('st', ['st', 'str', 'street', 'strt']),
        ('sta', ['sta', 'station', 'statn', 'stn']),
        ('stra', ['stra', 'strav', 'strave', 'straven', 'stravenue', 'stravn', 'strvn', 'strvnue']),
        ('strm', ['stream', 'streme', 'strm']),
        ('sts', ['streets']),
        ('ter', ['ter', 'terr', 'terrace']),
        ('tpke', ['tpk', 'tpke', 'trnpk', 'trpk', 'turnpike', 'turnpk']),
        ('trak', ['track', 'tracks', 'trak', 'trk', 'trks']),
        ('trce', ['trace', 'traces', 'trce']),
        ('trfy', ['trafficway', 'trfy']),
        ('trl', ['tr', 'trail', 'trails', 'trl', 'trls']),
        ('trwy', ['throughway']),
        ('tunl', ['tunel', 'tunl', 'tunls', 'tunnel', 'tunnels', 'tunnl']),
        ('un', ['un', 'union']),
        ('uns', ['unions']),
        ('upas', ['underpass']),
        ('via', ['vdct', 'via', 'viadct', 'viaduct']),
        ('vis', ['vis', 'vist', 'vista', 'vst', 'vsta']),
        ('vl', ['ville', 'vl']),
        ('vlg', ['vill', 'villag', 'village', 'villg', 'villiage', 'vlg']),
        ('vlgs', ['villages', 'vlgs']),
        ('vly', ['valley', 'vally', 'vlly', 'vly']),
        ('vlys', ['valleys', 'vlys']),
        ('vw', ['view', 'vw']),
        ('vws', ['views', 'vws']),
        ('walk', ['walk', 'walks']),
        ('wall', ['wall']),
        ('way', ['way', 'wy']),
        ('ways', ['ways']),
        ('wl', ['well']),
        ('wls', ['wells', 'wls']),
        ('xing', ['crossing', 'crssing', 'crssng', 'xing']),
        ('xrd', ['crossroad']),
        ])

# https://www.usps.com/send/official-abbreviations.htm
state_abbrev = OrderedDict([
        ('al', ['alabama']),
        ('ak', ['alaska']),
        ('as', ['american samoa']),
        ('az', ['arizona']),
        ('ar', ['arkansas']),
        ('ca', ['california']),
        ('co', ['colorado']),
        ('ct', ['connecticut']),
        ('de', ['delaware']),
        ('dc', ['district of columbia']),
        ('fm', ['federated states of micronesia']),
        ('fl', ['florida']),
        ('ga', ['georgia']),
        ('gu', ['guam gu']),
        ('hi', ['hawaii']),
        ('id', ['idaho']),
        ('il', ['illinois']),
        ('in', ['indiana']),
        ('ia', ['iowa']),
        ('ks', ['kansas']),
        ('ky', ['kentucky']),
        ('la', ['louisiana']),
        ('me', ['maine']),
        ('mh', ['marshall islands']),
        ('md', ['maryland']),
        ('ma', ['massachusetts']),
        ('mi', ['michigan']),
        ('mn', ['minnesota']),
        ('ms', ['mississippi']),
        ('mo', ['missouri']),
        ('mt', ['montana']),
        ('ne', ['nebraska']),
        ('nv', ['nevada']),
        ('nh', ['new hampshire']),
        ('nj', ['new jersey']),
        ('nm', ['new mexico']),
        ('ny', ['new york']),
        ('nc', ['north carolina']),
        ('nd', ['north dakota']),
        ('mp', ['northern mariana islands']),
        ('oh', ['ohio']),
        ('ok', ['oklahoma']),
        ('or', ['oregon']),
        ('pw', ['palau']),
        ('pa', ['pennsylvania']),
        ('pr', ['puerto rico']),
        ('ri', ['rhode island']),
        ('sc', ['south carolina']),
        ('sd', ['south dakota']),
        ('tn', ['tennessee']),
        ('tx', ['texas']),
        ('ut', ['utah']),
        ('vt', ['vermont']),
        ('vi', ['virgin islands']),
        ('va', ['virginia']),
        ('wa', ['washington']),
        ('wv', ['west virginia']),
        ('wi', ['wisconsin']),
        ('wy', ['wyoming']),
        ('aa', ['armed forces americas']),
        ('ae', ['armed forces africa', 'armed forces canada', 'armed forces europe', 'armed forces middle east']),
        ('ap', ['armed forces pacific']),
        ])

# TODO Suppport other country abbreviations
country_abbrev = OrderedDict([
        ('usa', ['united states', 'united states of america', 'us of a', 'usa', 'us']),
        ])

def _normalize_option(abbrev, options, item):
    for option in options:
        if option == item:
            return abbrev

    return None

def normalize_string(item):
    parts = item.split()
    item = ' '.join(parts)
    item = item.lower()

    for punc in string.punctuation:
        item = item.replace(punc, '')

    return item

def normalize_street(street):
    street = normalize_string(street)

    parts = street.split()
    # TODO Put cardinal direction after the street number,
    # if not already there, e.g., 'Main St S' -> 'S Main St'
    for abbrev, options in cardinal_abbrev.iteritems():
        for option in options:
            for part in parts:
                if option == part:
                    parts[parts.index(part)] = abbrev

    # TODO Put street abbrev after street name, if not
    # already there, e.g, 'St Main' -> 'Main St'
    for abbrev, options in street_abbrev.iteritems():
        for option in options:
            for part in parts:
                if option == part:
                    parts[parts.index(part)] = abbrev

    street = ' '.join(parts)

    return street

def normalize_city(city):
    # TODO Convert between abbreviations and full names
    city = normalize_string(city)

    return city

def normalize_state(state):
    # TODO Normalize duplicates, e.g., 'CA California' -> 'ca'
    state = normalize_string(state)

    for abbrev, options in state_abbrev.iteritems():
        change = _normalize_option(abbrev, options, state)
        if change is not None:
            state = change
            break

    return state

def normalize_country(country):
    # TODO Normalize duplicates, e.g., 'USA US' -> 'usa'
    country = normalize_string(country)

    for abbrev, options in country_abbrev.iteritems():
        change = _normalize_option(abbrev, options, country)
        if change is not None:
            country = change
            break

    return country

def normalize_zipcode(zipcode):
    if zipcode == '':
        return ''

    parts = zipcode.split('-')
    zip5 = parts[0]
    if len(zip5) < 5:
        return ''
    zip5 = zip5[:5]
    try:
        int(zip5)
    except ValueError:
        return ''

    return zip5

def streets_equal(street1, street2):
    street1 = normalize_street(street1)
    street2 = normalize_street(street2)

    if street1 == '' and street2 == '':
        return False

    equal = (street1 in street2
             or
             street2 in street1
             )

    return equal

def cities_equal(city1, city2):
    # TODO Compare city districts with their city
    # names, e.g., Hollywood and Los Angeles
    city1 = normalize_city(city1)
    city2 = normalize_city(city2)

    if city1 == '' and city2 == '':
        return False

    equal = (city1 == city2)

    return equal

def states_equal(state1, state2):
    state1 = normalize_state(state1)
    state2 = normalize_state(state2)

    if state1 == '' and state2 == '':
        return False

    equal = (state1 == state2)

    return equal

def countries_equal(country1, country2):
    country1 = normalize_country(country1)
    country2 = normalize_country(country2)

    if country1 == '' and country2 == '':
        return False

    equal = (country1 == country2)

    return equal

def zipcodes_equal(zip1, zip2):
    zip1 = normalize_zipcode(zip1)
    zip2 = normalize_zipcode(zip2)

    if zip1 == '' and zip2 == '':
        return False

    equal = (zip1 == zip2)

    return equal
