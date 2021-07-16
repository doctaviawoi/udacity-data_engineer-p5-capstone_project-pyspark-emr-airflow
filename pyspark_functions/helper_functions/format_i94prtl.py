from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def format_i94prtl(string, former_countries_dict, countries_ref_list):
    """
    This function formats port location label in `i94prtl.txt` and matches invalid
    port codes to airport and us-port-codes datasets to get a possible port location
    using fuzzywuzzy Python package.

    Arg:
        string (string): port location label in `i94prtl.txt`.

    Returns:
        string (string): formatted port location label.
    """
    to_replace = ['INVALID - ', ' (BPS)', ' (I-91)', ' (RT. 5)', ' (BP - SECTOR HQ)', ' #ARPT', '#ARPT', '-ARPT', 'ARPT']

    for char in to_replace:
        string = string.strip().replace(char, '')

    string = string.replace('NETH ANTILLES', 'Netherlands Antilles')\
                    .replace('WASHINGTON #INTL', 'WA')\
                    .replace('MEXICO Land (Banco de Mexico)', 'Mexico City, Mexico')\
                    .replace("PASO DEL NORTE,TX", "PASO DEL NORTE, TX")\
                    .replace("WASHINGTON DC", "WASHINGTON, DC")\
                    .replace("MARIPOSA AZ", "MARIPOSA, AZ")\
                    .replace("SAIPAN, SPN", "SAIPAN, MP")\
                    .replace("Abu Dhabi", "ABU DHABI, UNITED ARAB EMIRATES")\
                    .replace("SEOUL KOREA", "SEOUL, SOUTH KOREA")\
                    .replace("PRC", "CHINA")\
                    .replace("TURK & CAIMAN", "Turks and Caicos Islands")\
                    .replace("PAPUA, NEW GUINEA", "SEPIK PLAINS, PAPUA NEW GUINEA")\
                    .replace("ENGLAND", "UNITED KINGDOM")\
                    .replace("UNIDENTIFED AIR / SEAPORT", "Unknown code")\
                    .replace("UNKNOWN POE", "Unknown code")\
                    .replace("NOT REPORTED/UNKNOWN", "Unknown code")

    if string.find(',') != -1:
        if (len(string.split(', ')) > 2) & (len(string.strip(', ')[-1]) == 2):
            # eg. JUAREZ INTL, MEXICO CITY, MX

            result = string.split(', ')
            return result[0].title().strip() + ', ' + result[-1].strip()

        elif (len(string.split(', ')) > 2) & (len(string.strip(', ')[-1]) > 2):
            # eg. GUARULHOS INTL, SAO PAULO, BRAZIL

            result = string.split(', ')

            # find country name in former country name
            # and standardise country name based on countries_ref_list
            find_former_country = process.extract(result[-1], list(former_countries_dict.keys()))[0]

            if find_former_country[1] == 100:
                country = former_countries_dict[find_former_country[0]]
            else:
                country = result[-1]

            country = process.extract(country, countries_ref_list)[0][0]

            return result[0].title().strip() + ', ' + country

        elif len(string.split(', ')[-1]) > 2:
            # eg. HAKAI PASS, CANADA

            result = string.split(', ')

            # find country name in former country name
            # and standardise country name based on countries_ref_list
            find_former_country = process.extract(result[-1], list(former_countries_dict.keys()))[0]

            if find_former_country[1] == 100:
                country = former_countries_dict[find_former_country[0]]
            else:
                country = result[-1]

            country = process.extract(country, countries_ref_list)[0][0]

            return result[0].title().strip() + ', ' + country

        elif len(string.split(', ')[-1]) == 2:
            # eg. TAMPA, FL
            return string.split(',')[0].title() + ',' + string.split(',')[-1]
    else:
        return string
