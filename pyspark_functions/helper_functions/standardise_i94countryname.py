from fuzzywuzzy import fuzz
from fuzzywuzzy import process

#standardise_countryname function
def standardise_countryname(country, former_countries_dict, ref_list):
    """
    This function formats the country name label in i94cntyl,
    checks whether the country name is in former country ISO3166-3 (former_countries_dict),
    and finally finds a matching ISO 3166-1 country name in countries_ref_list
    using fuzzywuzzy package

    Args:
        country (string): country name to check.
        former_countries_dict (dictionary): Dictionary containing formerly used country name as key,
        and its adapted country name as value.
        ref_list (list): List of country name in ISO 3166-1.

    Returns:
        result[0] (string): formatted country name.
    """
    if '(FRENCH)' in country:
        country = 'France'

    country = country.strip().title().split(' (')[0].replace("'", "")\
                                                    .replace('Is.', 'Islands')\
                                                    .replace('Isls', 'Islands')\
                                                    .replace('Invalid: ', '')\
                                                    .replace('St.', 'Saint')

    if 'Cocos' in country:
        country = 'Cocos (Keeling) Islands'
    elif 'Johnson Island' in country:
        country = 'Johnston Island'
    elif 'Ussr' in country:
        country = 'USSR'
    elif 'Laos' in country:
        country = "Lao People's Democratic Republic"
    elif 'Kampuchea' in country:
        country = "Cambodia"
    elif 'Maarten' in country:
        country = 'Sint Maarten'

    find_former_country = process.extract(country, list(former_countries_dict.keys()))[0]

    if find_former_country[1] == 100:
        country = former_countries_dict[find_former_country[0]]
    else:
        country = country

    result = process.extract(country, ref_list)[0]

    if result[1] < 70:
        return 'No match found'
    else:
        return result[0]
