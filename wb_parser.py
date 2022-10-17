import random
import time
import re
from datetime import datetime
from datetime import date

from postgresql import Database
import config

import requests
from fake_useragent import UserAgent
import schedule


def parse_wildberries():
    # TODO: Programm process is realy long.
    # So it's necessery to logging because of troubleshooting.

    # TODO: Add attempts variable and pass it to all request functions.
    try:
        print(f'{now()} [INFO] Getting wildberries menu...')
        wb_menu = _get_menu_data()

        print(f'{now()} [INFO] Filtering categories...')
        filtered_wb_menu = _filter_categories(wb_menu)

        print(f'{now()} [INFO] Start extracting atomic categories...')
        atomic_categories = _extract_atomic_categories(filtered_wb_menu)

        print(f'{now()} [INFO] Splitting categories on xsubjects...')
        print(f"{now()} [INFO] It's a long process, be patient.")
        splitted_categories = _split_into_xsubjects(atomic_categories)

        print(f'{now()} [INFO] Preparing to parse products info...')
    except Exception as e:
        print(f'{now()} [ERR] Error occured while getting initial data!')
        print(f'{now()} [ERR] {e}')
        print(f'{now()} [ERR] Parsing process forced stop until tommorow.')
        return

    # TODO: Parsing process is awfully long becase of product count. You are
    # really don't want to break its.
    # So it's necessery to add try/except block if something unexpected will
    # happen.

    cur_date = date.today()

    for category in splitted_categories:
        # Set connection with database.
        db = Database()
        db.connect()

        # Set required variables.
        shard = category['shard']
        query = category['query']
        xsubject_id = category.pop('xsubject_id', None)
        xsubject = category.pop('xsubject', None)
        ctgr = category.pop('name', None)
        ctgr_id = category.pop('id', None)

        try:
            print(f"{now()} [INFO] Start parsing category '{ctgr}'...")
            filter_data = _get_filter_data(shard=shard, query=query)
        except Exception as e:
            print(f'{now()} [ERR] Error occured while getting filter data')
            print(f'{now()} [ERR] {e}')
            continue
        
        if xsubject is not None:
            print(f"{now()} [INFO] Category subject name '{xsubject}'.")
            print(f"{now()} [INFO] Category subject id {xsubject_id}.")
        else:
            print(f"{now()} [INFO] Category has no subject.")


        print(f'{now()} [INFO] Calculating number of pages...')
        #page_count = _get_page_count(filter_data=filter_data)
        # Case for testing.
        page_count = 1
        print(f'{now()} [INFO] Page count is {page_count}.')
        

        # Note: first page has number 1 (not 0).
        for page_number in range(1, page_count + 1):
            print(f'{now()} [INFO] Parsing page {page_number}...')

            try:
                products_data = _get_raw_catalog_page_data(
                        shard=shard, page=str(page_number), query=query)
                products_id = _extract_products_id(products_data)
                sales_amount_data = _get_sales_amount(products_id)
                products_data = _append_sales_amount(
                        products_data, sales_amount_data)
            except Exception as e:
                print(f'{now()} [ERR] Error occured while '
                f'parsing page {page_number}!')
                print(f'{now()} [ERR] {e}')

                continue

            try:
                for product in products_data['data']['products']:
                    _insert_product_in_db(db, product, ctgr, ctgr_id, xsubject,
                            xsubject_id, cur_date)
            except:
                print(f'{now()} [ERR] Error occured while insert '
                      f'page {page_number} data to database!')

        db.close()
        print(f'{now()} [INFO] Category parsing complete.')

#        if not _check_time():
#            print(f'{now()} [WRN] Parsing process is too long for today.'
#                    ' It is automatically skipped for next day.')
#            break

    print(f'{now()} [INFO] Parsing process is finished.')


def _get_sales_amount(products_id_list: list) -> list:
    """ That request contains sales amount of wildberries products.
    Post's json contains list of products id.
    """

    url = 'https://product-order-qnt.wildberries.ru/by-nms'
    user_agent = UserAgent().random
    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        # Already added when you pass json=
        # 'content-type': 'application/json',
        'Origin': 'https://www.wildberries.ru',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        # Requests doesn't support trailers
        # 'TE': 'trailers',
    }

    attempts = config.PARSE_ATTEMPTS
    while attempts > 0:
        try:
            response = requests.post(url, headers=headers,
                                     json=products_id_list).json()
            attempts = 0
        except Exception as e:
            print(f'{now()} [ERR] Exception occured! {e}')
            print(f'{now()} [ERR] Trying to request again...')
            time.sleep(config.PARSE_TIMEOUT)
            attempts -= 1
    delay()
    return response


def _extract_products_id(products_data: dict) -> list:
    """ Extract id of each product for another requests."""

    products_id = []
    for product in products_data['data']['products']:
        products_id.append(product['id'])
    return products_id


def _append_sales_amount(products_data: dict, sales_amount_data: list) -> dict:
    """ Append data about amount of sales for each product from another
    request."""

    for sales_amount, product in zip(
            sales_amount_data, products_data['data']['products']):
        product['salesAmount'] = sales_amount['qnt']
    return products_data


def _get_raw_catalog_page_data(shard: str, page: str, query: str) -> dict:
    """ Get specifice wildberries catalog page data.
    Request return 100 products info. Products sort by popularity.
    shard - category name.
    """

    user_agent = UserAgent().random
    url = (
        f'https://catalog.wb.ru/catalog/{shard}/catalog'
        '?appType=1'
        '&couponsGeo=2,7,3,6,19,21,8'
        '&curr=rub'
        '&dest=-1059500,-108082,-364545,123586027'
        '&emp=0'
        '&lang=ru'
        '&locale=ru'
        f'&page={page}'
        '&pricemarginCoeff=1.0'
        '&reg=0'
        '&regions=68,64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40'
        '&sort=popular'
        '&spp=0'
        '&' + query)

    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Origin': 'https://www.wildberries.ru',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
    }

    attempts = config.PARSE_ATTEMPTS
    while attempts > 0:
        try:
            response = requests.get(url, headers=headers).json()
            attempts = 0
        except Exception as e:
            print(f'{now()} [ERR] Exception occured! {e}')
            print(f'{now()} [ERR] Trying to request again...')
            time.sleep(config.PARSE_TIMEOUT)
            attempts -= 1
    delay()
    return response


def _get_filter_data(shard: str, query: str) -> dict:
    """ Get response that contains information about xsubjects name and 
    amounts of sales."""

    user_agent = UserAgent().random
    url = (
            f'https://catalog.wb.ru/catalog/{shard}/v4/filters'
            '?appType=1'
            '&couponsGeo=2,7,3,6,19,21,8'
            '&curr=rub'
            '&dest=-1059500,-108082,-364545,123586027'
            '&emp=0'
            '&lang=ru'
            '&locale=ru'
            '&pricemarginCoeff=1.0'
            '&reg=0'
            '&regions=68,64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40'
            '&spp=0'
            '&' + query
            )

    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Origin': 'https://www.wildberries.ru',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
    }

    attempts = config.PARSE_ATTEMPTS
    while attempts > 1:
        try:
            response = requests.get(url, headers=headers).json()
            attempts = 0
        except Exception as e:
            print(f'{now()} [ERR] Exception occured! {e}')
            print(f'{now()} [ERR] Trying to request again...')
            time.sleep(config.PARSE_ATTEMPTS)
            attempts -= 1
    delay()
    return response


def _extract_xsubjects(filter_data: dict) -> list[tuple] | None:
    """Extract subcategories name and ids from wildberries request."""

    try:
        sub_categories = filter_data['data']['filters'][0]['items']
        xsubjects = []
        for category in sub_categories:
            xsubjects.append((category['id'], category['name']))
        return xsubjects
    except:
        return None


def _get_page_count(filter_data: dict) -> int:
    """Function returns correct number of pages for filtered request."""

    product_count = _extract_product_count(filter_data)
    page_count = _calculate_page_count(product_count)
    return page_count


def _extract_product_count(response: dict) -> int:
    """Function extracts total number of products for filtered request."""

    return int(response['data']['total'])


def _calculate_page_count(product_count: int) -> int:
    """Function calculates number of pages based on the number of products.
    Wildberries has limit of 100 per filtered request. So we set number of
    pages equals to 100 if there are more."""

    # Whole number of pages.
    basic_pages = product_count // 100
    # Line below adds additional page if it's necessery
    # and it adds single page if product count lower than 100
    additional_page = 1 if product_count % 100 != 0 else 0
    page_count = basic_pages + additional_page
    return 100 if page_count > 100 else page_count


def _get_menu_data() -> list:
    """That request give us json representation of wilberries menu with all
    categories and sub-categories.
    """

    user_agent = UserAgent().random
    cookies = {
        'BasketUID': '8716ea16-cf7b-4958-a4d5-b909c1597f87',
        '_wbauid': '2246844731655471226',
        '___wbu': 'e4168767-2102-49b6-bf22-68ae99c9aec5.1655471227',
        '___wbu': '92fa38d6-79bc-49be-8930-e27eba9f342d.1659694892',
        '__store': '130744_117501_507_3158_204939_120762_117986_159402_2737_686_1733_1193_206968_206348_205228_172430_117442_117866',
        '__region': '68_64_83_4_38_80_33_70_82_86_30_69_1_48_22_66_31_40',
        '__pricemargin': '1.0--',
        '__cpns': '2_7_3_6_19_21_8',
        '__sppfix': '',
        '__dst': '-1059500_-108082_-364545_123586027',
        'ncache': '130744_117501_507_3158_204939_120762_117986_159402_2737_686_1733_1193_206968_206348_205228_172430_117442_117866%3B68_64_83_4_38_80_33_70_82_86_30_69_1_48_22_66_31_40%3B1.0--%3B2_7_3_6_19_21_8%3B%3B-1059500_-108082_-364545_123586027',
        '__tm': '1663349797',
        'route': '1656683746.042.10760.627647|0b90fde812cf571b8c1d1da630a06350',
        '__wba_s': '1',
        '_wbSes': 'CfDJ8GWigffatjpAmgU4Ds4%2BnhuWcTwaEOBsLCLqa8fXaaa6VogemDTDJh7prK0BweHn9tbzX%2FD3G0%2FOtrjNP9AXs9wZ77neUYa5U5FaFdRlvyGdOwzxwVSe42KVHp1pvcS46aJBSfFDrj5zfewuES1v4AtCeE%2FQQStl0zffj5A1UtRS',
        '__bsa': 'basket-ru-35',
        '__wbl': 'cityId%3D0%26regionId%3D0%26city%3D%D0%9D%D0%BE%D0%B2%D0%BE%D1%80%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%26phone%3D84957755505%26latitude%3D44%2C752861%26longitude%3D37%2C768311%26src%3D1',
    }

    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'x-requested-with': 'XMLHttpRequest',
        'x-spa-version': '9.3.36.1',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        # Requests doesn't support trailers
        # 'TE': 'trailers',
    }
    url = 'https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json'

    attempts = config.PARSE_ATTEMPTS
    while attempts > 1:
        try:
            response = requests.get(url, 
                        cookies=cookies, headers=headers).json()
            attempts = 0
        except Exception as e:
            print(f'{now()} [ERR] Exception occured! {e}')
            print(f'{now()} [ERR] Trying to request again...')
            time.sleep(config.PARSE_TIMEOUT)
            attempts -= 1
    delay()
    return response


def _filter_categories(all_categories_data: list) -> list:
    """ Wildberries categories request has categories without 'classic'
    product cards. We don't need to parse that so we filter these categories
    before start extracting all sub-sub-categories.
    """
    
    filtered_categories_data = filter(
            _check_category, all_categories_data)

    return list(filtered_categories_data)



def _check_category(category: dict) -> bool:
    """Function uses for filter() so if category_id is in unwanted 
    categories id list than return False.
    Need attention if unwanted categories will expand or ids are changed.
    """

    unwanted_categories_id = [
            128297,     # Premium
            12,         # Digital products
            2192,       # Promotions
            61037,      # Aviatickets
            4853,       # Brands
            111,        # Videoreview
            11,         # Express delivery
            128891,     # Alcohol
            128313,     # Local products
            128604,     # Vkusi Rossii
            ]
    return False if category['id'] in unwanted_categories_id else True



def _extract_atomic_categories(categories: list) -> list:
    """Function extracts as smallest categories as possible.
    Dictionary with category of that type don't have 'childs' keyword.
    """
    
    process_list = categories
    atomic_category_list = []
    while len(process_list) != 0:
        temp_list = []
        for one in process_list:
            if 'childs' in one.keys():
                for child in one['childs']:
                    temp_list.append(child)
            else:
                atomic_category_list.append(one)
        process_list = []
        process_list = temp_list.copy()

    return atomic_category_list


def _split_into_xsubjects(categories: list) -> list:
    """ Function takes atomic categories list and xsubjects list then split it
    to xsubjects.
    """

    splitted_categories = [] 
    for category in categories:
        # Some categories don't have query info. Its also don't have own 
        # shard name. It's duplicate categories. When we click on them 
        # wilberries redirect us to other directory. So we just will skip
        # categories of that type.
        if 'query' in category.keys():
            shard = category['shard']
            query = category['query']
            filter_data = _get_filter_data(shard=shard, query=query)
            xsubjects = _extract_xsubjects(filter_data)
            if xsubjects is not None:
                for xsubject in xsubjects:
                    tmp_category = category.copy()
                    tmp_category['xsubject_id'] = xsubject[0]
                    tmp_category['xsubject'] = xsubject[1]
                    tmp_category['query'] = tmp_category['query'] \
                            + "&xsubject=" + str(xsubject[0])
                    splitted_categories.append(tmp_category)
            else:
                tmp_category = category.copy()
                tmp_category['xsubject_id'] = None
                tmp_category['xsubject'] = None
                splitted_categories.append(tmp_category)

    return splitted_categories


def _insert_product_in_db(db: Database, product: dict,
        ctgr: str, ctgr_id:str, xsubject:str, xsubject_id: int,
        cur_date: date) -> None:
    """ That function execute database insert operation."""

    article_number = product.pop('id', None)
    product_name = product.pop('name', None)
    values = [(
        ctgr, ctgr_id, xsubject, xsubject_id, 
        product.pop('brand', None),
        product.pop('brandId', None), 
        article_number, product_name,
        _price_cut(product.pop('priceU', 0)),
        _price_cut(product.pop('salePriceU', 0)),
        _price_cut(product.pop('averagePrice', 0)), 
        product.pop('salesAmount', None),
        product.pop('rating', None), 
        product.pop('feedbacks', None), cur_date
        )]
    values = _filter_length(values)
    query = """
    INSERT INTO products (
    ctgr,
    ctgr_id,
    subject,
    subject_id,
    brand_name,
    brand_id,
    article_number,
    product_name,
    base_price,
    sale_price,
    average_price,
    sales_amount,
    rating,
    feedback_count,
    date) VALUES %s
    ON CONFLICT ON CONSTRAINT unique_product_data DO NOTHING; """
    db.execute(query, values)


def _filter_length(values: list[tuple]) -> list[tuple]:
    """ This function check whether string values more than 38 characters or
    not. It's necessery because of database type of string values is stricted
    to 38 characters."""

    filtered_values = []
    for value in values[0]:
        if type(value) == str:
            filtered_values.append(value[:38])
        else:
            filtered_values.append(value)
    return [tuple(filtered_values)]


def _price_cut(price: int) -> int:
    """ Wildberries price data integer have two extra zero. So this function
    cut them and return correct value."""

    if price != 0:
        return int(str(price)[:-2])
    else:
        return price


def _check_time() -> bool:
    """ Function check current time and return False if it's 23:00.
    It is used for setting day's time limit to parser."""

    cur_time = datetime.now()
    time_limit = cur_time.replace(hour=23, minute=0, second=0, microsecond=0)
    if cur_time > time_limit:
        return False
    else:
        return True


def delay() -> None:
    """Simple function that imitates delay."""

    delay_duration = random.uniform(0.1, 0.5)
    time.sleep(delay_duration)


def now() -> str:
    """Simple function that returns formatted current time"""

    return datetime.now().strftime("%H:%M:%S") 


def main():
    # First launch.
    parse_wildberries()

    # Then every day at 00:00 time parse_wildberries() is called.
    schedule.every().day.at("00:00").do(parse_wildberries)
    while True:
        # Checks whether a sheduled task is pending to run or not.
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()

