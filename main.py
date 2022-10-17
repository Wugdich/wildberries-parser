def main():
    test_product_id_list = [
        12333579,
        13600870,
        44275522,
        14880730,
        13233430,
        59648344,
        15740135
    ]
    # Maybe idList to idString (and vice versa) functions?

    test_product_id_string = ('12333579;13699870;44275522;14880730;13233430'
                              '59648344;15740135')

    test_sales_amount =[
            {'nmId': 12333579, 'qnt': 100000},
            {'nmId': 13600870, 'qnt': 2000},
            {'nmId': 44275522, 'qnt': 5300},
            {'nmId': 14880730, 'qnt': 6800},
            {'nmId': 13233430, 'qnt': 3000},
            {'nmId': 59648344, 'qnt': 1900},
            {'nmId': 15740135, 'qnt': 1700}
            ]
    
    test_data_to_catalog = ('bl_shirts', '41;184;1429', '41')

#    with open('menu_data.json', 'w') as json_file:
#        menu_data = _get_menu_data()
#        json.dump(
#                menu_data,
#                json_file, indent=4, ensure_ascii=False)
#
    with open('atomic_categories.json', 'r') as jf:
        categories = json.load(jf)
        _parse_all_products(categories)



if __name__ == '__main__':
    main()

