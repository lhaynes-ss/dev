import os

'''
Note:
- Sheet for excel needs to be named "weekly" or "monthly"
- Tables within sheet need to be named "campaign", "download", "time". Do this from "design" tab
'''


def get_app_config():
    return {
        'path': os.path.realpath(os.path.dirname(__file__)) + '\\'
        ,'destination_bucket_keys': {
            'test'              : {'bucket': 'samsung.ads.data.share', 'prefix': 'analytics/custom/vaughn/test/python/'}
            ,'dev_pplus_us'     : {'bucket': 'dev-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-us/'}
            ,'dev_pplus_int'    : {'bucket': 'dev-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-international/'}
            ,'prod_pplus_us'    : {'bucket': 'prod-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-us/'}
            ,'prod_pplus_int'   : {'bucket': 'prod-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-international/'}
            ,'dev_pluto_us'     : {'bucket': 'dev-samsung-dm-data-share-pluto', 'prefix': 'pluto-us/'}
            ,'dev_pluto_int'    : {'bucket': 'dev-samsung-dm-data-share-pluto', 'prefix': 'pluto-international/'}
            ,'prod_pluto_us'    : {'bucket': 'prod-samsung-dm-data-share-pluto', 'prefix': 'pluto-us/'}
            ,'prod_pluto_int'   : {'bucket': 'prod-samsung-dm-data-share-pluto', 'prefix': 'pluto-international/'}
        }
    }


def get_report_config():
    return {
        'start_date': '2024-02-19'
        ,'end_date': '2024-02-25'
        ,'reports':[
            # {
            #     'partner': 'paramount_plus'                       # paramount_plus | pluto
            #     ,'bucket_key': 'test'                             # see 'destination_bucket_keys' dictionary above for options
            #     ,'interval': 'weekly'                             # weekly | monthly
            #     ,'region': 'au'                                   # e.g., fr | es | de | it
            #     ,'filename': 'input\\test_files\\test_pplus_au.xlsx'     # file name or path relative to this directory
            # }
            # --------------------------------------
            # paramount+
            # --------------------------------------
            {
                'partner': 'paramount_plus'
                ,'bucket_key': 'test' 
                ,'interval': 'weekly'
                ,'region': 'au'
                ,'filename': 'input\\test_files\\test_pplus_au.xlsx'
            }
            ,{
                'partner': 'paramount_plus'
                ,'bucket_key': 'test' 
                ,'interval': 'weekly'
                ,'region': 'fr'
                ,'filename': 'input\\test_files\\test_pplus_fr.xlsx'
            }  
            # --------------------------------------
            # pluto
            # --------------------------------------
            ,{
                'partner': 'pluto'
                ,'bucket_key': 'test' 
                ,'interval': 'weekly'
                ,'region': 'de'
                ,'filename': 'input\\test_files\\test_pluto_de.xlsx'
            }
            ,{
                'partner': 'pluto'
                ,'bucket_key': 'test' 
                ,'interval': 'weekly'
                ,'region': 'fr'
                ,'filename': 'input\\test_files\\test_pluto_fr.xlsx'
            }
            # --------------------------------------
        ]
    }

